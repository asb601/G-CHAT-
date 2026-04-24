"""
Temporal parser — extracts a (date_from, date_to) window from natural language.

Used by the retrieval engine as Stage 1: if the user's query contains a time
reference, only files whose date_range_start/date_range_end overlaps the window
are eligible for the later BM25/vector stages.  If no time reference is found,
returns (None, None) and the engine proceeds with no date filter.

Design choices
--------------
- Zero-LLM: pure regex + relativedelta arithmetic.  Fast (<1 ms), free, no API.
- Relative expressions anchored to `today` (passed in for deterministic testing).
- Fiscal-year aware: FY2025 = Apr 2024 → Mar 2025 (standard Apr-start fiscal).
- Returns date objects (not strings) so SQLAlchemy can use them directly.
- All patterns are case-insensitive.

Public API
----------
    parse_temporal(query: str, today: date | None = None)
        -> tuple[date | None, date | None]

    Returns (date_from, date_to).  Both None = no time filter.
"""
from __future__ import annotations

import re
from datetime import date, timedelta

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _start_of_month(d: date) -> date:
    return d.replace(day=1)


def _end_of_month(d: date) -> date:
    """Last day of the month containing `d`."""
    # Advance to 1st of next month, subtract 1 day
    if d.month == 12:
        return d.replace(month=12, day=31)
    return d.replace(month=d.month + 1, day=1) - timedelta(days=1)


def _quarter_bounds(year: int, q: int) -> tuple[date, date]:
    """Calendar quarter q (1-4) for year.  Q1 = Jan-Mar, Q2 = Apr-Jun, …"""
    start_month = (q - 1) * 3 + 1
    start = date(year, start_month, 1)
    end = _end_of_month(date(year, start_month + 2, 1))
    return start, end


def _fiscal_quarter_bounds(year: int, q: int) -> tuple[date, date]:
    """Fiscal Q (1-4) where FY starts 1 Apr.
    FY2025 Q1 = Apr-Jun 2024, FY2025 Q4 = Jan-Mar 2025."""
    # FY year starts April of (year-1)
    fy_start_year = year - 1
    start_month = 4 + (q - 1) * 3        # Q1→Apr, Q2→Jul, Q3→Oct, Q4→Jan
    if start_month > 12:
        start_month -= 12
        fy_start_year += 1
    start = date(fy_start_year, start_month, 1)
    end = _end_of_month(date(fy_start_year, start_month + 2, 1)
                        if start_month <= 10
                        else date(fy_start_year + 1, (start_month + 2) % 12 or 12, 1))
    return start, end


_MONTH_ABBR = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}

# ---------------------------------------------------------------------------
# Pattern table  (ordered: most-specific first)
# ---------------------------------------------------------------------------
# Each entry: (compiled_regex, handler_callable(match, today) -> (date, date))

def _p(pattern: str):
    return re.compile(pattern, re.IGNORECASE)


def _h_last_n_days(m, today):
    n = int(m.group(1))
    return today - timedelta(days=n), today


def _h_last_n_weeks(m, today):
    n = int(m.group(1))
    return today - timedelta(weeks=n), today


def _h_last_n_months(m, today):
    n = int(m.group(1))
    # shift back n months
    month = today.month - n
    year = today.year
    while month <= 0:
        month += 12
        year -= 1
    d = date(year, month, today.day)
    return _start_of_month(d), today


def _h_last_n_years(m, today):
    n = int(m.group(1))
    return date(today.year - n, today.month, today.day), today


def _h_yesterday(m, today):
    d = today - timedelta(days=1)
    return d, d


def _h_today(m, today):
    return today, today


def _h_this_week(m, today):
    start = today - timedelta(days=today.weekday())   # Monday
    return start, start + timedelta(days=6)


def _h_last_week(m, today):
    monday = today - timedelta(days=today.weekday() + 7)
    return monday, monday + timedelta(days=6)


def _h_this_month(m, today):
    return _start_of_month(today), _end_of_month(today)


def _h_last_month(m, today):
    first = _start_of_month(today)
    last_m_end = first - timedelta(days=1)
    return _start_of_month(last_m_end), last_m_end


def _h_this_year(m, today):
    return date(today.year, 1, 1), date(today.year, 12, 31)


def _h_last_year(m, today):
    y = today.year - 1
    return date(y, 1, 1), date(y, 12, 31)


def _h_ytd(m, today):
    return date(today.year, 1, 1), today


def _h_mtd(m, today):
    return _start_of_month(today), today


def _h_qtd(m, today):
    q = (today.month - 1) // 3 + 1
    start, _ = _quarter_bounds(today.year, q)
    return start, today


def _h_this_quarter(m, today):
    q = (today.month - 1) // 3 + 1
    return _quarter_bounds(today.year, q)


def _h_last_quarter(m, today):
    q = (today.month - 1) // 3 + 1
    q -= 1
    year = today.year
    if q == 0:
        q = 4
        year -= 1
    return _quarter_bounds(year, q)


def _h_qN_year(m, today):
    """Q2 2024 / Q2FY25 / Q2 FY2025"""
    q = int(m.group(1))
    raw_year = int(m.group(2))
    fiscal = bool(m.group(0).lower().find("fy") >= 0 or
                  m.group(0).lower().find("fiscal") >= 0)
    year = raw_year if raw_year > 100 else 2000 + raw_year
    if fiscal:
        return _fiscal_quarter_bounds(year, q)
    return _quarter_bounds(year, q)


def _h_month_year(m, today):
    """March 2024 / Mar 2024"""
    month = _MONTH_ABBR.get(m.group(1).lower()[:3], None)
    if not month:
        return None, None
    raw_year = int(m.group(2))
    year = raw_year if raw_year > 100 else 2000 + raw_year
    d = date(year, month, 1)
    return _start_of_month(d), _end_of_month(d)


def _h_year_only(m, today):
    year = int(m.group(1))
    if year < 1900 or year > today.year + 5:
        return None, None
    return date(year, 1, 1), date(year, 12, 31)


def _h_fy_year(m, today):
    """FY2025 / FY25 → Apr 2024 – Mar 2025"""
    raw = int(m.group(1))
    year = raw if raw > 100 else 2000 + raw
    return date(year - 1, 4, 1), date(year, 3, 31)


def _h_since_year(m, today):
    year = int(m.group(1))
    return date(year, 1, 1), today


def _h_before_year(m, today):
    year = int(m.group(1))
    return date(1970, 1, 1), date(year - 1, 12, 31)


def _h_from_to(m, today):
    """from 2022 to 2024 / 2022-2024"""
    y1 = int(m.group(1))
    y2 = int(m.group(2))
    if y1 > y2:
        y1, y2 = y2, y1
    return date(y1, 1, 1), date(y2, 12, 31)


_PATTERNS: list[tuple] = [
    # --- relative days/weeks/months/years ---
    (_p(r"last\s+(\d+)\s+days?"),          _h_last_n_days),
    (_p(r"past\s+(\d+)\s+days?"),          _h_last_n_days),
    (_p(r"(\d+)\s+days?\s+ago"),            _h_last_n_days),
    (_p(r"last\s+(\d+)\s+weeks?"),         _h_last_n_weeks),
    (_p(r"last\s+(\d+)\s+months?"),        _h_last_n_months),
    (_p(r"past\s+(\d+)\s+months?"),        _h_last_n_months),
    (_p(r"last\s+(\d+)\s+years?"),         _h_last_n_years),
    # --- named periods ---
    (_p(r"\byesterday\b"),                  _h_yesterday),
    (_p(r"\btoday\b"),                      _h_today),
    (_p(r"\bthis\s+week\b"),               _h_this_week),
    (_p(r"\blast\s+week\b"),               _h_last_week),
    (_p(r"\bthis\s+month\b"),              _h_this_month),
    (_p(r"\blast\s+month\b"),              _h_last_month),
    (_p(r"\bthis\s+year\b"),               _h_this_year),
    (_p(r"\blast\s+year\b"),               _h_last_year),
    (_p(r"\bthis\s+quarter\b"),            _h_this_quarter),
    (_p(r"\blast\s+quarter\b"),            _h_last_quarter),
    # --- MTD / QTD / YTD ---
    (_p(r"\bmonth[\s\-]?to[\s\-]?date\b|\bmtd\b"), _h_mtd),
    (_p(r"\bquarter[\s\-]?to[\s\-]?date\b|\bqtd\b"), _h_qtd),
    (_p(r"\byear[\s\-]?to[\s\-]?date\b|\bytd\b"),  _h_ytd),
    # --- Q2 2024 / Q2FY25 / Q2 FY2025 ---
    (_p(r"\bq([1-4])\s*(?:fy|fiscal\s*year\s*)(\d{2,4})\b"), _h_qN_year),
    (_p(r"\bq([1-4])\s+(\d{4})\b"),        _h_qN_year),
    # --- FY2025 / FY25 ---
    (_p(r"\bfy\s*(\d{2,4})\b"),            _h_fy_year),
    # --- Month Year: March 2024 / Mar 2024 ---
    (_p(r"\b(january|february|march|april|may|june|july|august|september|"
        r"october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)"
        r"\s+(\d{4})\b"),                   _h_month_year),
    # --- from 2022 to 2024 / 2022-2024 ---
    (_p(r"\bfrom\s+(\d{4})\s+to\s+(\d{4})\b"), _h_from_to),
    (_p(r"\b(\d{4})\s*[-–]\s*(\d{4})\b"),  _h_from_to),
    # --- since / before ---
    (_p(r"\bsince\s+(\d{4})\b"),           _h_since_year),
    (_p(r"\bbefore\s+(\d{4})\b"),          _h_before_year),
    # --- bare year (lowest priority, only 4-digit 19xx/20xx) ---
    (_p(r"\b((?:19|20)\d{2})\b"),          _h_year_only),
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_temporal(
    query: str,
    today: date | None = None,
) -> tuple[date | None, date | None]:
    """
    Scan `query` for the first matching temporal expression.
    Returns (date_from, date_to) as date objects, or (None, None) if none found.

    Parameters
    ----------
    query : str
        Raw user query, e.g. "show me invoice data from last quarter"
    today : date, optional
        Override for deterministic unit tests. Defaults to date.today().

    Examples
    --------
    >>> parse_temporal("AR aging for last quarter", today=date(2026, 4, 24))
    (date(2026, 1, 1), date(2026, 3, 31))

    >>> parse_temporal("FY2025 revenue trend")
    (date(2024, 4, 1), date(2025, 3, 31))

    >>> parse_temporal("who are my top customers")
    (None, None)
    """
    if not query or not query.strip():
        return None, None

    if today is None:
        today = date.today()

    for pattern, handler in _PATTERNS:
        m = pattern.search(query)
        if m:
            result = handler(m, today)
            if result[0] is not None:
                return result

    return None, None
