"""Column type detection — composable detector registry.

Why this module exists
----------------------
Type detection during ingestion used to live inside data_preprocessor.py as a
~250-line `_build_converters` function with scattered helpers. That function
hardcoded every rule for every supported file type, so each new edge case
(LEDGER_ID being misread as a date, INVOICE_NUM losing leading zeros) added
another patch on top of an unreviewable pile.

This module replaces that pattern with a small, ordered registry of focused
detectors. Each detector has ONE responsibility, ONE file, and is independently
unit-testable.

Detection contract
------------------
A detector implements the `TypeDetector` Protocol:

    name: str
    def detect(col_name: str, sample: pd.Series) -> ColumnConverter | None

Returning None means "I don't claim this column" — registry tries the next
detector. Returning a ColumnConverter means "I'm taking it" — registry stops.

Order matters: identifier first (so we never coerce IDs), then boolean (most
restrictive value set), then date (semantic strongest), then numeric (broadest).
Adding a new detector = appending one class. No rewrites of existing code.

Public entry point
------------------
    detect_column_converter(col_name, sample) -> ColumnConverter | None

Replaces the old `_build_converters` per-column inner loop. The caller still
decides whether to install a converter (None = pass-through-as-string).
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date as _date_type, datetime, timedelta
from typing import Callable, Optional, Protocol

import numpy as np
import pandas as pd

from app.core.logger import ingest_logger


# ── Public dataclass ─────────────────────────────────────────────────────────

ConverterFn = Callable[[object], object]


@dataclass(frozen=True)
class ColumnConverter:
    """The result of a successful detection.

    Attributes:
        type_name: One of "identifier" | "boolean" | "date" | "numeric".
                   Purely informational — used in logs and warnings.
        convert:   Cell-by-cell function. Idempotent. Returns None for nulls.
                   Must NEVER raise — invalid values return the original value
                   unchanged so downstream code can decide what to do with them.
        detector:  Name of the detector that produced this converter.
    """
    type_name: str
    convert: ConverterFn
    detector: str


# ── Detector Protocol ────────────────────────────────────────────────────────

class TypeDetector(Protocol):
    """A single type detector. Stateless. Cheap to instantiate."""

    name: str

    def detect(
        self,
        col_name: str,
        sample: pd.Series,  # already dropna()'d, may still be empty
    ) -> Optional[ColumnConverter]:
        """Return a ColumnConverter if this detector claims the column, else None."""
        ...


# ══════════════════════════════════════════════════════════════════════════════
# Detector 1 — Identifier
# ══════════════════════════════════════════════════════════════════════════════

class IdentifierDetector:
    """Columns that name themselves as identifiers are NEVER coerced.

    Rule: column NAME is the source of truth, not values. A column called
    LEDGER_ID is an identifier even if its values look like years.

    The converter is a string-passthrough so leading zeros and exact original
    formatting are preserved verbatim.
    """

    name = "identifier"

    # Match by: exact name, suffix, or prefix.
    # Conservative additions only — when in doubt, omit (false negatives are
    # safe: numeric/date detector still gets a chance).
    EXACT_NAMES = frozenset({
        "id", "uuid", "guid", "sku", "isin", "cusip", "ein", "ssn",
        "ledger_id", "org_id", "set_of_books_id", "period_name",
    })

    SUFFIXES = (
        "_id", "_num", "_number", "_no", "_key", "_code", "_ref", "_uuid",
    )

    PREFIXES = (
        "ledger_", "code_combination", "voucher", "invoice_num", "invoice_no",
        "journal_", "transaction_id", "doc_num", "document_num",
    )

    @staticmethod
    def _identity_converter(value: object) -> object:
        if value is None:
            return None
        # Preserve exact string form — don't strip, don't normalize, don't cast.
        # The CSV writer will quote when needed.
        return str(value)

    def detect(self, col_name: str, sample: pd.Series) -> Optional[ColumnConverter]:
        normalized = (col_name or "").strip().lower()
        if not normalized:
            return None
        is_identifier = (
            normalized in self.EXACT_NAMES
            or normalized.endswith(self.SUFFIXES)
            or normalized.startswith(self.PREFIXES)
        )
        if not is_identifier:
            return None
        return ColumnConverter(
            type_name="identifier",
            convert=self._identity_converter,
            detector=self.name,
        )


# ══════════════════════════════════════════════════════════════════════════════
# Detector 2 — Boolean
# ══════════════════════════════════════════════════════════════════════════════

class BooleanDetector:
    """Columns whose entire value set is a known boolean vocabulary."""

    name = "boolean"

    TRUE_VALUES = frozenset({
        "yes", "y", "true", "t", "1", "on", "enabled", "active", "x", "\u2713",
    })
    FALSE_VALUES = frozenset({
        "no", "n", "false", "f", "0", "off", "disabled", "inactive", "\u2717",
    })

    @classmethod
    def _all_known(cls, values: set[str]) -> bool:
        # Empty set means we have nothing to judge → not boolean.
        if not values:
            return False
        return values.issubset(cls.TRUE_VALUES | cls.FALSE_VALUES)

    @classmethod
    def _make_converter(cls) -> ConverterFn:
        true_set, false_set = cls.TRUE_VALUES, cls.FALSE_VALUES

        def _convert(value: object) -> object:
            if value is None:
                return None
            normalized = str(value).strip().lower()
            if normalized in true_set:
                return "True"
            if normalized in false_set:
                return "False"
            return None  # unknown value → null, not silently coerced
        return _convert

    def detect(self, col_name: str, sample: pd.Series) -> Optional[ColumnConverter]:
        if sample.empty:
            return None
        normalized = {str(v).strip().lower() for v in sample}
        if not self._all_known(normalized):
            return None
        return ColumnConverter(
            type_name="boolean",
            convert=self._make_converter(),
            detector=self.name,
        )


# ══════════════════════════════════════════════════════════════════════════════
# Detector 3 — Date
# ══════════════════════════════════════════════════════════════════════════════

class DateDetector:
    """Columns whose values predominantly parse as calendar dates.

    Two thresholds:
        * column name contains a date hint (date, dt, time, ...) → 0.55
        * otherwise                                              → 0.80

    Conservative parsing rules to prevent false positives:
        * Bare 4-digit ints are NEVER dates (would default to Jan 1 of that year).
        * MON-YY / MON-YYYY ERP period labels (e.g. "JAN-25") are NOT dates.
        * Pure-text inputs ("January") are NOT dates.
        * Years outside 1901..2100 are rejected.
    """

    name = "date"

    HINT_TOKENS = frozenset({
        "date", "dt", "time", "timestamp", "created", "updated", "modified",
        "dob", "birth", "expir", "effective", "since", "until",
        "_at", "at_", "start", "end", "from", "to", "week",
        "day", "posted", "issued", "received", "shipped", "closed",
    })

    # Excel date-serial range: ~1920 to ~2099.
    EXCEL_EPOCH = datetime(1899, 12, 30)
    EXCEL_SERIAL_MIN = 7300
    EXCEL_SERIAL_MAX = 73000

    HINT_THRESHOLD = 0.55
    NO_HINT_THRESHOLD = 0.80

    EXPLICIT_FORMATS = (
        "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d",
        "%d-%m-%Y", "%m-%d-%Y", "%d.%m.%Y", "%m.%d.%Y",
        "%Y%m%d", "%d %b %Y", "%b %d %Y", "%d %B %Y", "%B %d %Y",
        "%b-%d-%Y", "%d-%b-%Y", "%b %Y", "%B %Y",
    )

    _PERIOD_LABEL_RE = re.compile(r"^[A-Za-z]{3}-\d{2,4}$")
    _BARE_YEAR_RE = re.compile(r"^\d{4}$")

    # ── helpers ──────────────────────────────────────────────────────────────

    @classmethod
    def _excel_serial_to_iso(cls, n: float) -> Optional[str]:
        try:
            n_int = int(n)
            if not (cls.EXCEL_SERIAL_MIN <= n_int <= cls.EXCEL_SERIAL_MAX):
                return None
            dt = cls.EXCEL_EPOCH + timedelta(days=n_int)
        except (ValueError, OverflowError, OSError):
            return None
        return dt.strftime("%Y-%m-%d") if 1900 <= dt.year <= 2100 else None

    @classmethod
    def _parse_one(cls, value: object) -> Optional[str]:
        """Parse a single value into ISO date string or return None."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d") if 1900 <= value.year <= 2100 else None
        if isinstance(value, _date_type):
            return value.isoformat() if 1900 <= value.year <= 2100 else None

        text = str(value).strip()
        if not text:
            return None

        # Reject ERP period labels like "JAN-25" — they are categorical, not dates.
        if cls._PERIOD_LABEL_RE.match(text):
            return None

        # Reject bare 4-digit years — dateutil would default to Jan 1.
        if cls._BARE_YEAR_RE.match(text):
            return None

        # Try Excel serial (numeric input).
        try:
            f = float(text.replace(",", ""))
            if f == int(f):
                serial = cls._excel_serial_to_iso(f)
                if serial is not None:
                    return serial
        except (ValueError, TypeError):
            pass

        # Try dateutil for free-form strings (must contain at least one digit).
        if any(c.isdigit() for c in text):
            try:
                from dateutil import parser as _dp  # noqa: PLC0415
                parsed = _dp.parse(text, default=datetime(1900, 1, 1), dayfirst=False)
                # Reject 1900 — it's dateutil's fill-in default for missing year.
                if 1901 <= parsed.year <= 2100:
                    return parsed.strftime("%Y-%m-%d")
            except (ValueError, TypeError, OverflowError):
                pass

        # Last resort — explicit common formats.
        for fmt in cls.EXPLICIT_FORMATS:
            try:
                parsed = datetime.strptime(text, fmt)
            except ValueError:
                continue
            if 1900 <= parsed.year <= 2100:
                return parsed.strftime("%Y-%m-%d")

        return None

    @classmethod
    def _parse_ratio(cls, sample: pd.Series) -> float:
        if sample.empty:
            return 0.0
        return sample.apply(cls._parse_one).notna().sum() / len(sample)

    @classmethod
    def _make_converter(cls) -> ConverterFn:
        return lambda v: cls._parse_one(v)

    def detect(self, col_name: str, sample: pd.Series) -> Optional[ColumnConverter]:
        if sample.empty:
            return None
        is_hint = any(h in col_name.lower() for h in self.HINT_TOKENS)
        threshold = self.HINT_THRESHOLD if is_hint else self.NO_HINT_THRESHOLD
        if self._parse_ratio(sample) < threshold:
            return None
        return ColumnConverter(
            type_name="date",
            convert=self._make_converter(),
            detector=self.name,
        )


# ══════════════════════════════════════════════════════════════════════════════
# Detector 4 — Numeric
# ══════════════════════════════════════════════════════════════════════════════

class NumericDetector:
    """Columns whose values predominantly parse as numbers.

    Handles currency symbols, thousand separators (comma or space), percent
    signs, and invisible Unicode noise. Lower threshold (0.50) when the
    column name suggests a metric (amount, price, total, ...).
    """

    name = "numeric"

    HINT_TOKENS = frozenset({
        "amount", "price", "cost", "total", "sum", "count", "qty", "quantity",
        "balance", "rate", "pct", "percent", "ratio", "score", "revenue",
        "profit", "loss", "tax", "fee", "charge", "salary", "wage", "budget",
        "vol", "volume",
    })

    HINT_THRESHOLD = 0.50
    NO_HINT_THRESHOLD = 0.75

    # Pre-compiled regex / charsets.
    _CURRENCY_RE = re.compile(
        r"[$\u20b9\u20ac\xa3\xa5\u20a9\u20a6\u20b1\u20ba\u20b4\u20bd\xa2\u0e3f]+"
    )
    _SPACE_THOU_RE = re.compile(r"(\d)\s(\d)")
    _COMMA_THOU_RE = re.compile(r"^[+-]?[\d,]+\.?\d*$")
    _PERCENT_RE = re.compile(r"^([+-]?\d+\.?\d*)\s*%$")
    _INVISIBLE_RE = re.compile(
        r"[\u200b\u200c\u200d\u200e\u200f\ufeff\u00ad"
        r"\u180e\u2060\u2061\u2062\u2063\u2064\u3000]"
    )

    # ── helpers ──────────────────────────────────────────────────────────────

    @classmethod
    def _strip_noise(cls, raw: str) -> Optional[str]:
        v = cls._INVISIBLE_RE.sub("", raw.strip())
        v = unicodedata.normalize("NFKC", v)
        m = cls._PERCENT_RE.match(v)
        if m:
            try:
                return str(round(float(m.group(1)) / 100, 12))
            except ValueError:
                pass
        v = cls._CURRENCY_RE.sub("", v).strip()
        v = cls._SPACE_THOU_RE.sub(r"\1\2", v)
        if cls._COMMA_THOU_RE.match(v):
            v = v.replace(",", "")
        return v.strip() or None

    @classmethod
    def _is_parseable(cls, value: object) -> bool:
        if value is None:
            return False
        cleaned = cls._strip_noise(str(value))
        if not cleaned:
            return False
        try:
            float(cleaned)
        except (ValueError, TypeError):
            return False
        return True

    @classmethod
    def _parse_ratio(cls, sample: pd.Series) -> float:
        if sample.empty:
            return 0.0
        return sample.apply(cls._is_parseable).sum() / len(sample)

    @classmethod
    def _make_converter(cls) -> ConverterFn:
        strip = cls._strip_noise

        def _convert(value: object) -> object:
            if value is None:
                return None
            text = str(value).strip()
            if not text:
                return None
            cleaned = strip(text)
            if not cleaned:
                return value  # unparseable — leave alone for downstream
            try:
                f = float(cleaned)
            except (ValueError, OverflowError):
                return value
            if np.isnan(f) or np.isinf(f):
                return None
            if f == int(f) and "." not in cleaned:
                return str(int(f))
            return str(f)
        return _convert

    def detect(self, col_name: str, sample: pd.Series) -> Optional[ColumnConverter]:
        if sample.empty:
            return None
        is_hint = any(h in col_name.lower() for h in self.HINT_TOKENS)
        threshold = self.HINT_THRESHOLD if is_hint else self.NO_HINT_THRESHOLD
        if self._parse_ratio(sample) < threshold:
            return None
        return ColumnConverter(
            type_name="numeric",
            convert=self._make_converter(),
            detector=self.name,
        )


# ══════════════════════════════════════════════════════════════════════════════
# Registry
# ══════════════════════════════════════════════════════════════════════════════

class TypeDetectionRegistry:
    """Ordered registry. First detector that returns a converter wins."""

    def __init__(self, detectors: list[TypeDetector]) -> None:
        if not detectors:
            raise ValueError("TypeDetectionRegistry requires at least one detector")
        self._detectors = list(detectors)

    def detect(
        self,
        col_name: str,
        sample: pd.Series,
    ) -> Optional[ColumnConverter]:
        for detector in self._detectors:
            try:
                result = detector.detect(col_name, sample)
            except Exception as exc:
                ingest_logger.warning(
                    "type_detector_failed",
                    detector=detector.name,
                    column=col_name,
                    error=str(exc)[:200],
                )
                continue
            if result is not None:
                return result
        return None


# Default ordering. Identifier first to prevent value-based coercion of IDs.
DEFAULT_REGISTRY = TypeDetectionRegistry([
    IdentifierDetector(),
    BooleanDetector(),
    DateDetector(),
    NumericDetector(),
])


def detect_column_converter(
    col_name: str,
    sample: pd.Series,
    registry: TypeDetectionRegistry = DEFAULT_REGISTRY,
) -> Optional[ColumnConverter]:
    """Convenience wrapper around the default registry.

    Caller is responsible for dropping nulls before passing the sample.
    Returns None if no detector claims the column — caller should leave the
    column as raw strings in that case.
    """
    return registry.detect(col_name, sample)
