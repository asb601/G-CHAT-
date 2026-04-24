import json
import logging
import logging.handlers
import textwrap
from pathlib import Path

import structlog

# ── Log directory ────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parent.parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ── Logger categories ────────────────────────────────────────────────────────
_SYSTEM_LOGGERS = {"upload", "folder", "container", "auth", "blob", "db", "users"}
_AI_LOGGERS = {"chat", "ingest"}
_LLM_LOGGERS = {"llm"}
_COST_LOGGERS = {"cost"}
_PIPELINE_LOGGERS = {"pipeline"}  # deep trace: full prompts, SQL, tool I/O, LLM outputs


# ── Routing filter: only accept specific logger names ────────────────────────
class _NameFilter(logging.Filter):
    """Accept records only from loggers whose name is in the allowed set."""
    def __init__(self, allowed: set[str]):
        super().__init__()
        self._allowed = allowed

    def filter(self, record: logging.LogRecord) -> bool:
        return record.name in self._allowed


class _ExcludeFilter(logging.Filter):
    """Reject records whose logger name is in the excluded set."""
    def __init__(self, excluded: set[str]):
        super().__init__()
        self._excluded = excluded

    def filter(self, record: logging.LogRecord) -> bool:
        return record.name not in self._excluded


# ── Pretty console formatter for pipeline events ─────────────────────────────
_W = 80  # console width
_DIV  = "─" * _W
_DIV2 = "═" * _W


def _wrap(text: str, indent: int = 4) -> str:
    prefix = " " * indent
    return textwrap.fill(str(text), width=_W - indent, initial_indent=prefix,
                         subsequent_indent=prefix, break_long_words=False)


def _fmt_rows(rows: list[dict], max_rows: int = 5) -> str:
    if not rows:
        return "    (no rows)"
    cols = list(rows[0].keys())
    col_w = max(12, (_W - 4) // max(len(cols), 1))
    header = "  " + " │ ".join(c[:col_w].ljust(col_w) for c in cols)
    sep    = "  " + "─┼─".join("─" * col_w for _ in cols)
    lines  = [header, sep]
    for row in rows[:max_rows]:
        lines.append("  " + " │ ".join(str(row.get(c, ""))[:col_w].ljust(col_w) for c in cols))
    if len(rows) > max_rows:
        lines.append(f"  … {len(rows) - max_rows} more rows")
    return "\n".join(lines)


class _PipelinePrettyFormatter(logging.Formatter):
    """Parses structlog JSON from pipeline logger and renders it as human-readable blocks."""

    def format(self, record: logging.LogRecord) -> str:
        try:
            ev = json.loads(record.getMessage())
        except (json.JSONDecodeError, ValueError):
            return record.getMessage()

        name = ev.get("event", "")
        ts   = ev.get("timestamp", "")[:19].replace("T", " ")

        if name == "query_received":
            return self._query_received(ev, ts)
        if name == "catalog_loaded":
            return self._catalog_loaded(ev, ts)
        if name == "catalog_empty":
            return self._catalog_empty(ev, ts)
        if name == "final_answer":
            return self._final_answer(ev, ts)
        if name == "system_prompt_built":
            return self._system_prompt(ev, ts)
        if name in ("llm_input", "llm_stream_input"):
            return self._llm_input(ev, ts)
        if name in ("llm_output", "llm_stream_output"):
            return self._llm_output(ev, ts)
        if name == "tool_call_start":
            return self._tool_start(ev, ts)
        if name == "tool_call_end":
            return self._tool_end(ev, ts)
        if name == "sql_execute_start":
            return self._sql_start(ev, ts)
        if name == "sql_execute_done":
            return self._sql_done(ev, ts)
        if name == "sql_execute_error":
            return self._sql_error(ev, ts)
        if name == "search_catalog":
            return self._catalog(ev, ts)
        if name == "get_file_schema":
            return self._schema(ev, ts)
        if name == "inspect_data_format":
            return self._sample(ev, ts)
        if name == "summarise_dataframe_done":
            return self._stats(ev, ts)
        if name == "ingest_llm_prompt":
            return self._ingest_prompt(ev, ts)
        if name == "ingest_llm_response":
            return self._ingest_response(ev, ts)
        # fallback — still pretty-print unknown pipeline events
        return f"\n[PIPELINE {ts}] {name}\n" + json.dumps(ev, indent=2, default=str)[:600]

    # ── step header helper ───────────────────────────────────────────────────────
    @staticmethod
    def _step_header(num: str, title: str, ts: str) -> str:
        title_full = f"  STEP {num}  │  {title}  │  {ts}"
        return f"\n{_DIV2}\n{title_full}\n{_DIV2}"

    # ── STEP 1: query received ────────────────────────────────────────────────────
    def _query_received(self, ev: dict, ts: str) -> str:
        q       = ev.get("query", "")
        has_ctx = ev.get("has_conversation_context", False)
        ctx_pre = ev.get("conversation_context_preview", "")
        lines = [
            self._step_header("1", "USER QUERY RECEIVED", ts),
            f"  ▶ Query: {q}",
            f"  ▶ Conversation context: {'yes' if has_ctx else 'none'}",
        ]
        if has_ctx and ctx_pre:
            lines.append("  ▶ Context preview:")
            for ln in ctx_pre.splitlines()[:8]:
                lines.append("      " + ln)
        return "\n".join(lines)

    # ── STEP 2: catalog loaded ───────────────────────────────────────────────────
    def _catalog_loaded(self, ev: dict, ts: str) -> str:
        files = ev.get("files", [])
        lines = [
            self._step_header("2", "CATALOG LOADED FROM DB", ts),
            f"  Container          : {ev.get('container', '')}",
            f"  Files in catalog   : {ev.get('file_count', 0)}",
            f"  Parquet-ready files: {ev.get('parquet_count', 0)}",
            f"  Relationships      : {ev.get('relationship_count', 0)}",
            "  Files available to the agent:",
        ]
        for f in files[:50]:
            lines.append(f"    • {f}")
        if len(files) > 50:
            lines.append(f"    … {len(files) - 50} more")
        return "\n".join(lines)

    def _catalog_empty(self, ev: dict, ts: str) -> str:
        return (
            f"\n{_DIV2}\n"
            f"  STEP 2  │  CATALOG EMPTY  │  {ts}\n"
            f"{_DIV2}\n"
            f"  ✗ No files have been ingested yet — agent cannot answer.\n"
            f"  Query was: {ev.get('query', '')}"
        )

    # ── FINAL STEP: answer ready ──────────────────────────────────────────────────
    def _final_answer(self, ev: dict, ts: str) -> str:
        ans = str(ev.get("answer", ""))
        lines = [
            self._step_header("FINAL", "ANSWER DELIVERED TO USER", ts),
            f"  Tool calls used : {ev.get('tool_calls', 0)}",
            f"  Rows returned   : {ev.get('row_count', 0)}",
            f"  Total time      : {ev.get('total_duration_ms', '?')} ms",
            f"  Query           : {ev.get('query', '')}",
            "  Answer:",
            _DIV,
        ]
        for ln in ans.splitlines():
            lines.append("  " + ln)
        lines.append(_DIV2)
        return "\n".join(lines)

    # ── individual renderers ──────────────────────────────────────────────────

    def _system_prompt(self, ev: dict, ts: str) -> str:
        files   = ev.get("catalog_file_count", "?")
        parquet = ev.get("parquet_file_count", "?")
        rels    = ev.get("has_relationships", False)
        ctx     = ev.get("has_conversation_context", False)
        prompt  = ev.get("system_prompt", "")
        lines   = [
            self._step_header("3", "SYSTEM PROMPT BUILT (sent to LLM)", ts),
            f"  Query      : {ev.get('query', '')}",
            f"  Container  : {ev.get('container', '')}",
            f"  Files in catalog  : {files}  |  Parquet-ready: {parquet}",
            f"  Relationships     : {'yes' if rels else 'none'}  |  Conv context: {'yes' if ctx else 'none'}",
            _DIV,
            "  FULL PROMPT SENT TO LLM:",
            _DIV,
        ]
        for line in prompt.splitlines():
            lines.append("  " + line)
        lines.append(_DIV2)
        return "\n".join(lines)

    def _llm_input(self, ev: dict, ts: str) -> str:
        msgs   = ev.get("messages", [])
        iter_n = ev.get("iteration", "?")
        lines  = [
            f"\n{_DIV}",
            f"  ▶  LLM INPUT  — Iteration {iter_n}  [{ts}]  ({len(msgs)} messages)",
            _DIV,
        ]
        for i, m in enumerate(msgs):
            mtype   = m.get("type", "?")
            content = str(m.get("content", ""))
            tcs     = m.get("tool_calls", [])
            tid     = m.get("tool_call_id", "")
            role_label = {
                "SystemMessage":   "SYSTEM",
                "HumanMessage":    "USER  ",
                "AIMessage":       "AI    ",
                "ToolMessage":     "TOOL  ",
            }.get(mtype, mtype.upper()[:6])
            lines.append(f"  [{i+1}] {role_label} ─────────────────────────")
            if content:
                for line in content.splitlines()[:60]:   # first 60 lines of each message
                    lines.append("    " + line)
                if len(content.splitlines()) > 60:
                    lines.append(f"    … ({len(content.splitlines()) - 60} more lines)")
            if tid:
                lines.append(f"    tool_call_id: {tid}")
            if tcs:
                lines.append("    Tool calls decided:")
                for tc in tcs:
                    lines.append(f"      → {tc.get('name')}({json.dumps(tc.get('args', {}), default=str)})")
        lines.append(_DIV)
        return "\n".join(lines)

    def _llm_output(self, ev: dict, ts: str) -> str:
        iter_n  = ev.get("iteration", "?")
        content = str(ev.get("content", ""))
        tcs     = ev.get("tool_calls", [])
        p_tok   = ev.get("prompt_tokens", "?")
        c_tok   = ev.get("completion_tokens", "?")
        dur     = ev.get("duration_ms", "?")
        title   = f"LLM DECISION (iteration {iter_n})"
        lines   = [
            self._step_header("4", title, ts),
            f"  Tokens: {p_tok} prompt + {c_tok} completion  |  {dur} ms",
            _DIV,
        ]
        if tcs:
            lines.append(f"  ▶ LLM decided to call {len(tcs)} tool(s):")
            for tc in tcs:
                lines.append(f"    ┌─ tool : {tc.get('name')}")
                args = tc.get("args", {})
                for k, v in args.items():
                    vstr = str(v)
                    if "\n" in vstr:
                        lines.append(f"    │  {k}:")
                        for vline in vstr.splitlines():
                            lines.append(f"    │      {vline}")
                    else:
                        lines.append(f"    │  {k} = {vstr}")
                lines.append("    └" + "─" * 40)
        elif content:
            lines.append("  ▶ LLM decided: generate final answer (no more tool calls)")
            lines.append("  ┌─ answer:")
            for line in content.splitlines()[:30]:
                lines.append("  │  " + line)
            if len(content.splitlines()) > 30:
                lines.append(f"  │  … ({len(content.splitlines()) - 30} more lines)")
            lines.append("  └" + "─" * 40)
        lines.append(_DIV)
        return "\n".join(lines)

    def _tool_start(self, ev: dict, ts: str) -> str:
        tool    = ev.get("tool", "?")
        iter_n  = ev.get("iteration", "?")
        inp     = ev.get("input", {})
        lines   = [
            f"\n  ⚙  TOOL START  [{ts}]  #{iter_n}  →  {tool}",
        ]
        if inp:
            inp_str = json.dumps(inp, indent=4, default=str)
            for line in inp_str.splitlines():
                lines.append("    " + line)
        return "\n".join(lines)

    def _tool_end(self, ev: dict, ts: str) -> str:
        tool   = ev.get("tool", "?")
        out    = str(ev.get("output", ""))
        # Try to pretty-print if JSON
        try:
            parsed = json.loads(out)
            out_pretty = json.dumps(parsed, indent=4, default=str)
        except Exception:
            out_pretty = out
        lines = [f"  ✓  TOOL END   [{ts}]  ←  {tool}"]
        out_lines = out_pretty.splitlines()
        for line in out_lines[:40]:
            lines.append("    " + line)
        if len(out_lines) > 40:
            lines.append(f"    … ({len(out_lines) - 40} more lines in pipeline.log)")
        return "\n".join(lines)

    def _sql_start(self, ev: dict, ts: str) -> str:
        sql = ev.get("sql", "")
        lines = [
            self._step_header("5", "DUCKDB SQL EXECUTING", ts),
            "  SQL being run:",
            _DIV,
        ]
        for line in sql.splitlines():
            lines.append("  " + line)
        lines.append(_DIV)
        return "\n".join(lines)

    def _sql_done(self, ev: dict, ts: str) -> str:
        dur      = ev.get("duration_ms", "?")
        returned = ev.get("rows_returned", "?")
        total    = ev.get("total_rows", "?")
        cols     = ev.get("columns", [])
        rows     = ev.get("preview_rows", [])
        lines    = [
            self._step_header("6", "DUCKDB RESULT RETURNED", ts),
            f"  ✓ {returned}/{total} rows  |  {dur} ms",
            f"  Columns: {', '.join(str(c) for c in cols)}",
        ]
        if rows:
            lines.append(_fmt_rows(rows, max_rows=8))
        lines.append("")
        return "\n".join(lines)

    def _sql_error(self, ev: dict, ts: str) -> str:
        sql = ev.get("sql", "")
        err = ev.get("error", "")
        return (
            f"\n{_DIV2}\n"
            f"  STEP 6  │  ✗ DUCKDB SQL FAILED  │  {ts}\n"
            f"{_DIV2}\n"
            f"  SQL : {sql[:300]}\n"
            f"  ERR : {err}\n"
        )

    def _catalog(self, ev: dict, ts: str) -> str:
        query = ev.get("query", "")
        files = ev.get("matched_files", [])
        descs = ev.get("result_descriptions", [])
        lines = [
            f"\n  🔍 CATALOG SEARCH  [{ts}]  (tool: search_catalog)",
            f"     Query  : {query}",
            f"     Matched: {len(files)} file(s) — names only:",
        ]
        for f in files:
            lines.append(f"       • {f}")
        # descriptions kept short / behind matched list
        if descs and any(descs):
            lines.append("     First match summary:")
            lines.append(_wrap(descs[0], indent=8))
        return "\n".join(lines)

    def _schema(self, ev: dict, ts: str) -> str:
        blob  = ev.get("blob_path", "")
        found = ev.get("found", False)
        cols  = ev.get("columns", [])
        types = ev.get("column_types", {})
        svals = ev.get("sample_values", {})
        lines = [
            f"\n  📋 FILE SCHEMA  [{ts}]",
            f"     File : {blob}  ({'found' if found else 'NOT FOUND'})",
        ]
        for c in cols:
            sv = svals.get(c, [])
            sv_str = ", ".join(str(v) for v in sv[:3]) if sv else "—"
            lines.append(f"     {c:<30} {types.get(c, ''):<15}  e.g. {sv_str}")
        return "\n".join(lines)

    def _sample(self, ev: dict, ts: str) -> str:
        cols  = ev.get("columns", [])
        rows  = ev.get("rows", [])
        lines = [
            f"\n  👁  INSPECT DATA FORMAT  [{ts}]",
            f"     Columns: {', '.join(cols)}",
            _fmt_rows(rows, max_rows=5),
        ]
        return "\n".join(lines)

    def _stats(self, ev: dict, ts: str) -> str:
        focus   = ev.get("focus", "")
        rc      = ev.get("row_count", "?")
        cc      = ev.get("column_count", "?")
        summary = ev.get("columns_summary", {})
        lines   = [
            f"\n  📊 DATAFRAME STATS  [{ts}]",
            f"     Focus  : {focus}",
            f"     Rows   : {rc}  |  Cols: {cc}",
        ]
        for col, info in list(summary.items())[:10]:
            dtype = info.get("dtype", "")
            nulls = info.get("nulls", 0)
            if "mean" in info:
                lines.append(f"     {col:<28} {dtype:<12}  min={info.get('min')}  max={info.get('max')}  mean={info.get('mean')}  nulls={nulls}")
            else:
                top = list((info.get("top_values") or {}).items())[:3]
                top_str = ", ".join(f"{k}:{v}" for k, v in top)
                lines.append(f"     {col:<28} {dtype:<12}  top=[{top_str}]  nulls={nulls}")
        return "\n".join(lines)

    def _ingest_prompt(self, ev: dict, ts: str) -> str:
        fname  = ev.get("filename", "")
        p_tok  = ev.get("estimated_prompt_tokens", "?")
        prompt = ev.get("prompt", "")
        lines  = [
            f"\n{_DIV}",
            f"  INGEST LLM PROMPT  [{ts}]  file={fname}  est_tokens={p_tok}",
            _DIV,
        ]
        for line in prompt.splitlines():
            lines.append("  " + line)
        lines.append(_DIV)
        return "\n".join(lines)

    def _ingest_response(self, ev: dict, ts: str) -> str:
        fname = ev.get("filename", "")
        dur   = ev.get("duration_ms", "?")
        p_tok = ev.get("prompt_tokens", "?")
        c_tok = ev.get("completion_tokens", "?")
        raw   = ev.get("raw_response", "")
        lines = [
            f"\n  INGEST LLM RESPONSE  [{ts}]  file={fname}  {p_tok}+{c_tok} tokens  {dur} ms",
            _DIV,
        ]
        for line in raw.splitlines():
            lines.append("  " + line)
        lines.append(_DIV)
        return "\n".join(lines)

# ── Public helper: format one raw pipeline JSON line as pretty text ───────────
_pretty_formatter = _PipelinePrettyFormatter()


def format_pipeline_line(json_line: str) -> str:
    """Format a single pipeline.log JSON line into human-readable text.

    Used by the /logs/pipeline/stream and /logs/pipeline/tail HTTP endpoints
    so developers can view live pipeline traces without SSH access.
    """
    record = logging.LogRecord(
        name="pipeline", level=logging.INFO, pathname="", lineno=0,
        msg=json_line.strip(), args=(), exc_info=None,
    )
    return _pretty_formatter.format(record)

# ── JSON file handler factory ────────────────────────────────────────────────
def _make_json_handler(
    filename: str, allowed: set[str]
) -> logging.handlers.RotatingFileHandler:
    handler = logging.handlers.RotatingFileHandler(
        LOG_DIR / filename,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(message)s"))
    handler.addFilter(_NameFilter(allowed))
    return handler


# System logs       → logs/system.log
_system_handler = _make_json_handler("system.log", _SYSTEM_LOGGERS)

# AI pipeline logs  → logs/ai_pipeline.log
_ai_handler = _make_json_handler("ai_pipeline.log", _AI_LOGGERS)

# LLM call logs     → logs/llm_calls.log  (every individual LLM invocation)
_llm_handler = _make_json_handler("llm_calls.log", _LLM_LOGGERS)

# Cost / money logs  → logs/costs.log  (every LLM + Azure blob cost event)
_cost_handler = _make_json_handler("costs.log", _COST_LOGGERS)

# Pipeline deep-trace logs  → logs/pipeline.log
# Contains: full prompts, full SQL, full LLM tool-call args, full tool outputs
_pipeline_handler = _make_json_handler("pipeline.log", _PIPELINE_LOGGERS)

# Console (non-pipeline) — raw JSON, no filter except exclude pipeline
_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.DEBUG)
_console_handler.setFormatter(logging.Formatter("%(message)s"))
_console_handler.addFilter(_ExcludeFilter(_PIPELINE_LOGGERS))

# Console (pipeline only) — pretty human-readable formatter
_pipeline_console_handler = logging.StreamHandler()
_pipeline_console_handler.setLevel(logging.DEBUG)
_pipeline_console_handler.setFormatter(_PipelinePrettyFormatter())
_pipeline_console_handler.addFilter(_NameFilter(_PIPELINE_LOGGERS))

# Root stdlib logger receives structlog output and fans out to all handlers
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[
        _console_handler,
        _pipeline_console_handler,
        _system_handler,
        _ai_handler,
        _llm_handler,
        _cost_handler,
        _pipeline_handler,
    ],
    force=True,
)

# Silence noisy third-party loggers
for _name in ("httpx", "httpcore", "openai", "azure", "urllib3", "asyncio"):
    logging.getLogger(_name).setLevel(logging.WARNING)


# ── structlog config ─────────────────────────────────────────────────────────
def _json_renderer(logger: object, method: str, event_dict: dict) -> str:
    """Render structured log as a single-line JSON string for file handlers."""
    return json.dumps(event_dict, default=str, ensure_ascii=False)


# Console gets colored dev output; file handlers get JSON via stdlib passthrough
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        _json_renderer,
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
    logger_factory=structlog.stdlib.LoggerFactory(),
)


# ── Category loggers ─────────────────────────────────────────────────────────
# System
upload_logger = structlog.get_logger("upload")
folder_logger = structlog.get_logger("folder")
container_logger = structlog.get_logger("container")
auth_logger = structlog.get_logger("auth")
blob_logger = structlog.get_logger("blob")
db_logger = structlog.get_logger("db")

# AI pipeline
chat_logger = structlog.get_logger("chat")
ingest_logger = structlog.get_logger("ingest")

# LLM call-level logger (every individual OpenAI invocation)
llm_logger = structlog.get_logger("llm")

# Cost / money logger (every LLM + Azure blob cost event → costs.log)
cost_logger = structlog.get_logger("cost")

# Pipeline deep-trace logger (full prompts, SQL, tool I/O → pipeline.log)
pipeline_logger = structlog.get_logger("pipeline")
