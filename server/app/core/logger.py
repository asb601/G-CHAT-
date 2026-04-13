import json
import logging
import logging.handlers
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


# ── Routing filter: only accept specific logger names ────────────────────────
class _NameFilter(logging.Filter):
    """Accept records only from loggers whose name is in the allowed set."""

    def __init__(self, allowed: set[str]):
        super().__init__()
        self._allowed = allowed

    def filter(self, record: logging.LogRecord) -> bool:
        return record.name in self._allowed


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

# Console — human-readable, no filter (shows everything)
_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.DEBUG)
_console_handler.setFormatter(logging.Formatter("%(message)s"))

# Root stdlib logger receives structlog output and fans out to all handlers
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[_console_handler, _system_handler, _ai_handler, _llm_handler, _cost_handler],
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
