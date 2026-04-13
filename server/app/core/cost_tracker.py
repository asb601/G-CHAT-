"""
Cost tracker — unified session accumulator for LLM calls and Azure Blob operations.

Every LLM call and every Azure blob transfer is recorded here in one place so you can
see, at any moment, exactly how much this session has spent and on what.

Writes to logs/costs.log (one JSON line per event, rotating at 10 MB).

Usage:
    from app.core.cost_tracker import track_llm, track_azure_blob, get_session_summary

    # called automatically by ai_client._track_and_log — you don't call this manually
    track_llm("generate_sql", "gpt-4o-mini", 845, 210, 0.00023, 1340.2)

    # called automatically by parquet_service after each download/upload
    track_azure_blob("download", "files/data.csv", size_bytes=3_000_000_000, duration_ms=87_000)
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone

# ─── Azure Blob Storage pricing (USD, East US, LRS Hot Tier, April 2026) ──────
# Source: https://azure.microsoft.com/en-us/pricing/details/storage/blobs/
#
# NOTE: If your app server runs inside Azure in the SAME region as the storage
# account, egress is FREE. Set AZURE_EGRESS_FREE = True in that case.
#
# For a local / non-Azure server:
#   Egress 0-10 GB/month: $0.087/GB
#   Egress 10-50 GB/month: $0.083/GB  (we use flat $0.087 to be conservative)
#   Write ops: $0.055 per 10,000 = $0.0000055 each
#   Read  ops: $0.044 per 100,000 = $0.00000044 each
AZURE_EGRESS_FREE = False          # change to True if server is co-located in Azure
_AZURE_EGRESS_PER_GB = 0.087       # USD — applies when AZURE_EGRESS_FREE is False
_AZURE_WRITE_PER_OP = 0.0000055    # USD per upload (PutBlob)
_AZURE_READ_PER_OP  = 0.00000044   # USD per download (GetBlob)


def _calc_azure_cost(operation: str, size_bytes: int) -> float:
    """
    Returns estimated USD cost for one Azure Blob read or write.

    download = GetBlob op cost + egress cost (if server is not in Azure)
    upload   = PutBlob op cost only (ingress is always free)
    """
    if operation == "download":
        op_cost = _AZURE_READ_PER_OP
        egress_cost = 0.0 if AZURE_EGRESS_FREE else (size_bytes / 1_073_741_824) * _AZURE_EGRESS_PER_GB
        return round(op_cost + egress_cost, 8)
    elif operation == "upload":
        return round(_AZURE_WRITE_PER_OP, 8)
    return 0.0


# ─── Session accumulators ─────────────────────────────────────────────────────
_lock = threading.Lock()

_session: dict = {
    # LLM
    "llm_calls":            0,
    "llm_cost_usd":         0.0,
    "llm_prompt_tokens":    0,
    "llm_completion_tokens": 0,
    # Azure
    "azure_ops":            0,
    "azure_cost_usd":       0.0,
    "azure_bytes_in":       0,   # bytes downloaded FROM Azure to server
    "azure_bytes_out":      0,   # bytes uploaded TO Azure from server
    # Combined
    "total_cost_usd":       0.0,
}


def get_session_summary() -> dict:
    """Return a snapshot of the current session's accumulated costs and usage."""
    with _lock:
        snap = dict(_session)
    snap["llm_cost_usd"] = round(snap["llm_cost_usd"], 6)
    snap["azure_cost_usd"] = round(snap["azure_cost_usd"], 6)
    snap["total_cost_usd"] = round(snap["total_cost_usd"], 6)
    snap["azure_bytes_in_mb"] = round(snap["azure_bytes_in"] / 1024 / 1024, 2)
    snap["azure_bytes_out_mb"] = round(snap["azure_bytes_out"] / 1024 / 1024, 2)
    return snap


# ─── Lazy cost logger (avoids circular import with logger.py) ─────────────────
_cost_logger = None


def _get_cost_logger():
    global _cost_logger
    if _cost_logger is None:
        import structlog
        _cost_logger = structlog.get_logger("cost")
    return _cost_logger


# ─── Public tracking functions ────────────────────────────────────────────────

def track_llm(
    function: str,
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
    cost_usd: float,
    duration_ms: float,
) -> None:
    """
    Record one LLM call.  Called automatically by ai_client._track_and_log —
    you don't need to call this directly.
    """
    with _lock:
        _session["llm_calls"] += 1
        _session["llm_cost_usd"] += cost_usd
        _session["llm_prompt_tokens"] += prompt_tokens
        _session["llm_completion_tokens"] += completion_tokens
        _session["total_cost_usd"] += cost_usd
        snap = {
            "llm_calls":             _session["llm_calls"],
            "session_llm_usd":       round(_session["llm_cost_usd"], 6),
            "session_total_usd":     round(_session["total_cost_usd"], 6),
        }

    _get_cost_logger().info(
        "llm_call",
        function=function,
        model=model,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        call_cost_usd=round(cost_usd, 8),
        duration_ms=duration_ms,
        **snap,
    )


def track_azure_blob(
    operation: str,          # "download" | "upload"
    blob_path: str,
    size_bytes: int,
    duration_ms: float,
) -> None:
    """
    Record one Azure Blob download or upload with estimated cost.
    Call this after the operation completes so you have the real size.
    """
    cost_usd = _calc_azure_cost(operation, size_bytes)
    size_mb = round(size_bytes / 1024 / 1024, 2)

    with _lock:
        _session["azure_ops"] += 1
        _session["azure_cost_usd"] += cost_usd
        _session["total_cost_usd"] += cost_usd
        if operation == "download":
            _session["azure_bytes_in"] += size_bytes
        else:
            _session["azure_bytes_out"] += size_bytes
        snap = {
            "azure_ops":             _session["azure_ops"],
            "session_azure_usd":     round(_session["azure_cost_usd"], 6),
            "session_total_usd":     round(_session["total_cost_usd"], 6),
        }

    _get_cost_logger().info(
        "azure_blob",
        operation=operation,
        blob_path=blob_path,
        size_mb=size_mb,
        call_cost_usd=round(cost_usd, 8),
        duration_ms=duration_ms,
        egress_free=AZURE_EGRESS_FREE,
        **snap,
    )
