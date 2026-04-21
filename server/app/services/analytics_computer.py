"""Pure analytics computation on sample rows (no DB writes)."""
from __future__ import annotations

from typing import Any

import pandas as pd

_NUMERIC_TYPES = {
    "int64",
    "float64",
    "int32",
    "float32",
    "double",
    "bigint",
    "integer",
    "decimal",
    "numeric",
    "real",
}

_SKIP_COLS = {
    "id",
    "uuid",
    "session_id",
    "ip_address",
    "email",
    "phone",
    "description",
    "name",
    "created_at",
    "updated_at",
}


def json_safe_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def json_safe_rows(rows: list[dict]) -> list[dict]:
    return [{k: json_safe_value(v) for k, v in row.items()} for row in rows]


def is_numeric(col: dict) -> bool:
    dtype = (col.get("type") or "").lower()
    name = col.get("name", "").lower()
    if name in _SKIP_COLS:
        return False
    return any(t in dtype for t in _NUMERIC_TYPES)


def is_categorical(col: dict) -> bool:
    dtype = (col.get("type") or "").lower()
    name = col.get("name", "").lower()
    if name in _SKIP_COLS:
        return False
    return dtype in ("str", "object", "category") or any(
        t in dtype for t in ("varchar", "string", "char", "text")
    )


def round_value(val: Any) -> Any:
    if val is None:
        return None
    try:
        return round(float(val), 4)
    except (ValueError, TypeError):
        return val


def compute_sample_analytics(columns_info: list[dict], sample_rows: list[dict]) -> dict[str, Any]:
    """Compute column stats, value counts and cross-tabs from sample rows."""
    df = pd.DataFrame(sample_rows) if sample_rows else pd.DataFrame()

    numeric_cols = [c["name"] for c in columns_info if is_numeric(c)]
    categorical_cols = [c["name"] for c in columns_info if is_categorical(c)]

    column_stats: dict[str, Any] = {}

    for col in numeric_cols:
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if series.empty:
            continue
        column_stats[col] = {
            "dtype": "numeric",
            "min": round_value(series.min()),
            "max": round_value(series.max()),
            "mean": round_value(series.mean()),
            "sum": round_value(series.sum()),
            "std": round_value(series.std()),
            "nulls": int(df[col].isna().sum()),
            "note": "estimated from 500-row sample",
        }

    for col in categorical_cols:
        if col not in df.columns:
            continue
        series = df[col].dropna()
        column_stats[col] = {
            "dtype": "categorical",
            "unique": int(series.nunique()),
            "nulls": int(df[col].isna().sum()),
            "note": "from 500-row sample",
        }

    value_counts: dict[str, Any] = {}
    for col in categorical_cols[:10]:
        if col not in df.columns:
            continue
        vc = df[col].dropna().value_counts().head(20)
        if not vc.empty:
            value_counts[col] = {str(k): int(v) for k, v in vc.items()}
            value_counts[f"{col}__note"] = "from 500-row sample"

    cross_tabs: list[dict] = []
    for dim in categorical_cols[:3]:
        if dim not in df.columns:
            continue
        for metric in numeric_cols[:3]:
            if metric not in df.columns:
                continue
            try:
                num_series = pd.to_numeric(df[metric], errors="coerce")
                tmp = df[[dim]].copy()
                tmp[metric] = num_series
                tmp = tmp.dropna()
                if tmp.empty:
                    continue

                grouped = (
                    tmp.groupby(dim)[metric]
                    .agg(total="sum", avg="mean", count="count")
                    .reset_index()
                    .sort_values("total", ascending=False)
                    .head(15)
                )
                cross_tabs.append(
                    {
                        "group_by": dim,
                        "metric": metric,
                        "agg": "sum",
                        "data": json_safe_rows(
                            grouped.rename(columns={dim: "dimension"}).to_dict("records")
                        ),
                        "note": "from 500-row sample",
                    }
                )
            except Exception:
                pass

    return {
        "numeric_cols": numeric_cols,
        "categorical_cols": categorical_cols,
        "column_stats": column_stats,
        "value_counts": value_counts,
        "cross_tabs": cross_tabs,
    }
