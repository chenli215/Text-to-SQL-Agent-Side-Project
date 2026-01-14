from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from google.cloud import bigquery


# =========================
# schema_cache.py
# 目的：
# 1) 从 BigQuery 的 INFORMATION_SCHEMA 拉 schema（表 + 字段）
# 2) 缓存到本地 json（避免每次启动都扫一遍）
# 3) 构造“真实表 allowlist”，给 Guard 用来限制只能访问允许的数据集表
# =========================


def _now_ts() -> int:
    return int(time.time())


def _ensure_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def _default_cache_path(dataset_fq: str) -> str:
    # dataset_fq 形如：bigquery-public-data.thelook_ecommerce
    safe = dataset_fq.replace(":", "_").replace(".", "_")
    return os.path.join(".cache", f"schema_{safe}.json")


def _bq(dataset_fq: str) -> Tuple[str, str]:
    # dataset_fq: project.dataset
    if "." not in dataset_fq:
        raise ValueError(f"dataset_fq must be like 'project.dataset', got: {dataset_fq}")
    project, dataset = dataset_fq.split(".", 1)
    return project, dataset


def fetch_schema_from_information_schema(
    client: bigquery.Client,
    dataset_fq: str,
    location: str = "US",
) -> Dict[str, Any]:
    """
    从 INFORMATION_SCHEMA 拉取：
    - 表列表
    - 每张表的字段列表（name/type）
    """
    project, dataset = _bq(dataset_fq)

    tables_sql = f"""
    SELECT table_name, table_type
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
    ORDER BY table_name
    """

    cols_sql = f"""
    SELECT table_name, column_name, data_type
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    ORDER BY table_name, ordinal_position
    """

    tables = list(client.query(tables_sql, location=location).result())
    cols = list(client.query(cols_sql, location=location).result())

    tables_out: Dict[str, Any] = {}
    for r in tables:
        tables_out[str(r["table_name"])] = {
            "table_type": str(r["table_type"]),
            "columns": [],
        }

    for r in cols:
        t = str(r["table_name"])
        if t not in tables_out:
            tables_out[t] = {"table_type": "UNKNOWN", "columns": []}
        tables_out[t]["columns"].append(
            {"name": str(r["column_name"]), "type": str(r["data_type"])}
        )

    return {
        "dataset_fq": dataset_fq,
        "generated_at": _now_ts(),
        "tables": tables_out,
    }


def get_schema_cache(
    client: bigquery.Client,
    dataset_fq: str,
    location: str = "US",
    cache_path: Optional[str] = None,
    ttl_seconds: int = 24 * 3600,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    """
    读取缓存；过期/不存在则拉新 schema 并写入缓存。
    """
    if cache_path is None:
        cache_path = _default_cache_path(dataset_fq)

    if (not force_refresh) and os.path.exists(cache_path):
        try:
            raw = json.loads(open(cache_path, "r", encoding="utf-8").read())
            age = _now_ts() - int(raw.get("generated_at", 0))
            if age <= ttl_seconds and raw.get("dataset_fq") == dataset_fq:
                return raw
        except Exception:
            pass  # 读坏了就当不存在

    # 刷新
    schema = fetch_schema_from_information_schema(client, dataset_fq, location=location)

    _ensure_dir(cache_path)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, ensure_ascii=False, indent=2)

    return schema


def build_table_allowlist(schema_cache: Dict[str, Any]) -> Set[str]:
    """
    把 schema_cache 变成 allowlist（标准化成：project.dataset.table）
    注意：这里默认只允许 BASE TABLE / VIEW（如果你不想允许 VIEW，可加过滤）
    """
    dataset_fq = str(schema_cache["dataset_fq"])
    tables: Dict[str, Any] = schema_cache.get("tables", {}) or {}

    allow: Set[str] = set()

    for table_name, meta in tables.items():
        ttype = str((meta or {}).get("table_type", "")).upper()
        if ttype in ("BASE TABLE", "VIEW", "MATERIALIZED VIEW", "EXTERNAL"):
            allow.add(f"{dataset_fq}.{table_name}")

    # 额外允许 INFORMATION_SCHEMA 的两个视图（方便你 debug / schema 功能）
    # （如果你不想在 UI 里让用户查 schema，可以删掉这两行）
    allow.add(f"{dataset_fq}.INFORMATION_SCHEMA.TABLES")
    allow.add(f"{dataset_fq}.INFORMATION_SCHEMA.COLUMNS")

    return allow


def render_schema_for_prompt(
    schema_cache: Dict[str, Any],
    max_tables: int = 30,
    max_cols_per_table: int = 30,
) -> str:
    """
    把 schema 变成一段短文本，塞给 LLM 做提示（soft semantic）
    """
    dataset_fq = str(schema_cache.get("dataset_fq", ""))
    tables: Dict[str, Any] = schema_cache.get("tables", {}) or {}

    lines: List[str] = []
    lines.append(f"Dataset: {dataset_fq}")
    lines.append("Tables and columns (name: type):")

    table_names = sorted(tables.keys())[:max_tables]
    for t in table_names:
        cols = (tables.get(t) or {}).get("columns", []) or []
        cols = cols[:max_cols_per_table]
        cols_txt = ", ".join([f"{c['name']}:{c['type']}" for c in cols])
        lines.append(f"- {t}: {cols_txt}")

    if len(tables) > max_tables:
        lines.append(f"... ({len(tables) - max_tables} more tables omitted)")

    return "\n".join(lines)

# =========================
# Backward-compatible helpers for app.py
# (so app.py can import fetch_schema_map / render_schema_context)
# =========================

def _get_bq_client_for_schema(billing_project: Optional[str] = None) -> bigquery.Client:
    """
    Create a BigQuery client for INFORMATION_SCHEMA queries.
    Uses BQ_BILLING_PROJECT if provided (recommended).
    """
    proj = billing_project or os.getenv("BQ_BILLING_PROJECT")
    if proj:
        return bigquery.Client(project=proj)
    # Fallback: rely on default project from ADC (less stable)
    return bigquery.Client()


def fetch_schema_map(
    allow_tables: List[str],
    dataset_fq: str,
    location: str = "US",
    ttl_seconds: int = 24 * 3600,
    force_refresh: bool = False,
    cache_path: Optional[str] = None,
    billing_project: Optional[str] = None,
) -> Dict[str, List[str]]:
    """
    App-facing schema map:
      { "orders": ["order_id", "user_id", ...], ... }
    Only returns tables in allow_tables.
    """
    client = _get_bq_client_for_schema(billing_project=billing_project)
    cache = get_schema_cache(
        client=client,
        dataset_fq=dataset_fq,
        location=location,
        cache_path=cache_path,
        ttl_seconds=ttl_seconds,
        force_refresh=force_refresh,
    )

    tables: Dict[str, Any] = cache.get("tables", {}) or {}
    out: Dict[str, List[str]] = {}

    for t in allow_tables:
        meta = tables.get(t) or tables.get(str(t)) or {}
        cols = (meta.get("columns") or [])
        out[str(t)] = [str(c.get("name")) for c in cols if c and c.get("name")]

    return out


def render_schema_context(
    schema_map: Dict[str, List[str]],
    allow_tables: List[str],
    dataset_fq: str,
    max_cols_per_table: int = 160,
) -> str:
    """
    App-facing schema context block (short, column names only).
    """
    lines: List[str] = ["SCHEMA (only use columns listed below; do NOT invent columns):"]
    for t in allow_tables:
        cols = schema_map.get(t, []) or []
        if not cols:
            continue
        cols_show = cols[:max_cols_per_table]
        suffix = "" if len(cols) <= max_cols_per_table else " ... (truncated)"
        lines.append(f"- `{dataset_fq}.{t}` columns: {', '.join(cols_show)}{suffix}")
    return "\n".join(lines).strip()


