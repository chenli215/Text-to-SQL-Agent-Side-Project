# bq_runner.py
from typing import Optional, Tuple
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

from clients import get_bq_client


def estimate_scan_gb(sql: str, location: str) -> float:
    client = get_bq_client()
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(sql, job_config=job_config, location=location)
    bytes_scanned = int(job.total_bytes_processed or 0)
    return bytes_scanned / (1024 ** 3)


def run_query(sql: str, location: str) -> pd.DataFrame:
    client = get_bq_client()
    job = client.query(sql, location=location)
    return job.result().to_dataframe()


def dryrun_and_execute_with_one_fix(
    question: str,
    sql: str,
    row_cap: int,
    schema_context: str,
    location: str,
    fix_fn,  # callable
    max_scan_gb: Optional[float] = None,
) -> Tuple[str, float, Optional[pd.DataFrame], bool, Optional[str]]:
    """
    Returns: (final_sql, est_gb, df_or_none, fixed_used, bq_error_or_none)
    - Always dry-run first to estimate scan.
    - If max_scan_gb provided and scan exceeds, it will block BEFORE execute.
    - Try at most ONE fix if BigQuery rejects SQL (dry-run or execute).
    """
    fixed_used = False

    # 1) Dry-run
    try:
        est_gb = estimate_scan_gb(sql, location)
    except BadRequest as e:
        err = str(e)
        sql = fix_fn(question, sql, err, row_cap=row_cap, schema_context=schema_context)
        fixed_used = True
        est_gb = estimate_scan_gb(sql, location)

    # 2) Block before execute if scan too large
    if (max_scan_gb is not None) and (est_gb > float(max_scan_gb)):
        return sql, est_gb, None, fixed_used, (
            f"Blocked: scan {est_gb:.2f} GB exceeds max allowed {float(max_scan_gb):.2f} GB."
        )

    # 3) Execute
    try:
        df = run_query(sql, location)
        return sql, est_gb, df, fixed_used, None
    except BadRequest as e:
        err = str(e)
        if fixed_used:
            return sql, est_gb, None, fixed_used, err

        sql = fix_fn(question, sql, err, row_cap=row_cap, schema_context=schema_context)
        fixed_used = True

        # re-dryrun just for updated estimate (optional but nice)
        try:
            est_gb = estimate_scan_gb(sql, location)
        except Exception:
            pass

        if (max_scan_gb is not None) and (est_gb > float(max_scan_gb)):
            return sql, est_gb, None, fixed_used, (
                f"Blocked: scan {est_gb:.2f} GB exceeds max allowed {float(max_scan_gb):.2f} GB."
            )

        df = run_query(sql, location)
        return sql, est_gb, df, fixed_used, None
