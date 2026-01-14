# agent_sql.py
import re
from typing import List, Tuple

from clients import get_openai_client
from metrics import render_metrics_block, match_metric_intent
from semantic import SEMANTIC_RULES, normalize_sql, looks_incomplete_sql
from assumptions import infer_assumptions


def wants_monthly(question: str) -> bool:
    q = (question or "").lower()
    return ("by month" in q) or ("monthly" in q) or ("per month" in q)


def wants_mom(question: str) -> bool:
    q = (question or "").lower()
    return ("mom" in q) or ("month-over-month" in q) or ("month over month" in q) or ("growth" in q)


def wants_active_users_events(question: str) -> bool:
    """
    Disambiguation:
    - If user says "active users"/MAU/DAU and NOT buyers/purchasers -> events-based
    """
    q = (question or "").lower()
    if ("active users" in q) or ("monthly active users" in q) or ("mau" in q) or ("dau" in q):
        if ("buyer" not in q) and ("buyers" not in q) and ("purchaser" not in q) and ("purchasers" not in q):
            return True
    return False


def _dedup(items: List[str]) -> List[str]:
    out = []
    seen = set()
    for x in items:
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


def maybe_add_limit(sql: str, row_cap: int) -> str:
    s = (sql or "").strip()
    lower = s.lower()

    if re.search(r"\blimit\b", lower):
        return s

    likely_multi = (" group by " in lower) or (" order by " in lower) or (" join " in lower)
    scalar_agg = (" group by " not in lower) and any(k in lower for k in ("count(", "sum(", "avg(", "min(", "max("))

    if likely_multi and not scalar_agg:
        return s.rstrip(";") + f"\nLIMIT {int(row_cap)}"
    return s


def generate_sql(
    question: str,
    row_cap: int,
    schema_context: str,
    allow_tables: List[str],
    dataset_hint: str,
    default_model: str,
    fallback_model: str,
) -> Tuple[str, str, List[str], List[str], List[str]]:
    """
    Returns: (sql, used_model, selected_metrics, selected_qualified, assumptions)
    """
    client = get_openai_client()

    selected_metrics, selected_qualified = match_metric_intent(question)
    assumptions = infer_assumptions(question)

    use_events_active_users = wants_active_users_events(question)
    if use_events_active_users:
        # 避免 “active users” 被 synonyms 拉去 ACTIVE_BUYERS
        selected_metrics = [m for m in (selected_metrics or []) if m != "ACTIVE_BUYERS"]

    metrics_block = render_metrics_block(selected_metrics, selected_qualified)

    monthly = wants_monthly(question)
    mom = wants_mom(question)

    # 让输出更“可 join”：统一 month 列名 + 指定指标别名
    output_hint_lines: List[str] = []
    if monthly:
        output_hint_lines.append("OUTPUT FORMAT (important):")
        output_hint_lines.append("- If grouped by month, include a DATE column named `month` = DATE_TRUNC(DATE(<time_field>), MONTH).")

        alias_map = {
            "GMV": "gmv",
            "ORDERS": "orders",
            "ACTIVE_BUYERS": "active_buyers",
            "AOV": "aov",
        }
        for mname in (selected_metrics or []):
            if mname in alias_map:
                output_hint_lines.append(f"- If you compute {mname}, alias the column exactly as `{alias_map[mname]}`.")

        if use_events_active_users:
            output_hint_lines.append("- If you compute events-based active users, alias it exactly as `active_users`.")

    if mom and monthly:
        output_hint_lines.append("- If MoM is requested, compute base monthly metrics first, then add MoM columns using window LAG over ORDER BY month.")

    output_hint = "\n".join(output_hint_lines).strip()

    join_safety = """
JOIN SAFETY RULES:
- Do NOT use USING(...). Always use explicit ON conditions.
- If joining monthly CTEs, ensure each CTE outputs a DATE column named `month`, then join with:
  ON a.month = b.month
""".strip()

    extra_semantic = ""
    if use_events_active_users:
        extra_semantic = """
Active users rule (when user asks "active users"):
- Use `events` (alias e), active_users = COUNT(DISTINCT e.user_id)
- Time field: DATE(e.created_at)
""".strip()

    allow_tables_lines = "\n".join([f"  - `{dataset_hint}.{t}`" for t in allow_tables])

    instructions = f"""
You are a senior analytics engineer.
Write ONE BigQuery Standard SQL query to answer the user.

Hard rules:
- Output SQL only. No markdown. No explanation. No trailing semicolon.
- Use BigQuery Standard SQL.
- Use fully-qualified table names with backticks.
- Use ONLY these tables:
{allow_tables_lines}

- Use ONLY columns that exist in the SCHEMA block. Do NOT invent columns.
- Follow SEMANTIC RULES and METRICS DICTIONARY strictly when relevant.
- If it could return many rows, add LIMIT {row_cap} (and do NOT add a trailing semicolon).

{SEMANTIC_RULES}

{join_safety}

{extra_semantic}

{metrics_block}

{output_hint}

{schema_context}
""".strip()

    def call(model: str) -> str:
        resp = client.responses.create(
            model=model,
            instructions=instructions,
            input=question.strip(),
            max_output_tokens=1400,
        )
        return normalize_sql(getattr(resp, "output_text", "") or "")

    sql = call(default_model)
    used_model = default_model

    if (not sql) or looks_incomplete_sql(sql):
        sql = call(fallback_model)
        used_model = fallback_model

    if not sql or looks_incomplete_sql(sql):
        raise ValueError("Model returned empty or incomplete SQL. Try again.")

    sql = maybe_add_limit(sql, row_cap)
    return sql, used_model, selected_metrics, selected_qualified, _dedup(assumptions)


def fix_sql(
    question: str,
    bad_sql: str,
    bq_error: str,
    row_cap: int,
    schema_context: str,
    allow_tables: List[str],
    dataset_hint: str,
    default_model: str,
) -> str:
    client = get_openai_client()

    selected_metrics, selected_qualified = match_metric_intent(question)
    use_events_active_users = wants_active_users_events(question)
    if use_events_active_users:
        selected_metrics = [m for m in (selected_metrics or []) if m != "ACTIVE_BUYERS"]

    metrics_block = render_metrics_block(selected_metrics, selected_qualified)

    join_safety = """
JOIN SAFETY RULES:
- Do NOT use USING(...). Always use explicit ON conditions.
- If joining monthly CTEs, ensure each CTE outputs a DATE column named `month`, then join with:
  ON a.month = b.month
""".strip()

    extra_semantic = ""
    if use_events_active_users:
        extra_semantic = """
Active users rule (when user asks "active users"):
- Use `events` (alias e), active_users = COUNT(DISTINCT e.user_id)
- Time field: DATE(e.created_at)
""".strip()

    allow_tables_lines = "\n".join([f"  - `{dataset_hint}.{t}`" for t in allow_tables])

    instructions = f"""
You are fixing a BigQuery Standard SQL query.

Rules:
- Output SQL only. No markdown. No explanation. No trailing semicolon.
- Use BigQuery Standard SQL.
- Use fully-qualified table names with backticks.
- Use ONLY these tables:
{allow_tables_lines}

- Use ONLY columns that exist in the SCHEMA block. Do NOT invent columns.
- Follow SEMANTIC RULES and METRICS DICTIONARY strictly when relevant.
- If it could return many rows, add LIMIT {row_cap}.

{SEMANTIC_RULES}

{join_safety}

{extra_semantic}

{metrics_block}

{schema_context}
""".strip()

    prompt = f"""
User question:
{question}

Previous SQL (failed):
{bad_sql}

BigQuery error:
{bq_error}

Return a corrected SQL query only.
""".strip()

    resp = client.responses.create(
        model=default_model,
        instructions=instructions,
        input=prompt,
        max_output_tokens=1400,
    )

    fixed = normalize_sql(getattr(resp, "output_text", "") or "")
    fixed = maybe_add_limit(fixed, row_cap)
    if not fixed or looks_incomplete_sql(fixed):
        raise ValueError("Fix attempt returned empty/incomplete SQL.")
    return fixed
