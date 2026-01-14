# semantic.py
import re

DATASET_HINT = "bigquery-public-data.thelook_ecommerce"
LOCATION = "US"

SEMANTIC_RULES = f"""
SEMANTIC RULES (follow strictly):
- Use ONLY tables from dataset `{DATASET_HINT}`.
- Only SELECT/WITH queries. Never use INSERT/UPDATE/DELETE/CREATE/DROP/ALTER.
- Output SQL only (no markdown, no explanation).
- Use fully-qualified table names with backticks.
- Do NOT use USING(...) in joins. Always use explicit ON conditions.

FACT TABLE GUIDANCE (hard rules):
- Money metrics (GMV, AOV, top products by GMV): use `order_items` (alias oi) and DATE(oi.created_at).
- Orders / buyer activity (orders, active buyers, repeat buyers): use `orders` (alias o) and DATE(o.created_at).
- New users (signups): use `users` (alias u) and DATE(u.created_at).
- Active users (engagement, MAU/DAU, sessions/events): use `events` (alias e) and DATE(e.created_at).

TIME FIELD RULES (hard):
- buyer activity / orders: DATE(o.created_at)
- GMV / item sales: DATE(oi.created_at)
- new user signup: DATE(u.created_at)
- active users (events): DATE(e.created_at)
- Never use users.created_at as the date for "active buyers" or "orders".

JOIN KEYS (logical relationships):
- o.order_id = oi.order_id
- o.user_id = u.id
- oi.product_id = p.id

DISAMBIGUATION RULES (very important):
- If user says "active buyers" / "buyers" / "purchasers": use ORDERS-based active buyers = COUNT(DISTINCT o.user_id).
- If user says "active users" / "MAU" / "DAU" / "engaged users" AND does NOT mention buyers/purchasers/orders:
  use EVENTS-based active users = COUNT(DISTINCT e.user_id).
- If user only says "active" without buyers/users:
  assume "active buyers" (orders-based) by default.
- If user asks GMV + Orders + AOV by month:
  compute monthly GMV from order_items, monthly Orders from orders, and monthly AOV from order_items,
  then join monthly CTEs on month using explicit ON (a.month = b.month). Do not use USING(month).

MULTI-METRIC OUTPUT RULE (recommended for stability):
- When returning monthly results, create a DATE column named `month` = DATE_TRUNC(DATE(<time_field>), MONTH).
""".strip()


def normalize_sql(text: str) -> str:
    """Extract SQL from model output (remove ```sql fences etc.)."""
    if not text:
        return ""
    t = text.strip()
    m = re.search(r"```(?:sql)?\s*(.*?)```", t, flags=re.IGNORECASE | re.DOTALL)
    if m:
        t = m.group(1).strip()
    t = t.strip().rstrip(";").strip()
    return t


def looks_incomplete_sql(sql: str) -> bool:
    """Heuristic: detect truncated / incomplete SQL."""
    if not sql:
        return True

    s = sql.strip()
    lower = s.lower()

    if "；" in s:
        return True

    if s.count("(") != s.count(")"):
        return True

    bad_endings = ("where", "and", "or", "between", "join", "on", "from", "with", "select", ",")
    if any(lower.endswith(x) for x in bad_endings):
        return True

    return False
