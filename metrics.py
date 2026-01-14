# metrics.py
import re

METRICS = {
    "GMV": {
        "meaning": "Gross Merchandise Value (total sales). Default: booked GMV.",
        "metric_type": "sum",
        "grain": "item",
        "synonyms": ["gmv", "sales", "revenue", "gross sales", "gross merchandise value"],
        "expr_tmpl": "SUM({f}.sale_price)",
        "fact": {
            "table": "bigquery-public-data.thelook_ecommerce.order_items",
            "alias": "oi",
            "time_field": "DATE({f}.created_at)",
        },
        "default_filters": [],
        "allowed_dimensions": ["date", "month", "product_id", "category", "brand"],
        "join_hints": {
            "product_id": {"join": "JOIN `bigquery-public-data.thelook_ecommerce.products` p ON {f}.product_id = p.id", "select": "p.id AS product_id"},
            "category": {"join": "JOIN `bigquery-public-data.thelook_ecommerce.products` p ON {f}.product_id = p.id", "select": "p.category AS category"},
            "brand": {"join": "JOIN `bigquery-public-data.thelook_ecommerce.products` p ON {f}.product_id = p.id", "select": "p.brand AS brand"},
        },
        "examples": [
            "SELECT SUM(oi.sale_price) AS gmv FROM `bigquery-public-data.thelook_ecommerce.order_items` oi "
            "WHERE DATE(oi.created_at) BETWEEN '2024-01-01' AND '2024-12-31';"
        ],
        "notes": ["Use order_items for money metrics.", "Time field for GMV: DATE(order_items.created_at)."],
    },

    "AOV": {
        "meaning": "Average Order Value = GMV / number of distinct orders.",
        "metric_type": "ratio",
        "grain": "order",
        "synonyms": ["aov", "average order value"],
        "expr_tmpl": "SAFE_DIVIDE(SUM({f}.sale_price), COUNT(DISTINCT {f}.order_id))",
        "fact": {
            "table": "bigquery-public-data.thelook_ecommerce.order_items",
            "alias": "oi",
            "time_field": "DATE({f}.created_at)",
        },
        "default_filters": [],
        "allowed_dimensions": ["date", "month"],
        "join_hints": {},
        "examples": [
            "SELECT SAFE_DIVIDE(SUM(oi.sale_price), COUNT(DISTINCT oi.order_id)) AS aov "
            "FROM `bigquery-public-data.thelook_ecommerce.order_items` oi "
            "WHERE DATE(oi.created_at) BETWEEN '2024-01-01' AND '2024-12-31';"
        ],
        "notes": ["Do NOT use AVG(item_price). Use GMV / distinct orders."],
    },

    "ORDERS": {
        "meaning": "Number of orders placed in the period.",
        "metric_type": "count",
        "grain": "order",
        "synonyms": ["orders", "order count", "number of orders", "how many orders"],
        "expr_tmpl": "COUNT(DISTINCT {f}.order_id)",
        "fact": {
            "table": "bigquery-public-data.thelook_ecommerce.orders",
            "alias": "o",
            "time_field": "DATE({f}.created_at)",
        },
        "default_filters": [],
        "allowed_dimensions": ["date", "month", "status"],
        "join_hints": {},
        "examples": [
            "SELECT COUNT(DISTINCT o.order_id) AS orders "
            "FROM `bigquery-public-data.thelook_ecommerce.orders` o "
            "WHERE DATE(o.created_at) BETWEEN '2024-01-01' AND '2024-12-31';"
        ],
        "notes": ["Use orders.created_at for order activity time."],
    },

    "ACTIVE_BUYERS": {
        "meaning": "Active buyers = users who placed >= 1 order in the period.",
        "metric_type": "count_distinct",
        "grain": "user",
        # IMPORTANT: remove 'active users' to avoid confusion
        "synonyms": ["active buyers", "purchasing users", "buyers", "purchasers"],
        "expr_tmpl": "COUNT(DISTINCT {f}.user_id)",
        "fact": {
            "table": "bigquery-public-data.thelook_ecommerce.orders",
            "alias": "o",
            "time_field": "DATE({f}.created_at)",
        },
        "default_filters": [],
        "allowed_dimensions": ["date", "month"],
        "join_hints": {},
        "examples": [
            "SELECT COUNT(DISTINCT o.user_id) AS active_buyers "
            "FROM `bigquery-public-data.thelook_ecommerce.orders` o "
            "WHERE DATE(o.created_at) BETWEEN '2024-01-01' AND '2024-12-31';"
        ],
        "notes": ["IMPORTANT: active buyers uses orders.created_at, NOT users.created_at."],
    },

    "ACTIVE_USERS": {
        "meaning": "Active users (events-based) = distinct users with >= 1 event in the period (MAU/DAU).",
        "metric_type": "count_distinct",
        "grain": "user",
        "synonyms": ["active users", "monthly active users", "mau", "daily active users", "dau", "engaged users"],
        "expr_tmpl": "COUNT(DISTINCT {f}.user_id)",
        "fact": {
            "table": "bigquery-public-data.thelook_ecommerce.events",
            "alias": "e",
            "time_field": "DATE({f}.created_at)",
        },
        "default_filters": [],
        "allowed_dimensions": ["date", "month"],
        "join_hints": {},
        "examples": [
            "SELECT COUNT(DISTINCT e.user_id) AS active_users "
            "FROM `bigquery-public-data.thelook_ecommerce.events` e "
            "WHERE DATE(e.created_at) BETWEEN '2024-01-01' AND '2024-12-31';"
        ],
        "notes": ["Use events-based definition when user asks MAU/DAU/active users."],
    },

    "NEW_USERS": {
        "meaning": "New users = users created (signed up) in the period.",
        "metric_type": "count_distinct",
        "grain": "user",
        "synonyms": ["new users", "signups", "registrations", "signed up"],
        "expr_tmpl": "COUNT(DISTINCT {f}.id)",
        "fact": {
            "table": "bigquery-public-data.thelook_ecommerce.users",
            "alias": "u",
            "time_field": "DATE({f}.created_at)",
        },
        "default_filters": [],
        "allowed_dimensions": ["date", "month", "country", "state"],
        "join_hints": {},
        "examples": [
            "SELECT COUNT(DISTINCT u.id) AS new_users "
            "FROM `bigquery-public-data.thelook_ecommerce.users` u "
            "WHERE DATE(u.created_at) BETWEEN '2024-01-01' AND '2024-12-31';"
        ],
        "notes": ["Use users.created_at only for signup metrics."],
    },
}

QUALIFIED_METRICS = {
    "ACTIVE_BUYERS_3PLUS": {
        "meaning": "Active buyers (3+ orders) = users who placed at least 3 distinct orders in the period.",
        "grain": "user",
        "synonyms": ["buyers with 3+ orders", "3+ orders buyers", "repeat buyers 3+", "at least 3 orders"],
        "base_fact": {
            "table": "bigquery-public-data.thelook_ecommerce.orders",
            "alias": "o",
            "time_field": "DATE({f}.created_at)",
        },
        "entity_key": "{f}.user_id",
        "qualifying_expr": "COUNT(DISTINCT {f}.order_id)",
        "threshold_op": ">=",
        "threshold_value": 3,
        "default_filters": [],
        "examples": [
            "WITH u AS ("
            "  SELECT o.user_id, COUNT(DISTINCT o.order_id) AS orders_cnt "
            "  FROM `bigquery-public-data.thelook_ecommerce.orders` o "
            "  WHERE DATE(o.created_at) BETWEEN '2024-01-01' AND '2024-12-31' "
            "  GROUP BY o.user_id"
            ") "
            "SELECT COUNT(*) AS active_buyers_3plus FROM u WHERE orders_cnt >= 3;"
        ],
        "notes": ["Two-step: per user aggregate -> filter -> count users.", "Time field: DATE(orders.created_at)."],
    }
}


def _render_metric_one(name: str, m: dict) -> str:
    f = m["fact"]["alias"]
    expr = m["expr_tmpl"].replace("{f}", f)
    tf = m["fact"]["time_field"].replace("{f}", f)

    lines = [
        f"- {name}: {m['meaning']}",
        f"  - metric_type: {m['metric_type']}, grain: {m['grain']}",
        f"  - synonyms: {', '.join(m.get('synonyms', []))}",
        f"  - fact: `{m['fact']['table']}` AS {f}",
        f"  - time_field: {tf}",
        f"  - expr: {expr}",
    ]

    if m.get("allowed_dimensions"):
        lines.append("  - allowed_dimensions: " + ", ".join(m["allowed_dimensions"]))

    if m.get("join_hints"):
        dims = ", ".join(sorted(m["join_hints"].keys()))
        lines.append(f"  - join_hints available for: {dims}")

    if m.get("notes"):
        lines.append("  - notes: " + " | ".join(m["notes"]))

    if m.get("examples"):
        lines.append("  - example: " + m["examples"][0])

    return "\n".join(lines)


def _render_qualified_one(name: str, m: dict) -> str:
    f = m["base_fact"]["alias"]
    tf = m["base_fact"]["time_field"].replace("{f}", f)

    lines = [
        f"- {name} (QUALIFIED): {m['meaning']}",
        f"  - grain: {m['grain']}",
        f"  - synonyms: {', '.join(m.get('synonyms', []))}",
        f"  - base_fact: `{m['base_fact']['table']}` AS {f}",
        f"  - time_field: {tf}",
        f"  - per_entity: {m['qualifying_expr'].replace('{f}', f)} GROUP BY {m['entity_key'].replace('{f}', f)}",
        f"  - qualification: {m['qualifying_expr'].replace('{f}', f)} {m['threshold_op']} {m['threshold_value']}",
    ]

    if m.get("notes"):
        lines.append("  - notes: " + " | ".join(m["notes"]))

    if m.get("examples"):
        lines.append("  - example: " + m["examples"][0])

    return "\n".join(lines)


def render_metrics_block(selected_metrics=None, selected_qualified=None) -> str:
    selected_metrics = selected_metrics or []
    selected_qualified = selected_qualified or []

    parts = ["METRICS DICTIONARY (use these definitions exactly):"]

    metric_items = METRICS.items() if not selected_metrics else [(k, METRICS[k]) for k in selected_metrics if k in METRICS]
    for name, m in metric_items:
        parts.append(_render_metric_one(name, m))

    parts.append("QUALIFIED METRICS (two-step aggregation + filter):")
    qualified_items = QUALIFIED_METRICS.items() if not selected_qualified else [(k, QUALIFIED_METRICS[k]) for k in selected_qualified if k in QUALIFIED_METRICS]
    for name, m in qualified_items:
        parts.append(_render_qualified_one(name, m))

    parts.append("DISAMBIGUATION RULE: If question mentions '3+ orders' / 'at least 3 orders', use ACTIVE_BUYERS_3PLUS.")
    parts.append("DISAMBIGUATION RULE: 'active users/MAU/DAU' => use ACTIVE_USERS (events-based); 'active buyers' => use ACTIVE_BUYERS (orders-based).")
    return "\n".join(parts)


def match_metric_intent(question: str):
    q = (question or "").strip().lower()

    # Qualified pattern: 3+ orders
    three_plus = bool(re.search(r"(3\+|>=\s*3|at\s+least\s+3|minimum\s+3|three\s+or\s+more|three\s*\+)", q))
    mentions_orders = ("order" in q) or ("orders" in q)
    if three_plus and mentions_orders:
        return [], ["ACTIVE_BUYERS_3PLUS"]

    # Strong disambiguation: active users/MAU/DAU => ACTIVE_USERS
    if any(k in q for k in ["active users", "monthly active users", "mau", "daily active users", "dau", "engaged users"]):
        # if they also explicitly mention buyers/purchasers/orders, let model decide using semantic rules,
        # but we still include ACTIVE_USERS so it has the definition available.
        selected = ["ACTIVE_USERS"]
        # also include GMV/orders if mentioned
        for k2, m2 in METRICS.items():
            syns = [s.lower() for s in m2.get("synonyms", [])]
            if any(s in q for s in syns) and k2 not in selected:
                selected.append(k2)
        return selected, []

    selected_metrics = []
    for k, m in METRICS.items():
        syns = [s.lower() for s in m.get("synonyms", [])]
        if any(s in q for s in syns):
            selected_metrics.append(k)

    # Dedup keep order
    seen = set()
    out = []
    for x in selected_metrics:
        if x not in seen:
            out.append(x)
            seen.add(x)

    return out, []
