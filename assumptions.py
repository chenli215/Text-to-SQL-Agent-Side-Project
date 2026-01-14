# assumptions.py
import re
from typing import List

def infer_assumptions(question: str) -> List[str]:
    """
    Return a list of assumption strings (English) for transparency/debug.
    This does NOT change SQL itself; it just documents how we interpret ambiguous terms.
    """
    q = (question or "").strip().lower()
    out: List[str] = []

    # ---- Active users vs active buyers ----
    has_active_users = any(k in q for k in ["active users", "monthly active users", "mau", "daily active users", "dau", "engaged users"])
    has_buyers_words = any(k in q for k in ["buyer", "buyers", "purchaser", "purchasers", "purchase", "purchases"])
    has_orders_words = any(k in q for k in ["order", "orders"])

    if has_active_users and not has_buyers_words and not has_orders_words:
        out.append("Interpreting 'active users/MAU/DAU' as EVENTS-based active users (distinct events.user_id).")

    # If user says "active" but not specify users/buyers -> default to active buyers
    if ("active" in q) and (not has_active_users) and (not has_buyers_words) and (not has_orders_words):
        out.append("Term 'active' is ambiguous. Defaulting to ACTIVE BUYERS (orders-based: distinct orders.user_id).")

    # ---- Revenue / Sales ambiguity ----
    # If they say revenue/sales but not explicitly net profit/refunds, assume GMV
    has_revenue = any(k in q for k in ["revenue", "sales"])
    has_gmv = "gmv" in q or "gross merchandise value" in q or "gross sales" in q
    has_net = any(k in q for k in ["net", "refund", "return", "canceled", "cancelled", "profit", "margin"])
    if has_revenue and (not has_net) and (not has_gmv):
        out.append("Interpreting 'revenue/sales' as GMV (booked sales) due to lack of refunds/returns tables in scope.")

    # ---- MoM growth formula convention ----
    has_mom = any(k in q for k in ["mom", "month-over-month", "month over month", "growth"])
    if has_mom:
        out.append("MoM growth uses SAFE_DIVIDE(x - LAG(x), LAG(x)) with LAG over ORDER BY month.")

    # ---- Time range missing ----
    # If user doesn't specify a year/date range, call it out (helps realism)
    has_year = bool(re.search(r"\b(19|20)\d{2}\b", q))
    has_date_hint = any(k in q for k in ["between", "from", "to", "since", "after", "before", "ytd", "q1", "q2", "q3", "q4"])
    if (not has_year) and (not has_date_hint):
        out.append("No explicit date range provided; query may use full available data or require an inferred default window.")

    return out
