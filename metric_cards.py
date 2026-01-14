# metric_cards.py
import streamlit as st
from typing import List, Optional

from metrics import METRICS, QUALIFIED_METRICS
from agent_sql import wants_mom, wants_active_users_events


def _metric_card_payload(name: str, definition: str, time_field: str, formula: str) -> dict:
    return {
        "Metric Name": name,
        "Definition": definition,
        "Time field": time_field,
        "Formula": formula,
    }


def render_metric_cards_simplified(question: str, selected_metrics: Optional[List[str]], selected_qualified: Optional[List[str]]):
    st.subheader("Metric Cards")

    cards = []

    # Normal metrics
    for k in (selected_metrics or []):
        if k not in METRICS:
            continue
        m = METRICS[k]
        f = m["fact"]["alias"]
        time_field = m["fact"]["time_field"].replace("{f}", f)
        formula = m["expr_tmpl"].replace("{f}", f)
        cards.append(_metric_card_payload(k, m.get("meaning", ""), time_field, formula))

    # Qualified metrics
    for k in (selected_qualified or []):
        if k not in QUALIFIED_METRICS:
            continue
        m = QUALIFIED_METRICS[k]
        f = m["base_fact"]["alias"]
        time_field = m["base_fact"]["time_field"].replace("{f}", f)
        formula = f"{m['qualifying_expr'].replace('{f}', f)} {m['threshold_op']} {m['threshold_value']} (per {m['entity_key'].replace('{f}', f)})"
        cards.append(_metric_card_payload(k, m.get("meaning", ""), time_field, formula))

    # MoM virtual card
    if wants_mom(question):
        cards.append(_metric_card_payload(
            "MoM Growth",
            "Month-over-month growth rate: (current_month - prev_month) / prev_month",
            "month (DATE_TRUNC(..., MONTH))",
            "SAFE_DIVIDE(x - LAG(x) OVER(ORDER BY month), LAG(x) OVER(ORDER BY month))"
        ))

    # Events-based active users card (if triggered)
    if wants_active_users_events(question):
        cards.append(_metric_card_payload(
            "ACTIVE_USERS (events-based)",
            "Active users = distinct users with at least one event in the month.",
            "DATE(events.created_at) → DATE_TRUNC(..., MONTH)",
            "COUNT(DISTINCT e.user_id)"
        ))

    if not cards:
        st.info("No metric intent matched. Try mentioning GMV / orders / AOV / active buyers explicitly.")
        return

    cols = st.columns(2)
    for i, c in enumerate(cards):
        with cols[i % 2]:
            st.markdown(f"### {c['Metric Name']}")
            st.write("**Definition:**", c["Definition"])
            st.write("**Time field:**", c["Time field"])
            st.write("**Formula:**")
            st.code(c["Formula"], language="sql")
