# app.py
import os
from dotenv import load_dotenv
load_dotenv()

import streamlit as st

from guard import SQLGuard
from semantic import DATASET_HINT, LOCATION
from schema_cache import fetch_schema_map, render_schema_context
from agent_sql import generate_sql, fix_sql
from bq_runner import dryrun_and_execute_with_one_fix
from metric_cards import render_metric_cards_simplified


# -----------------------------
# App config
# -----------------------------
st.set_page_config(page_title="Text-to-SQL (theLook + BigQuery)", layout="wide")

APP_BUILD = "build_2026-01-13_speed_cache_v1"
DEFAULT_MODEL = "gpt-5-mini"
FALLBACK_MODEL = "gpt-5.2"
ROW_CAP_DEFAULT = 200

ALLOW_TABLES = [
    "orders",
    "order_items",
    "users",
    "products",
    "events",
    "inventory_items",
    "distribution_centers",
]


def fq_table(t: str) -> str:
    return f"{DATASET_HINT}.{t}"


def allowed_tables_fq_set():
    return {fq_table(t) for t in ALLOW_TABLES}


# -----------------------------
# Cached schema bundle (IMPORTANT for speed)
# -----------------------------
@st.cache_data(ttl=3600, show_spinner=False)
def get_schema_bundle(allow_tables_tuple, dataset_hint: str, location: str):
    """
    Returns (schema_map, schema_context).
    We cache this so reruns don't re-hit BigQuery / rebuild huge strings.
    """
    allow_tables = list(allow_tables_tuple)
    schema_map = fetch_schema_map(allow_tables, dataset_hint, location)
    schema_context = render_schema_context(schema_map, allow_tables, dataset_hint)
    return schema_map, schema_context


# -----------------------------
# UI
# -----------------------------
st.title("Text-to-SQL Agent (theLook + BigQuery)")
st.caption("Generate SQL → Guard → Dry-run scan → Execute → Metric Cards")

with st.sidebar:
    st.header("Settings")
    st.write("APP_BUILD:", APP_BUILD)

    row_cap = st.number_input(
        "Max rows to display",
        min_value=50,
        max_value=2000,
        value=ROW_CAP_DEFAULT,
        step=50,
    )

    max_scan_gb = st.number_input(
        "Max scan allowed (GB)",
        min_value=0.01,
        max_value=500.0,
        value=2.0,
        step=0.25,
    )

    force_regen = st.checkbox("Force regenerate SQL", value=False)

    if st.button("Refresh schema cache"):
        # Clear Streamlit cache wrapper (fast)
        get_schema_bundle.clear()
        st.success("Schema cache cleared (Streamlit). It will refresh on next run.")


# Pull schema ONCE per rerun (cached)
schema_map, schema_context = get_schema_bundle(tuple(ALLOW_TABLES), DATASET_HINT, LOCATION)

guard = SQLGuard(
    allowed_tables=allowed_tables_fq_set(),
    default_dataset_fq=DATASET_HINT,
)

question = st.text_input(
    "Your question (English)",
    value="In 2024, show GMV, orders, active buyers, and AOV by month, and compute MoM growth for each metric.",
)

col1, col2 = st.columns([1, 1])
with col1:
    generate_btn = st.button("Generate SQL")
with col2:
    run_btn = st.button("Run (Generate + Query)")


def _need_regen(q: str, cap: int, force: bool, clicked_generate: bool) -> bool:
    if clicked_generate:
        return True
    if force:
        return True
    last_key = st.session_state.get("gen_cache_key")
    cur_key = (q.strip(), int(cap), DATASET_HINT, APP_BUILD)
    if last_key != cur_key:
        return True
    if not st.session_state.get("gen_sql"):
        return True
    return False


if generate_btn or run_btn:
    try:
        # 1) Generate SQL (or reuse cached generation)
        regen = _need_regen(question, int(row_cap), force_regen, generate_btn)

        if regen:
            with st.spinner("Generating SQL..."):
                sql, used_model, sel_m, sel_q, assumptions = generate_sql(
                    question=question,
                    row_cap=int(row_cap),
                    schema_context=schema_context,
                    allow_tables=ALLOW_TABLES,
                    dataset_hint=DATASET_HINT,
                    default_model=DEFAULT_MODEL,
                    fallback_model=FALLBACK_MODEL,
                )

            # cache generation result in session_state so Run won't regenerate
            st.session_state["gen_cache_key"] = (question.strip(), int(row_cap), DATASET_HINT, APP_BUILD)
            st.session_state["gen_sql"] = sql
            st.session_state["gen_used_model"] = used_model
            st.session_state["gen_sel_m"] = sel_m
            st.session_state["gen_sel_q"] = sel_q
            st.session_state["gen_assumptions"] = assumptions
        else:
            sql = st.session_state["gen_sql"]
            used_model = st.session_state["gen_used_model"]
            sel_m = st.session_state["gen_sel_m"]
            sel_q = st.session_state["gen_sel_q"]
            assumptions = st.session_state.get("gen_assumptions") or []

        st.subheader("Generated SQL")
        st.caption(f"SQL model used: **{used_model}**")

        if assumptions:
            st.info("Assumptions applied:\n- " + "\n- ".join(assumptions))

        st.code(sql, language="sql")

        # Metric Cards
        render_metric_cards_simplified(question, sel_m, sel_q)

        # 2) Guard check
        gr = guard.check(sql)
        with st.expander("Guard debug meta (for troubleshooting)"):
            st.json(gr.meta)

        if not gr.ok:
            st.error("Blocked by guard:")
            for r in gr.reasons:
                st.write(f"- {r}")
            st.stop()

        # 3) Run query
        if run_btn:
            def _fix_fn(q, bad_sql, err, row_cap, schema_context):
                return fix_sql(
                    question=q,
                    bad_sql=bad_sql,
                    bq_error=err,
                    row_cap=row_cap,
                    schema_context=schema_context,
                    allow_tables=ALLOW_TABLES,
                    dataset_hint=DATASET_HINT,
                    default_model=DEFAULT_MODEL,
                )

            with st.spinner("Dry-run scan → Execute (auto-fix once if needed)..."):
                final_sql, est_gb, df, fixed_used, bq_err = dryrun_and_execute_with_one_fix(
                    question=question,
                    sql=gr.cleaned_sql,
                    row_cap=int(row_cap),
                    schema_context=schema_context,
                    location=LOCATION,
                    fix_fn=_fix_fn,
                    max_scan_gb=float(max_scan_gb),   # ✅ now blocks BEFORE execute
                )

            st.write(f"Estimated scan: **{est_gb:.2f} GB**")

            if fixed_used:
                st.subheader("Final SQL (after auto-fix)")
                st.code(final_sql, language="sql")

                gr2 = guard.check(final_sql)
                with st.expander("Guard debug meta (final SQL)"):
                    st.json(gr2.meta)

                if not gr2.ok:
                    st.error("Fixed SQL blocked by guard:")
                    for r in gr2.reasons:
                        st.write(f"- {r}")
                    st.stop()

            if bq_err:
                st.error("BigQuery failed / blocked:")
                st.write(bq_err)
                st.stop()

            st.subheader("Query Result")
            st.dataframe(df.head(int(row_cap)), use_container_width=True)

    except Exception as e:
        st.error(f"Error: {e}")
