# Text-to-SQL Agent (theLook + BigQuery)

A Streamlit app that converts natural language questions into **BigQuery Standard SQL** using an LLM, with:
- **Metric Dictionary** (metrics.py) to ground definitions + formulas
- **Semantic Layer rules** (semantic.py) to enforce business meaning (time fields, fact tables, joins)
- **SQL Guardrails** (guard.py) to restrict to allowlisted tables + safe query types
- **Schema grounding + caching** (schema_cache.py) to reduce hallucinated columns and speed up reruns
- **BigQuery dry-run + scan cap + one-shot auto-fix** (bq_runner.py)

---

## 1) Features

### What it does
- Generate **ONE** BigQuery Standard SQL query from English questions
- Enforce:
  - SQL-only output (no markdown, no semicolon)
  - allowlisted dataset tables only
  - read-only queries (SELECT/WITH)
  - existing columns only (via schema prompt context)
- Show explainability:
  - **Assumptions applied** (assumptions.py)
  - **Metric Cards** (metric_cards.py): Metric Name / Definition / Time field / Formula
- Execute safely:
  - dry-run to estimate scan GB
  - block queries above scan cap
  - optional **one-shot** auto-fix if BigQuery errors

---

## 2) Repo Structure


- app.py # Streamlit UI + orchestration
- agent_sql.py # LLM: generate_sql + fix_sql
- assumptions.py # infer_assumptions(question) -> list[str]
- bq_runner.py # dry-run / execute / one-shot fix
- clients.py # get_bq_client(), get_openai_client()
- guard.py # SQLGuard: allowlist + safety checks
- metric_cards.py # render_metric_cards_simplified()
- metrics.py # METRICS + QUALIFIED_METRICS + intent matching + prompt renderer
- schema_cache.py # INFORMATION_SCHEMA fetch + local json cache + allowlist builder
- semantic.py # SEMANTIC_RULES + normalize_sql + looks_incomplete_sql
