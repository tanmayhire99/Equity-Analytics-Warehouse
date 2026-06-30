# equity-pipeline — Roadmap & Progress

_Part of the **MIDAS** platform (data → reasoning → systematic execution). The
combined vision + cross-project scorecard lives in `docs/MIDAS_VISION.md` in the
MultiAgentFinanceApp (FinAI) repo. Last updated: 2026-06-30._

## What this is
The **data backbone**: an automated pipeline that ingests daily NSE bhavcopy
(NIFTY-50) via `jugaad-data`, validates it, loads a normalized **PostgreSQL
star schema** (`dim_stock`, `dim_date`, `fact_prices` + `error_log`,
`pipeline_runs`), and exposes a stable analytics layer — SQL views,
`fn_ticker_report()`, the `mv_stock_snapshot` materialized view, and a read-only
`consumer_api.py`. Downstream (the FinAI app) reads that layer, not market APIs.

## Current state (2026-06)
- **51 tests**; star schema + analytics layer live on **both** local Postgres
  (~12.8k `fact_prices` rows) and **Supabase** (~13.5k rows), kept consistent.
- Analytics views set `security_invoker = true` (Supabase-lint clean).
- Reusable **data-quality monitor** (`pipeline/quality.py`) + standalone runner +
  Airflow DAG + post-ingest scheduler hook (see Completed).
- Derived **technical analytics** (`get_technicals`) exposed via `consumer_api`
  and consumed by FinAI's `indian_stock` worker.

## Phased roadmap
| Phase | Work | Status |
|---|---|---|
| **A** | Productionize the daily run + **data-quality alerting** | ✅ checks + alerting + standalone runner + post-ingest hook; ☐ prod scheduler + Supabase auto-fresh |
| **B** | Widen universe + value-add analytics | ✅ **technical analytics** shipped; ☐ universe widening (NIFTY-500); ☐ corporate-action adjustment. **Fundamentals (P/E, market cap) intentionally out of scope** — needs earnings/shares the bhavcopy lacks; the project rejects yfinance by design |
| **C** | **Intraday / near-real-time** feed; TimescaleDB / partitioning for scale | ☐ not started |
| **D** | Expose a proper **authed data API** (or dbt-modeled data product) | ☐ not started |

## Completed progress (this session)
- ✅ **Data-quality checks + alerting** (`pipeline/quality.py`) — pure evaluators
  (freshness, completeness, duplicates, price-sanity, row-volume, error-rate) +
  thin DB wrappers + structured `QualityReport`; `emit_alert` logs and POSTs a
  Slack-compatible message on FAIL (`QUALITY_ALERT_WEBHOOK`). Standalone
  `run_quality.py` (cron/CI/Supabase, non-zero exit on FAIL); the Airflow DAG +
  the daily scheduler now call the shared module. **Green on the live warehouse.**
  `bc0b984`
- ✅ **Technical-analytics layer** (`pipeline/technicals.py` + `consumer_api`'s
  `get_technicals`) — SMA 20/50/200, trailing 1m/3m/6m returns, annualised
  volatility, max drawdown, trend vs SMA-50, computed from warehoused closes via
  the analytics layer (no new source). Verified live (RELIANCE). `491c4f2`

## Remaining progress
- ☐ **Productionize the daily scheduler** in prod (Airflow/cron under a process
  manager) and keep **Supabase auto-fresh** (it drifted until re-backfilled).
- ☐ **Widen the universe** beyond NIFTY-50 (NIFTY-500 → all-NSE). Note: the
  fetcher is per-symbol via jugaad-data, so this needs a batch/bhavcopy path to
  stay reliable at scale.
- ☐ **Corporate-action adjustment** (splits / dividends) for clean long series.
- ☐ **Phase C — intraday / real-time** feed; TimescaleDB or partitioning.
- ☐ **Phase D — authed data API** (or a dbt-modeled data product) for external
  consumers; add auth to the consumer layer.

## Progress
v1 warehouse + secured analytics + dual-target deployment + **quality monitoring
and technical analytics now shipped**. **~55% of the "full data product" vision**
(up from ~45%). The biggest remaining levers: a productionized scheduler that
keeps Supabase fresh automatically, and widening the universe.
