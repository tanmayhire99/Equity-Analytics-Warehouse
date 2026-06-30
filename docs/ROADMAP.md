# equity-pipeline — Roadmap

_Part of the **MIDAS** platform (data → reasoning → systematic execution). The
combined vision + cross-project scorecard lives in `docs/MIDAS_VISION.md` in the
MultiAgentFinanceApp (FinAI) repo._

## What this is
The **data backbone**: an automated pipeline that ingests daily NSE bhavcopy
(NIFTY-50) via `jugaad-data`, validates it, loads a normalized **PostgreSQL
star schema** (`dim_stock`, `dim_date`, `fact_prices` + `error_log`,
`pipeline_runs`), and exposes a stable analytics layer — SQL views,
`fn_ticker_report()`, the `mv_stock_snapshot` materialized view, and a read-only
`consumer_api.py`. Downstream (the FinAI app) reads that layer, not market APIs.

## Current state (2026-06)
- **27 tests**; star schema + analytics layer live on **both** local Postgres
  (~12.8k `fact_prices` rows) and **Supabase** (~13.5k rows), kept consistent.
- Analytics views set `security_invoker = true` (Supabase-lint clean; respect
  the caller's RLS).
- Airflow DAGs for ingest / quality / maintenance; `consumer_api` is the stable
  contract the MIDAS app's Indian-stock worker consumes.

## Strengths
Clean, exchange-sourced, decoupled consumer contract, secured, dual-target
(local ↔ Supabase via one env switch).

## Gaps
- **NIFTY-50 and EOD only**; no fundamentals (P/E, market cap), no intraday.
- No corporate-action adjustment (splits/dividends).
- Daily scheduler not productionized (Supabase drifted until re-backfilled).
- No prod data-quality alerting; `consumer_api` has no auth layer.

## Phased roadmap
| Phase | Work |
|---|---|
| **A** | Productionize the daily scheduler (Airflow/cron) + **data-quality alerting**; keep Supabase auto-fresh |
| **B** | Widen universe (NIFTY-500 → all-NSE) + add a **fundamentals** source + corporate-action adjustment |
| **C** | **Intraday / near-real-time** feed; TimescaleDB or partitioning for scale |
| **D** | Expose a proper **authed data API** (or dbt-modeled data product) |

## Progress
Solid, secured, integrated **v1 (~45% of the "full data product" vision)**. The
warehouse + analytics layer + dual-target deployment are done; universe breadth,
fundamentals, intraday, and a productionized scheduler are ahead. Recommended
next: **Phase A** (productionize the scheduler + quality alerting) then **B**
(fundamentals), since both directly enrich FinAI's analysis.
