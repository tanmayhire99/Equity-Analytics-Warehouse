# Equity Market Data Pipeline & Analytics Warehouse

An end-to-end, automated data engineering pipeline that ingests daily **NSE**
equity data (a **NIFTY 50** watchlist), validates it, loads it into a normalized
**star schema** on PostgreSQL, and exposes SQL views and a table-returning
function as a clean analytics layer for downstream consumers.

Data source is the **official NSE bhavcopy** (via [`jugaad-data`](https://github.com/jugaad-py/jugaad-data)) —
the same end-of-day file Indian brokers use — so corporate actions and previous
closes come straight from the exchange rather than a third-party aggregator.

## Architecture

```
NSE bhavcopy (jugaad-data)
        │
        ▼
┌───────────────────┐
│   Fetch Stage     │  fetcher.py — OHLCV+VWAP per ticker; corrects NSE's
│                   │              UTC-encoded IST trade dates
└────────┬──────────┘
         ▼
┌───────────────────┐
│  Validate Stage   │  validator.py — 4 vectorized checks; bad rows → error_log
└────────┬──────────┘
         ▼
┌───────────────────┐
│ Transform Stage   │  transformer.py — resolves stock_id / date_id foreign keys
└────────┬──────────┘
         ▼
┌───────────────────┐
│   Load Stage      │  loader.py — bulk insert with ON CONFLICT DO NOTHING
└────────┬──────────┘
         ▼
┌───────────────────┐
│   Audit Stage     │  auditor.py — one row to pipeline_runs per execution
└───────────────────┘
         ▼
  PostgreSQL star schema
  ┌─────────────┬────────────┬─────────────┬───────────┬──────────────┐
  │  dim_stock  │  dim_date  │ fact_prices │ error_log │ pipeline_runs│
  └─────────────┴────────────┴─────────────┴───────────┴──────────────┘
         ▼
  SQL Views & Function (data virtualization layer)
  ┌──────────────────────────────┐
  │ vw_sector_weekly_performance │
  │ vw_top_movers_30d            │
  │ vw_volume_anomalies          │
  │ fn_ticker_report()           │
  └──────────────────────────────┘
```

## Schema

Five tables — a two-dimension star schema plus two operational tables:

| Table | Type | Purpose |
|---|---|---|
| `dim_stock` | Dimension | Company metadata (ticker, company, sector, exchange) |
| `dim_date` | Dimension | Calendar attributes (week, quarter, year, is_weekend) |
| `fact_prices` | Fact | One row per stock per trading day (OHLC, VWAP, volume) |
| `error_log` | Operational | Rejected rows captured with full JSON context — never silently dropped |
| `pipeline_runs` | Operational | Audit trail — one row per pipeline execution |

`fact_prices` stores `vwap` (a useful India-specific intraday metric, free from
the bhavcopy) and omits `adj_close` (the NSE bhavcopy does not provide it).

### Why normalize? (star schema benefit)

If TCS changes sector from "Information Technology" to "Technology", you update
**one row**:

```sql
UPDATE dim_stock SET sector = 'Technology' WHERE ticker = 'TCS';
```

Without normalization you would update **every price row** for TCS — thousands of
rows, with the risk of partial updates and inconsistent data.

## SQL Analytics Layer

SQL written for PostgreSQL 15+. Core constructs (CTEs, window functions, composite
indexes, table-returning functions) are ANSI SQL-compatible and portable to
MySQL 8+, SQL Server 2019+, and SingleStore with minor syntax adjustments
(e.g. `NUMERIC` → `DECIMAL`, `BIGSERIAL` → `BIGINT AUTO_INCREMENT`).

| Object | Type | Description |
|---|---|---|
| `vw_sector_weekly_performance` | View | Avg daily return % and volume by sector and week |
| `vw_top_movers_30d` | View | Top gainers/losers over 30 days (window functions, one row per stock) |
| `vw_volume_anomalies` | View | Days with volume > 2 std-devs above the 30-day mean (z-score) |
| `fn_ticker_report()` | Function | OHLCV + VWAP + 7-day/30-day moving averages for a ticker + date range |
| `mv_stock_snapshot` | Materialized view | Pre-aggregated per-stock snapshot (latest quote, 52w range, 30d return) for fast consumer reads |
| `sp_refresh_reporting()` | Procedure | Refreshes `mv_stock_snapshot`; called automatically after each load |
| `sp_purge_old_errors(days)` | Procedure | Data-retention maintenance (demonstrates procedure transaction control) |

A `FUNCTION` is used for `fn_ticker_report` because PostgreSQL procedures cannot
return result sets; procedures are reserved for actions (refresh, retention).

### SQL Views as a Virtual Data Layer

These views act as a **data virtualization layer**: downstream consumers
(dashboards, notebooks, the MIDAS multi-agent app) query the stable view/function
interface and stay decoupled from the physical fact/dimension layout. Schema
changes to `fact_prices` only require updating the views, not every consumer.

## Setup

Requires Python 3.10+ and Docker (for local PostgreSQL).

```bash
# 1. Create a virtualenv and install dependencies
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Start local PostgreSQL in Docker
docker run -d --name equity-pg \
  -e POSTGRES_USER=equity_user \
  -e POSTGRES_PASSWORD=equity_pass \
  -e POSTGRES_DB=equity_db \
  -p 5433:5432 postgres:16

# 3. Configure environment
cp .env.example .env          # defaults already match the Docker container above

# 4. Create schema, seed the calendar + stocks, and build the analytics layer
python setup_db.py            # runs entirely through psycopg2 — no psql client needed

# 5. Backfill ~3 months of history (run once)
python run_backfill.py

# 6. Run a single incremental pipeline pass
python run_once.py

# 7. Start the daily scheduler (Mon–Fri at 16:00 IST)
python scheduler.py
```

## Switching to Supabase (cloud PostgreSQL)

Set `DATABASE_URL_SUPABASE` in `.env` (Supabase → Settings → Database → Session
pooler URI; URL-encode special chars in the password), then run with
`DB_TARGET=supabase`:

```bash
DB_TARGET=supabase python setup_db.py
DB_TARGET=supabase python run_backfill.py
```

Nothing else changes — same code, same SQL.

## Orchestration

Two options share the same `pipeline/` code:

- **APScheduler** (lightweight, in-process): `python scheduler.py` — runs the
  pipeline Mon–Fri at 16:00 IST.
- **Airflow** (Dockerized, for orchestration + run observability):

  ```bash
  docker compose -f airflow/docker-compose.yaml up -d --build
  # http://localhost:8080  (admin / admin)
  ```

  The `equity_pipeline` DAG runs `ingest → quality_gate → report_snapshot`,
  reusing the pipeline modules and connecting to the warehouse via
  `host.docker.internal`. Airflow is purely the orchestrator — the ETL logic
  lives in one place.

## Running Tests

```bash
pytest tests/ -v
```

[GitHub Actions CI](.github/workflows/ci.yml) runs the suite on every push: it
spins up PostgreSQL, applies the full schema + analytics via `setup_db.py` (so
the SQL is validated too), then runs `pytest`. DB-backed tests skip cleanly when
no database is present.

## Consumer API

[`consumer_api.py`](consumer_api.py) is a small read-only client over the
analytics layer — the stable contract downstream apps call instead of hitting
market APIs at query time:

```python
import consumer_api
consumer_api.get_quote("RELIANCE")           # latest price, 52w range, 30d return
consumer_api.get_history("TCS")              # OHLCV + VWAP + 7d/30d moving averages
consumer_api.get_top_movers(limit=10)        # 30-day gainers/losers
consumer_api.get_sector_performance()        # weekly return by sector
```

It reads only views/function/snapshot (never the physical tables) and returns
JSON-native types, so consumers stay decoupled from the schema.

## Data Consumer (MIDAS)

This warehouse is the data backend for
[MIDAS — Agent-Driven Financial Intelligence](https://github.com/tanmayhire99/MultiAgentFinanceApp).
MIDAS can back its Indian-stock quote and historical/technical lookups with
`consumer_api` instead of hitting raw APIs at query time. (Fundamentals such as
P/E and market cap are not in the NSE bhavcopy and remain the app's own concern.)

## Cross-Engine Portability

The schema and analytics are ported to **MySQL 8** and verified running — see
[`docs/portability.md`](docs/portability.md), with dialect variants in
[`db/schema_mysql.sql`](db/schema_mysql.sql) and
[`db/analytics_mysql.sql`](db/analytics_mysql.sql). The same patterns translate
to SQL Server and SingleStore.

## Docs

- [`docs/query_performance.md`](docs/query_performance.md) — `EXPLAIN ANALYZE` index evidence
- [`docs/portability.md`](docs/portability.md) — MySQL 8 port and dialect deltas
