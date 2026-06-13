# Equity Market Data Pipeline & Analytics Warehouse

An end-to-end, automated data engineering pipeline that ingests daily **NSE**
equity data, validates it, loads it into a normalized **star schema** on
PostgreSQL, and exposes SQL views and a table-returning function as a clean
analytics layer for downstream consumers.

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

Point `DATABASE_URL` in `.env` at your Supabase connection string and re-run
`python setup_db.py`. Nothing else changes — same code, same SQL.

## Running Tests

```bash
pytest tests/ -v
```

## Data Consumer

This warehouse is the data backend for
[MIDAS — Agent-Driven Financial Intelligence](https://github.com/tanmayhire99/MultiAgentFinanceApp).
MIDAS queries the SQL views and `fn_ticker_report()` to ground its multi-agent
portfolio analysis in validated, structured market data instead of hitting raw
APIs at query time.

## Docs

- [`docs/query_performance.md`](docs/query_performance.md) — `EXPLAIN ANALYZE` index evidence
