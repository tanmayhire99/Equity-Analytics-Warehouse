# Cross-Engine SQL Portability

The warehouse is built on PostgreSQL, but the schema and analytics are written
with portable, ANSI-style SQL. This document records the MySQL 8 port and the
exact dialect deltas — proving the design moves to other relational engines
(MySQL, SQL Server, SingleStore) with mechanical, well-understood changes.

The MySQL variants live in [`db/schema_mysql.sql`](../db/schema_mysql.sql) and
[`db/analytics_mysql.sql`](../db/analytics_mysql.sql), and were **verified
running on MySQL 8.4** (Docker `mysql:8`) — not just claimed.

## Dialect deltas (PostgreSQL → MySQL 8)

| Concern | PostgreSQL | MySQL 8 |
|---|---|---|
| Auto-increment PK | `SERIAL` / `BIGSERIAL` | `INT` / `BIGINT AUTO_INCREMENT` |
| Exact decimal | `NUMERIC(p,s)` | `DECIMAL(p,s)` |
| JSON payload | `JSONB` | `JSON` |
| Float | `FLOAT` | `DOUBLE` |
| Default timestamp | `DEFAULT NOW()` | `DEFAULT CURRENT_TIMESTAMP` |
| One-row-per-group | `DISTINCT ON (col)` | `ROW_NUMBER() OVER (...)` + `WHERE rn = 1` |
| Date math | `CURRENT_DATE - INTERVAL '30 days'` | `CURRENT_DATE - INTERVAL 30 DAY` |
| Numeric rounding | `ROUND(x::NUMERIC, 2)` | `ROUND(x, 2)` (no cast) |
| Pre-aggregated table | Materialized view + `REFRESH` | Table + `TRUNCATE`/`INSERT` refresh procedure |
| Per-ticker report | **must be a FUNCTION** (`RETURN QUERY`) | can be a **PROCEDURE** returning a result set |
| Replace routine | `CREATE OR REPLACE PROCEDURE` | `DROP PROCEDURE IF EXISTS` + `CREATE` |

CTEs and window functions (`FIRST_VALUE`, `LAST_VALUE`, `ROW_NUMBER`, windowed
`AVG` for moving averages) are supported natively by **both** engines, so the
analytical core ports unchanged.

## The interesting one: function vs procedure

In PostgreSQL a `PROCEDURE` cannot return a result set, so `fn_ticker_report`
*must* be a `FUNCTION` with `RETURN QUERY`. In MySQL (and SQL Server) a
`PROCEDURE` returns a result set directly, so the same logic becomes
`sp_ticker_report`. The portable rule of thumb: keep set-returning logic in
views/functions and treat procedures as actions — then each engine's idiom is a
thin wrapper.

## Verified output on MySQL 8.4

```
-- vw_top_movers_30d (DISTINCT ON rewritten as ROW_NUMBER)
ticker     return_30d_pct
RELIANCE   0.93
TCS        0.90
INFY       0.88

-- CALL sp_refresh_reporting(); SELECT ... FROM stock_snapshot;
ticker     latest_close  high_52w  low_52w  return_30d_pct
INFY       115.0000      115.0000  94.0000  0.88
RELIANCE   109.0000      115.0000  94.0000  0.93
TCS        112.0000      115.0000  94.0000  0.90

-- CALL sp_ticker_report('RELIANCE', ...)  (procedure returns a result set)
trade_date  open    high    low    close   vwap    volume   ma_7d   ma_30d
2026-04-24  101.00  107.00  95.00  104.00  101.00  1051000  104.00  104.00
...

-- vw_volume_anomalies (z-score) caught an injected volume spike
ticker     volume     z_score
RELIANCE   99000000   4.58
```

## SQL Server / SingleStore

The same deltas apply with engine-specific spellings: `IDENTITY` for
auto-increment and `TOP`/`OFFSET FETCH` on SQL Server; SingleStore is largely
MySQL-wire-compatible and accepts the MySQL DDL with minor storage-clause
changes. Window functions and CTEs are available on both.
