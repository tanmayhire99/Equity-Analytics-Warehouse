-- ===========================================================================
-- Equity Warehouse — Analytics Layer (views, table-returning function, indexes)
-- ---------------------------------------------------------------------------
-- SQL Compatibility Note:
--   Written for PostgreSQL 15+. Core constructs (CTEs, window functions,
--   composite indexes, table-returning functions) are ANSI SQL-compatible and
--   portable to MySQL 8+, SQL Server 2019+, and SingleStore with minor syntax
--   adjustments (e.g. NUMERIC -> DECIMAL, BIGSERIAL -> BIGINT AUTO_INCREMENT,
--   and rewriting the PL/pgSQL function as the target engine's procedure form).
--
-- These views form a DATA VIRTUALIZATION LAYER: downstream consumers (the MIDAS
-- multi-agent app, dashboards, notebooks) query these stable view/function
-- interfaces and stay decoupled from the physical fact/dim table layout.
-- All objects use CREATE OR REPLACE so this file is safe to re-run.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- View 1: Sector-wise weekly performance
--   Average intraday return and traded volume, grouped by sector and ISO week.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_sector_weekly_performance AS
SELECT
    s.sector,
    d.year,
    d.week_number,
    ROUND(AVG((fp.close - fp.open) / NULLIF(fp.open, 0) * 100)::NUMERIC, 2) AS avg_daily_return_pct,
    SUM(fp.volume)             AS total_volume,
    COUNT(DISTINCT s.stock_id) AS stocks_in_sector
FROM fact_prices fp
JOIN dim_stock s ON fp.stock_id = s.stock_id
JOIN dim_date  d ON fp.date_id  = d.date_id
GROUP BY s.sector, d.year, d.week_number
ORDER BY d.year DESC, d.week_number DESC;


-- ---------------------------------------------------------------------------
-- View 2: Top movers over the last 30 days
--   One row per stock (DISTINCT ON), comparing first vs last close in the
--   window. FIRST_VALUE/LAST_VALUE are computed per-row, so without DISTINCT ON
--   the CTE would emit N identical rows per stock (the C-2 bug from the plan).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_top_movers_30d AS
WITH ranked AS (
    SELECT
        s.ticker,
        s.company_name,
        s.sector,
        FIRST_VALUE(fp.close) OVER w AS close_30d_ago,
        LAST_VALUE(fp.close)  OVER w AS close_today
    FROM fact_prices fp
    JOIN dim_stock s ON fp.stock_id = s.stock_id
    JOIN dim_date  d ON fp.date_id  = d.date_id
    WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
    WINDOW w AS (
        PARTITION BY fp.stock_id
        ORDER BY d.full_date ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
)
SELECT DISTINCT ON (ticker)
    ticker,
    company_name,
    sector,
    close_30d_ago,
    close_today,
    ROUND((close_today - close_30d_ago) / NULLIF(close_30d_ago, 0) * 100, 2) AS return_30d_pct
FROM ranked
ORDER BY ticker, return_30d_pct DESC;


-- ---------------------------------------------------------------------------
-- View 3: Volume anomaly detection
--   Days where traded volume exceeds the 30-day mean by > 2 standard deviations
--   (z-score), surfacing unusual activity.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_volume_anomalies AS
WITH avg_volume AS (
    SELECT
        fp.stock_id,
        AVG(fp.volume)    AS avg_vol_30d,
        STDDEV(fp.volume) AS stddev_vol_30d
    FROM fact_prices fp
    JOIN dim_date d ON fp.date_id = d.date_id
    WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY fp.stock_id
)
SELECT
    s.ticker,
    s.company_name,
    d.full_date,
    fp.volume,
    ROUND(av.avg_vol_30d)                                                   AS avg_vol_30d,
    ROUND((fp.volume - av.avg_vol_30d) / NULLIF(av.stddev_vol_30d, 0), 2)   AS z_score
FROM fact_prices fp
JOIN dim_stock  s  ON fp.stock_id = s.stock_id
JOIN dim_date   d  ON fp.date_id  = d.date_id
JOIN avg_volume av ON fp.stock_id = av.stock_id
WHERE fp.volume > av.avg_vol_30d + (2 * av.stddev_vol_30d)
ORDER BY d.full_date DESC, z_score DESC;


-- ---------------------------------------------------------------------------
-- Function: per-ticker trend report (C-1 fix)
--   A PostgreSQL PROCEDURE cannot return a result set, so this is a FUNCTION
--   using RETURN QUERY. Returns OHLCV + VWAP plus 7-row and 30-row moving
--   averages of close, computed inline with window functions.
--   Call with:  SELECT * FROM fn_ticker_report('RELIANCE', '2026-01-01', '2026-06-30');
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_ticker_report(
    p_ticker    VARCHAR,
    p_from_date DATE,
    p_to_date   DATE
)
RETURNS TABLE (
    trade_date DATE,
    open       NUMERIC,
    high       NUMERIC,
    low        NUMERIC,
    close      NUMERIC,
    vwap       NUMERIC,
    volume     BIGINT,
    ma_7d      NUMERIC,
    ma_30d     NUMERIC
)
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM dim_stock WHERE ticker = p_ticker) THEN
        RAISE EXCEPTION 'Ticker % not found in dim_stock', p_ticker;
    END IF;

    RETURN QUERY
    SELECT
        d.full_date,
        fp.open,
        fp.high,
        fp.low,
        fp.close,
        fp.vwap,
        fp.volume,
        ROUND(AVG(fp.close) OVER (
            ORDER BY d.full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2) AS ma_7d,
        ROUND(AVG(fp.close) OVER (
            ORDER BY d.full_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2) AS ma_30d
    FROM fact_prices fp
    JOIN dim_stock s ON fp.stock_id = s.stock_id
    JOIN dim_date  d ON fp.date_id  = d.date_id
    WHERE s.ticker     = p_ticker
      AND d.full_date BETWEEN p_from_date AND p_to_date
    ORDER BY d.full_date;
END;
$$;


-- ---------------------------------------------------------------------------
-- Indexes
--   NOTE on what is intentionally NOT created:
--     * The composite (stock_id, date_id) index already exists — it is created
--       by the UNIQUE (stock_id, date_id) constraint on fact_prices, which both
--       prevents duplicate rows AND serves ticker+date lookups. Re-creating it
--       would just add write/storage overhead, so we don't.
--     * dim_stock(ticker) is likewise already indexed by its UNIQUE constraint.
--   The index below is the genuinely additive one: it accelerates the
--   cross-sectional, date-range scans used by the sector and volume views,
--   which filter on date across all stocks.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fact_prices_date ON fact_prices (date_id);


-- ---------------------------------------------------------------------------
-- Materialized view: per-stock snapshot for fast consumer reads
--   Pre-computes the expensive per-stock aggregates (latest quote, 52-week
--   high/low, 30-day return and average volume) so downstream consumers (e.g.
--   the MIDAS app's get_quote) read one indexed row instead of re-scanning the
--   fact table on every call. Refreshed by sp_refresh_reporting() after loads.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_stock_snapshot AS
WITH latest AS (
    SELECT DISTINCT ON (fp.stock_id)
        fp.stock_id,
        d.full_date AS latest_date,
        fp.close    AS latest_close,
        fp.vwap     AS latest_vwap,
        fp.volume   AS latest_volume
    FROM fact_prices fp
    JOIN dim_date d ON fp.date_id = d.date_id
    ORDER BY fp.stock_id, d.full_date DESC
),
w52 AS (
    SELECT fp.stock_id, MAX(fp.high) AS high_52w, MIN(fp.low) AS low_52w
    FROM fact_prices fp
    JOIN dim_date d ON fp.date_id = d.date_id
    WHERE d.full_date >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY fp.stock_id
),
w30 AS (
    SELECT DISTINCT ON (fp.stock_id)
        fp.stock_id,
        AVG(fp.volume) OVER (PARTITION BY fp.stock_id) AS avg_vol_30d,
        FIRST_VALUE(fp.close) OVER w AS close_30d_ago,
        LAST_VALUE(fp.close)  OVER w AS close_latest
    FROM fact_prices fp
    JOIN dim_date d ON fp.date_id = d.date_id
    WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
    WINDOW w AS (
        PARTITION BY fp.stock_id ORDER BY d.full_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
)
SELECT
    s.ticker,
    s.company_name,
    s.sector,
    l.latest_date,
    l.latest_close,
    l.latest_vwap,
    l.latest_volume,
    w52.high_52w,
    w52.low_52w,
    ROUND(w30.avg_vol_30d)                                                       AS avg_vol_30d,
    ROUND((w30.close_latest - w30.close_30d_ago) / NULLIF(w30.close_30d_ago, 0) * 100, 2) AS return_30d_pct
FROM dim_stock s
LEFT JOIN latest l   ON s.stock_id = l.stock_id
LEFT JOIN w52        ON s.stock_id = w52.stock_id
LEFT JOIN w30        ON s.stock_id = w30.stock_id;

-- Unique index enables REFRESH ... CONCURRENTLY (non-blocking refresh) at scale
-- and speeds single-ticker consumer lookups.
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_stock_snapshot_ticker ON mv_stock_snapshot (ticker);


-- ---------------------------------------------------------------------------
-- Stored PROCEDURES (vs the FUNCTION above)
--   Procedures perform actions/maintenance and return no result set; they are
--   invoked with CALL and — unlike functions — may manage transactions
--   (COMMIT/ROLLBACK). (Note: in MySQL/SQL Server a PROCEDURE *can* return a
--   result set; that engine difference is why fn_ticker_report is a FUNCTION
--   here — see the portability notes.)
-- ---------------------------------------------------------------------------

-- Refresh the reporting layer. Called after each successful load (automation)
-- and safe to run inside the pipeline's transaction. Swap in
-- "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_stock_snapshot" for non-blocking
-- refreshes at scale (requires the unique index above and autocommit).
CREATE OR REPLACE PROCEDURE sp_refresh_reporting()
LANGUAGE plpgsql AS $$
BEGIN
    REFRESH MATERIALIZED VIEW mv_stock_snapshot;
    RAISE NOTICE 'mv_stock_snapshot refreshed';
END;
$$;

-- Data-retention maintenance: delete error_log rows older than the retention
-- window. Demonstrates procedure transaction control via COMMIT, so it must be
-- called on an autocommit connection (a standalone maintenance job), e.g.:
--     CALL sp_purge_old_errors(90);
CREATE OR REPLACE PROCEDURE sp_purge_old_errors(p_retention_days INT DEFAULT 90)
LANGUAGE plpgsql AS $$
DECLARE
    v_deleted INT;
BEGIN
    DELETE FROM error_log
    WHERE logged_at < NOW() - make_interval(days => p_retention_days);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    COMMIT;
    RAISE NOTICE 'Purged % error_log row(s) older than % days', v_deleted, p_retention_days;
END;
$$;
