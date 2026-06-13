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
