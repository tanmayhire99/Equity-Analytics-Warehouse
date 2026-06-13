-- ===========================================================================
-- MySQL 8 dialect of analytics.sql — portability proof (verified on MySQL 8.4).
-- Load with:
--   docker exec -i equity-mysql mysql -uroot -prootpass equity_db < db/analytics_mysql.sql
--
-- Dialect deltas vs PostgreSQL:
--   DISTINCT ON (col)            -> ROW_NUMBER() OVER (...) then WHERE rn = 1
--   INTERVAL '30 days'           -> INTERVAL 30 DAY
--   ROUND(x::NUMERIC, 2)         -> ROUND(x, 2)            (no cast needed)
--   Materialized view + REFRESH  -> a table + TRUNCATE/INSERT refresh procedure
--                                   (MySQL has no materialized views)
--   fn_ticker_report (FUNCTION)  -> sp_ticker_report (PROCEDURE) — in MySQL a
--                                   PROCEDURE *can* return a result set, so the
--                                   PostgreSQL "must be a function" constraint
--                                   does not apply here.
--   CREATE OR REPLACE PROCEDURE  -> DROP PROCEDURE IF EXISTS + CREATE
-- CTEs and window functions are supported natively in MySQL 8.
-- ===========================================================================

-- --- View 1: sector weekly performance ------------------------------------
CREATE OR REPLACE VIEW vw_sector_weekly_performance AS
SELECT
    s.sector,
    d.`year`,
    d.week_number,
    ROUND(AVG((fp.`close` - fp.`open`) / NULLIF(fp.`open`, 0) * 100), 2) AS avg_daily_return_pct,
    SUM(fp.volume)             AS total_volume,
    COUNT(DISTINCT s.stock_id) AS stocks_in_sector
FROM fact_prices fp
JOIN dim_stock s ON fp.stock_id = s.stock_id
JOIN dim_date  d ON fp.date_id  = d.date_id
GROUP BY s.sector, d.`year`, d.week_number
ORDER BY d.`year` DESC, d.week_number DESC;

-- --- View 2: top movers (DISTINCT ON -> ROW_NUMBER + WHERE rn = 1) ---------
CREATE OR REPLACE VIEW vw_top_movers_30d AS
WITH ranked AS (
    SELECT
        s.ticker,
        s.company_name,
        s.sector,
        FIRST_VALUE(fp.`close`) OVER w AS close_30d_ago,
        LAST_VALUE(fp.`close`)  OVER w AS close_today,
        ROW_NUMBER() OVER (PARTITION BY fp.stock_id ORDER BY d.full_date DESC) AS rn
    FROM fact_prices fp
    JOIN dim_stock s ON fp.stock_id = s.stock_id
    JOIN dim_date  d ON fp.date_id  = d.date_id
    WHERE d.full_date >= CURRENT_DATE - INTERVAL 30 DAY
    WINDOW w AS (
        PARTITION BY fp.stock_id ORDER BY d.full_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
)
SELECT
    ticker, company_name, sector, close_30d_ago, close_today,
    ROUND((close_today - close_30d_ago) / NULLIF(close_30d_ago, 0) * 100, 2) AS return_30d_pct
FROM ranked
WHERE rn = 1
ORDER BY return_30d_pct DESC;

-- --- View 3: volume anomalies ---------------------------------------------
CREATE OR REPLACE VIEW vw_volume_anomalies AS
WITH avg_volume AS (
    SELECT fp.stock_id, AVG(fp.volume) AS avg_vol_30d, STDDEV(fp.volume) AS stddev_vol_30d
    FROM fact_prices fp
    JOIN dim_date d ON fp.date_id = d.date_id
    WHERE d.full_date >= CURRENT_DATE - INTERVAL 30 DAY
    GROUP BY fp.stock_id
)
SELECT
    s.ticker,
    s.company_name,
    d.full_date,
    fp.volume,
    ROUND(av.avg_vol_30d)                                                AS avg_vol_30d,
    ROUND((fp.volume - av.avg_vol_30d) / NULLIF(av.stddev_vol_30d, 0), 2) AS z_score
FROM fact_prices fp
JOIN dim_stock  s  ON fp.stock_id = s.stock_id
JOIN dim_date   d  ON fp.date_id  = d.date_id
JOIN avg_volume av ON fp.stock_id = av.stock_id
WHERE fp.volume > av.avg_vol_30d + (2 * av.stddev_vol_30d)
ORDER BY d.full_date DESC, z_score DESC;

-- --- Reporting snapshot: table + refresh procedure (matview equivalent) ----
CREATE TABLE IF NOT EXISTS stock_snapshot (
    ticker         VARCHAR(20) PRIMARY KEY,
    company_name   VARCHAR(255),
    sector         VARCHAR(100),
    latest_date    DATE,
    latest_close   DECIMAL(12, 4),
    high_52w       DECIMAL(12, 4),
    low_52w        DECIMAL(12, 4),
    return_30d_pct DECIMAL(8, 2)
);

DROP PROCEDURE IF EXISTS sp_refresh_reporting;
DELIMITER //
CREATE PROCEDURE sp_refresh_reporting()
BEGIN
    -- MySQL has no materialized views; "refresh" = truncate + repopulate.
    TRUNCATE TABLE stock_snapshot;
    INSERT INTO stock_snapshot
        (ticker, company_name, sector, latest_date, latest_close, high_52w, low_52w, return_30d_pct)
    WITH latest AS (
        SELECT fp.stock_id, d.full_date, fp.`close`,
               ROW_NUMBER() OVER (PARTITION BY fp.stock_id ORDER BY d.full_date DESC) AS rn
        FROM fact_prices fp JOIN dim_date d ON fp.date_id = d.date_id
    ),
    w52 AS (
        SELECT fp.stock_id, MAX(fp.high) AS high_52w, MIN(fp.low) AS low_52w
        FROM fact_prices fp JOIN dim_date d ON fp.date_id = d.date_id
        WHERE d.full_date >= CURRENT_DATE - INTERVAL 365 DAY
        GROUP BY fp.stock_id
    ),
    w30 AS (
        SELECT fp.stock_id,
               FIRST_VALUE(fp.`close`) OVER w AS c0,
               LAST_VALUE(fp.`close`)  OVER w AS c1,
               ROW_NUMBER() OVER (PARTITION BY fp.stock_id ORDER BY d.full_date DESC) AS rn
        FROM fact_prices fp JOIN dim_date d ON fp.date_id = d.date_id
        WHERE d.full_date >= CURRENT_DATE - INTERVAL 30 DAY
        WINDOW w AS (
            PARTITION BY fp.stock_id ORDER BY d.full_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
    )
    SELECT s.ticker, s.company_name, s.sector,
           l.full_date, l.`close`,
           w52.high_52w, w52.low_52w,
           ROUND((w30.c1 - w30.c0) / NULLIF(w30.c0, 0) * 100, 2)
    FROM dim_stock s
    JOIN latest l ON s.stock_id = l.stock_id AND l.rn = 1
    LEFT JOIN w52 ON s.stock_id = w52.stock_id
    LEFT JOIN w30 ON s.stock_id = w30.stock_id AND w30.rn = 1;
END //
DELIMITER ;

-- --- Per-ticker report: PROCEDURE returning a result set -------------------
DROP PROCEDURE IF EXISTS sp_ticker_report;
DELIMITER //
CREATE PROCEDURE sp_ticker_report(IN p_ticker VARCHAR(20), IN p_from DATE, IN p_to DATE)
BEGIN
    SELECT
        d.full_date AS trade_date,
        fp.`open`, fp.high, fp.low, fp.`close`, fp.vwap, fp.volume,
        ROUND(AVG(fp.`close`) OVER (ORDER BY d.full_date ROWS BETWEEN 6  PRECEDING AND CURRENT ROW), 2) AS ma_7d,
        ROUND(AVG(fp.`close`) OVER (ORDER BY d.full_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) AS ma_30d
    FROM fact_prices fp
    JOIN dim_stock s ON fp.stock_id = s.stock_id
    JOIN dim_date  d ON fp.date_id  = d.date_id
    WHERE s.ticker = p_ticker
      AND d.full_date BETWEEN p_from AND p_to
    ORDER BY d.full_date;
END //
DELIMITER ;

-- Additive index (composite (stock_id, date_id) already exists via UNIQUE).
CREATE INDEX idx_fact_prices_date ON fact_prices (date_id);
