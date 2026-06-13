-- ===========================================================================
-- Equity Market Data Warehouse — Schema (star schema + operational tables)
-- ---------------------------------------------------------------------------
-- Target: PostgreSQL 15+. Core constructs are ANSI SQL and port to MySQL 8+,
-- SQL Server 2019+, and SingleStore with minor syntax changes (e.g.
-- BIGSERIAL -> BIGINT AUTO_INCREMENT, NUMERIC -> DECIMAL, JSONB -> JSON).
-- All statements are idempotent (IF NOT EXISTS) so setup can be re-run safely.
-- ===========================================================================

-- --- Dimension: company metadata -------------------------------------------
CREATE TABLE IF NOT EXISTS dim_stock (
    stock_id     SERIAL PRIMARY KEY,
    ticker       VARCHAR(20)  NOT NULL UNIQUE,   -- plain NSE symbol, e.g. 'RELIANCE'
    company_name VARCHAR(255) NOT NULL,
    sector       VARCHAR(100),
    exchange     VARCHAR(10)  NOT NULL,          -- 'NSE'
    is_active    BOOLEAN      DEFAULT TRUE,
    created_at   TIMESTAMP    DEFAULT NOW()
);

-- --- Dimension: calendar ----------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_id     SERIAL PRIMARY KEY,
    full_date   DATE    NOT NULL UNIQUE,
    day_of_week VARCHAR(10),                     -- 'Monday' ... 'Sunday'
    week_number INT,
    month       INT,
    quarter     INT,
    year        INT,
    is_weekend  BOOLEAN DEFAULT FALSE,
    is_holiday  BOOLEAN DEFAULT FALSE            -- reserved for NSE holiday flagging
);

-- --- Fact: one row per stock per trading day --------------------------------
-- adj_close is intentionally omitted: the NSE bhavcopy (jugaad-data source)
-- does not provide it. vwap is included as it ships free from the source and
-- is a meaningful India-specific intraday metric.
CREATE TABLE IF NOT EXISTS fact_prices (
    price_id   BIGSERIAL PRIMARY KEY,
    stock_id   INT NOT NULL REFERENCES dim_stock(stock_id),
    date_id    INT NOT NULL REFERENCES dim_date(date_id),
    open       NUMERIC(12, 4),
    high       NUMERIC(12, 4),
    low        NUMERIC(12, 4),
    close      NUMERIC(12, 4) NOT NULL,
    vwap       NUMERIC(12, 4),
    volume     BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (stock_id, date_id)                   -- first line of defence vs duplicates
);

-- --- Operational: rejected rows are captured, never silently dropped --------
CREATE TABLE IF NOT EXISTS error_log (
    error_id        BIGSERIAL PRIMARY KEY,
    pipeline_run_id INT,                          -- FK-soft link to pipeline_runs
    ticker          VARCHAR(20),
    trade_date      DATE,
    raw_data        JSONB,                        -- the original offending row
    error_type      VARCHAR(100),                 -- NULL_CLOSE, INVALID_PRICE, ...
    error_detail    TEXT,
    logged_at       TIMESTAMP DEFAULT NOW()
);

-- --- Operational: one row per pipeline execution (audit trail) --------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           SERIAL PRIMARY KEY,
    run_timestamp    TIMESTAMP NOT NULL DEFAULT NOW(),
    tickers_fetched  INT,
    rows_fetched     INT,
    rows_inserted    INT,
    rows_rejected    INT,
    duration_seconds FLOAT,
    status           VARCHAR(20),                 -- RUNNING, SUCCESS, PARTIAL, FAILED
    error_message    TEXT
);
