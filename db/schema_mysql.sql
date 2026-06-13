-- ===========================================================================
-- MySQL 8 dialect of schema.sql — portability proof (verified on MySQL 8.4).
-- Load with:
--   docker exec -i equity-mysql mysql -uroot -prootpass equity_db < db/schema_mysql.sql
--
-- Dialect deltas vs PostgreSQL:
--   SERIAL        -> INT AUTO_INCREMENT
--   BIGSERIAL     -> BIGINT AUTO_INCREMENT
--   NUMERIC(p,s)  -> DECIMAL(p,s)
--   JSONB         -> JSON
--   FLOAT         -> DOUBLE
--   NOW()         -> CURRENT_TIMESTAMP
-- The star schema, constraints, and indexing strategy are otherwise identical.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS dim_stock (
    stock_id     INT AUTO_INCREMENT PRIMARY KEY,
    ticker       VARCHAR(20)  NOT NULL UNIQUE,
    company_name VARCHAR(255) NOT NULL,
    sector       VARCHAR(100),
    exchange     VARCHAR(10)  NOT NULL,
    is_active    BOOLEAN      DEFAULT TRUE,
    created_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id     INT AUTO_INCREMENT PRIMARY KEY,
    full_date   DATE NOT NULL UNIQUE,
    day_of_week VARCHAR(10),
    week_number INT,
    `month`     INT,
    quarter     INT,
    `year`      INT,
    is_weekend  BOOLEAN DEFAULT FALSE,
    is_holiday  BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS fact_prices (
    price_id   BIGINT AUTO_INCREMENT PRIMARY KEY,
    stock_id   INT NOT NULL,
    date_id    INT NOT NULL,
    `open`     DECIMAL(12, 4),
    high       DECIMAL(12, 4),
    low        DECIMAL(12, 4),
    `close`    DECIMAL(12, 4) NOT NULL,
    vwap       DECIMAL(12, 4),
    volume     BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (stock_id, date_id),
    FOREIGN KEY (stock_id) REFERENCES dim_stock(stock_id),
    FOREIGN KEY (date_id)  REFERENCES dim_date(date_id)
);

CREATE TABLE IF NOT EXISTS error_log (
    error_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
    pipeline_run_id INT,
    ticker          VARCHAR(20),
    trade_date      DATE,
    raw_data        JSON,
    error_type      VARCHAR(100),
    error_detail    TEXT,
    logged_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           INT AUTO_INCREMENT PRIMARY KEY,
    run_timestamp    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tickers_fetched  INT,
    rows_fetched     INT,
    rows_inserted    INT,
    rows_rejected    INT,
    duration_seconds DOUBLE,
    status           VARCHAR(20),
    error_message    TEXT
);
