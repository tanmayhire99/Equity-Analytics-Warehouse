-- ===========================================================================
-- Seed dim_date with every calendar day from 2020-01-01 to 2026-12-31.
-- ---------------------------------------------------------------------------
-- Covers any historical backfill plus near-future daily runs. Weekends and
-- holidays are included so that every real trading day has a matching row;
-- weekend/holiday rows simply carry no facts. Idempotent via ON CONFLICT.
-- ===========================================================================

INSERT INTO dim_date (full_date, day_of_week, week_number, month, quarter, year, is_weekend)
SELECT
    d::date                                  AS full_date,
    TRIM(TO_CHAR(d, 'Day'))                  AS day_of_week,
    EXTRACT(WEEK    FROM d)::int             AS week_number,
    EXTRACT(MONTH   FROM d)::int             AS month,
    EXTRACT(QUARTER FROM d)::int             AS quarter,
    EXTRACT(YEAR    FROM d)::int             AS year,
    EXTRACT(DOW     FROM d) IN (0, 6)        AS is_weekend   -- 0=Sun, 6=Sat
FROM generate_series('2020-01-01'::date, '2026-12-31'::date, INTERVAL '1 day') AS d
ON CONFLICT (full_date) DO NOTHING;
