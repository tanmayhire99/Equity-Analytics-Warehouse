# Index Performance Evidence

Captured against the local warehouse (**12,317 fact rows, 51 stocks, ~1 year**).

## Composite index on (stock_id, date_id)

The composite index is provided by the `UNIQUE (stock_id, date_id)` constraint on
`fact_prices` (index name `fact_prices_stock_id_date_id_key`). It serves a dual
purpose: it prevents duplicate rows **and** accelerates ticker + date-range
lookups — the most common access pattern.

### Query: all prices for RELIANCE

```sql
EXPLAIN ANALYZE
SELECT fp.* FROM fact_prices fp
JOIN dim_stock s ON fp.stock_id = s.stock_id
WHERE s.ticker = 'RELIANCE';
```

```
Nested Loop  (actual time=... rows=246)
  ->  Seq Scan on dim_stock s            -- 51-row dim table: seq scan is optimal
        Filter: (ticker = 'RELIANCE')
  ->  Bitmap Heap Scan on fact_prices fp
        ->  Bitmap Index Scan on fact_prices_stock_id_date_id_key
              Index Cond: (stock_id = s.stock_id)
Execution Time: 0.152 ms
```

The composite index `fact_prices_stock_id_date_id_key` resolves the per-stock
lookup against the 12k-row fact table (246 matching rows in 0.15 ms). Note the
planner *correctly* sequential-scans `dim_stock` — at 51 rows an index would be
slower than a scan, which is the optimizer making the right call, not a missing
index.

## Note on data volume and the date index

The additive `idx_fact_prices_date` targets cross-sectional, date-range scans
(the sector and volume-anomaly views, which filter on date across all stocks).
Its benefit grows with table size; at ~12k rows the planner may still scan for
wide date ranges that touch a large fraction of rows — index value increases as
daily data accumulates over months/years.
