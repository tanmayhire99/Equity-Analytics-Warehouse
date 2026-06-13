# Index Performance Evidence

Captured against the local warehouse (590 fact rows, 10 stocks, ~59 trading days).

## Composite index on (stock_id, date_id)

The composite index is provided by the `UNIQUE (stock_id, date_id)` constraint on
`fact_prices` (index name `fact_prices_stock_id_date_id_key`). It serves a dual
purpose: it prevents duplicate rows **and** accelerates ticker + date-range
lookups — the most common access pattern.

### Query: all prices for RELIANCE in Apr–Jun 2026

```sql
EXPLAIN ANALYZE
SELECT * FROM fact_prices fp
JOIN dim_stock s ON fp.stock_id = s.stock_id
JOIN dim_date  d ON fp.date_id  = d.date_id
WHERE s.ticker = 'RELIANCE'
  AND d.full_date BETWEEN '2026-04-01' AND '2026-06-30';
```

```
Nested Loop  (cost=5.15..25.81 rows=1 ...) (actual time=0.068..0.115 rows=49)
  ->  Nested Loop  (actual time=0.050..0.063 rows=59)
        ->  Index Scan using dim_stock_ticker_key on dim_stock s
              Index Cond: ((ticker)::text = 'RELIANCE'::text)
        ->  Bitmap Heap Scan on fact_prices fp
              Recheck Cond: (stock_id = s.stock_id)
              ->  Bitmap Index Scan on fact_prices_stock_id_date_id_key
                    Index Cond: (stock_id = s.stock_id)
  ->  Index Scan using dim_date_pkey on dim_date d
        Index Cond: (date_id = fp.date_id)
Planning Time: 0.883 ms
Execution Time: 0.177 ms
```

All three joins resolve via index scans — no sequential scan on the fact table.
`fact_prices_stock_id_date_id_key` (the composite index) handles the stock lookup,
and `dim_stock_ticker_key` (the ticker UNIQUE index) handles the ticker filter.

## Note on data volume

At this volume the planner already prefers index scans for selective lookups. The
additive `idx_fact_prices_date` index targets cross-sectional, date-range scans
(the sector and volume-anomaly views, which filter on date across all stocks);
its benefit grows as the fact table accumulates more daily rows.
