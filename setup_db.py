"""One-shot database setup: schema, calendar seed, analytics layer, stock seed.

Runs everything through psycopg2 so no local `psql` client is required and the
exact same script works against Docker PostgreSQL or Supabase. Idempotent —
safe to re-run at any time.

Usage:
    python setup_db.py
"""
from __future__ import annotations

from pathlib import Path

import psycopg2.extras
from loguru import logger

from config.settings import EXCHANGE, STOCKS
from db.connection import get_connection

DB_DIR = Path(__file__).resolve().parent / "db"


def _run_sql_file(conn, filename: str) -> None:
    path = DB_DIR / filename
    if not path.exists():
        logger.warning("Skipping {} (not found yet)", filename)
        return
    sql = path.read_text()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info("Applied {}", filename)


def seed_stocks(conn) -> int:
    """Upsert the dim_stock reference data from settings.STOCKS."""
    records = [
        (s["symbol"], s["company_name"], s["sector"], EXCHANGE)
        for s in STOCKS
    ]
    sql = """
        INSERT INTO dim_stock (ticker, company_name, sector, exchange)
        VALUES %s
        ON CONFLICT (ticker) DO UPDATE
            SET company_name = EXCLUDED.company_name,
                sector       = EXCLUDED.sector,
                exchange     = EXCLUDED.exchange
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, records)
    conn.commit()
    return len(records)


def _summarise(conn) -> None:
    with conn.cursor() as cur:
        for table in ("dim_stock", "dim_date", "fact_prices", "error_log", "pipeline_runs"):
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            logger.info("  {:<14} {} rows", table, cur.fetchone()[0])


def main() -> None:
    logger.info("Connecting to database...")
    conn = get_connection()
    try:
        _run_sql_file(conn, "schema.sql")
        _run_sql_file(conn, "seed_dim_date.sql")
        _run_sql_file(conn, "analytics.sql")  # may not exist on first runs
        n = seed_stocks(conn)
        logger.info("Seeded {} stocks into dim_stock", n)
        logger.info("Row counts:")
        _summarise(conn)
        logger.success("Database setup complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
