"""Single database connection factory used everywhere in the pipeline.

Centralising connection creation means the rest of the code never embeds a
connection string, and switching from local PostgreSQL to Supabase is a single
`.env` change with no code edits.
"""
from __future__ import annotations

import psycopg2
from psycopg2.extensions import connection as PgConnection

from config.settings import DATABASE_URL


def get_connection() -> PgConnection:
    """Open a new PostgreSQL connection using the configured DATABASE_URL."""
    return psycopg2.connect(DATABASE_URL)
