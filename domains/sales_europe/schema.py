"""
domains/sales_europe/schema.py

All CREATE TABLE IF NOT EXISTS statements for the sales_europe domain.
"""

import sqlite3

RAW_TABLE     = "raw_orders"
STAGING_TABLE = "stg_orders"

DIMENSION_CONFIGS: list[dict] = [
    {
        "table": "dim_customer",
        "natural_key": ["customer_id"],
        "tracked_columns": ["customer_email", "region"],
    }
]

RECONCILIATION_SUM_COLUMNS: list[str] = ["amount"]


def create_tables(conn: sqlite3.Connection) -> None:
    """Create all tables for the sales_europe domain."""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_orders (
            order_id        TEXT,
            customer_id     TEXT,
            customer_email  TEXT,
            region          TEXT,
            order_date      TEXT,
            product_id      TEXT,
            quantity        TEXT,
            unit_price      TEXT,
            amount          TEXT,
            _loaded_at      TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stg_orders (
            order_id        TEXT    NOT NULL,
            customer_id     TEXT    NOT NULL,
            customer_email  TEXT    NOT NULL,
            region          TEXT    NOT NULL,
            order_date      TEXT    NOT NULL,
            product_id      TEXT    NOT NULL,
            quantity        REAL    NOT NULL,
            unit_price      REAL    NOT NULL,
            amount          REAL    NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_id     TEXT    NOT NULL,
            customer_email  TEXT    NOT NULL,
            region          TEXT    NOT NULL,
            valid_from      DATE    NOT NULL,
            valid_to        DATE,
            is_current      INTEGER NOT NULL DEFAULT 1
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fct_sales (
            order_date      TEXT    NOT NULL,
            region          TEXT    NOT NULL,
            total_amount    REAL    NOT NULL,
            order_count     INTEGER NOT NULL,
            total_quantity  REAL    NOT NULL
        )
    """)

    conn.commit()
