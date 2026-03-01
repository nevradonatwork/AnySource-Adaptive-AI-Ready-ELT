"""Tests for core/dimensions.py"""

import sqlite3

import pytest

from core.dimensions import upsert_dimension


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE stg_orders (
            customer_id  TEXT,
            email        TEXT,
            region       TEXT,
            order_date   TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE dim_customer (
            customer_id  TEXT,
            email        TEXT,
            region       TEXT,
            valid_from   DATE,
            valid_to     DATE,
            is_current   INTEGER DEFAULT 1
        )
    """)
    return conn


def test_inserts_new_row_when_no_existing_record():
    conn = _make_conn()
    conn.execute("INSERT INTO stg_orders VALUES ('C1','a@b.com','EU','2025-01-01')")
    conn.commit()

    inserted = upsert_dimension(
        conn, "stg_orders", "dim_customer",
        natural_key=["customer_id"], tracked_columns=["email", "region"],
        partition_key="order_date", partition_value="2025-01",
    )
    assert inserted == 1
    row = conn.execute("SELECT * FROM dim_customer WHERE customer_id='C1'").fetchone()
    assert row is not None
    assert row[5] == 1  # is_current


def test_no_insert_when_unchanged():
    conn = _make_conn()
    conn.execute("INSERT INTO dim_customer VALUES ('C1','a@b.com','EU','2025-01-01',NULL,1)")
    conn.execute("INSERT INTO stg_orders VALUES ('C1','a@b.com','EU','2025-02-01')")
    conn.commit()

    inserted = upsert_dimension(
        conn, "stg_orders", "dim_customer",
        natural_key=["customer_id"], tracked_columns=["email", "region"],
        partition_key="order_date", partition_value="2025-02",
    )
    assert inserted == 0
    count = conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    assert count == 1


def test_expires_old_and_inserts_new_on_change():
    conn = _make_conn()
    conn.execute("INSERT INTO dim_customer VALUES ('C1','old@b.com','EU','2025-01-01',NULL,1)")
    conn.execute("INSERT INTO stg_orders VALUES ('C1','new@b.com','EU','2025-02-01')")
    conn.commit()

    inserted = upsert_dimension(
        conn, "stg_orders", "dim_customer",
        natural_key=["customer_id"], tracked_columns=["email"],
        partition_key="order_date", partition_value="2025-02",
    )
    assert inserted == 1

    rows = conn.execute("SELECT is_current, email FROM dim_customer ORDER BY valid_from").fetchall()
    assert len(rows) == 2
    assert rows[0][0] == 0   # old row expired
    assert rows[1][0] == 1   # new row active
    assert rows[1][1] == "new@b.com"


def test_returns_zero_for_empty_partition():
    conn = _make_conn()
    inserted = upsert_dimension(
        conn, "stg_orders", "dim_customer",
        natural_key=["customer_id"], tracked_columns=["email"],
        partition_key="order_date", partition_value="2025-03",
    )
    assert inserted == 0
