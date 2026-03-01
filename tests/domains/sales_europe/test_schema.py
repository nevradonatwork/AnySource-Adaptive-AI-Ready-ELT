"""Tests for domains/sales_europe/schema.py"""

import sqlite3

import pytest

from domains.sales_europe.schema import (
    RAW_TABLE,
    STAGING_TABLE,
    DIMENSION_CONFIGS,
    RECONCILIATION_SUM_COLUMNS,
    create_tables,
)


@pytest.fixture()
def conn():
    c = sqlite3.connect(":memory:")
    yield c
    c.close()


def _table_exists(conn, name):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def _columns(conn, table):
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def test_constants():
    assert RAW_TABLE == "raw_orders"
    assert STAGING_TABLE == "stg_orders"


def test_create_tables_raw(conn):
    create_tables(conn)
    assert _table_exists(conn, "raw_orders")


def test_create_tables_staging(conn):
    create_tables(conn)
    assert _table_exists(conn, "stg_orders")


def test_create_tables_dim_customer(conn):
    create_tables(conn)
    assert _table_exists(conn, "dim_customer")


def test_create_tables_fct_sales(conn):
    create_tables(conn)
    assert _table_exists(conn, "fct_sales")


def test_raw_table_has_loaded_at(conn):
    create_tables(conn)
    assert "_loaded_at" in _columns(conn, "raw_orders")


def test_dim_customer_has_scd2_columns(conn):
    create_tables(conn)
    cols = _columns(conn, "dim_customer")
    assert {"valid_from", "valid_to", "is_current"}.issubset(cols)


def test_fct_sales_has_required_columns(conn):
    create_tables(conn)
    cols = _columns(conn, "fct_sales")
    assert {"order_date", "region", "total_amount", "order_count", "total_quantity"}.issubset(cols)


def test_create_tables_is_idempotent(conn):
    create_tables(conn)
    create_tables(conn)  # should not raise


def test_dimension_configs_structure():
    assert len(DIMENSION_CONFIGS) == 1
    cfg = DIMENSION_CONFIGS[0]
    assert cfg["table"] == "dim_customer"
    assert "customer_id" in cfg["natural_key"]
    assert "customer_email" in cfg["tracked_columns"]
    assert "region" in cfg["tracked_columns"]


def test_reconciliation_sum_columns():
    assert "amount" in RECONCILIATION_SUM_COLUMNS
