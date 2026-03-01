"""Tests for core/reconciliation.py"""

import sqlite3

import pytest

from core.schema_base import create_log_tables
from core.reconciliation import run_reconciliation


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    create_log_tables(conn)
    conn.execute("CREATE TABLE raw_orders (order_date TEXT, amount REAL)")
    conn.execute("CREATE TABLE stg_orders (order_date TEXT, amount REAL)")
    conn.execute("CREATE TABLE fct_sales  (order_date TEXT, total_amount REAL)")
    return conn


def test_no_failures_when_counts_match():
    conn = _make_conn()
    conn.execute("INSERT INTO raw_orders VALUES ('2025-01-01', 10.0)")
    conn.execute("INSERT INTO stg_orders VALUES ('2025-01-01', 10.0)")
    conn.execute("INSERT INTO fct_sales  VALUES ('2025-01-01', 10.0)")
    conn.commit()

    failures = run_reconciliation(
        conn, "run-1", "raw_orders", "stg_orders", ["fct_sales"],
        "order_date", "2025-01",
    )
    assert failures == []


def test_failure_when_staging_empty_and_raw_has_rows():
    conn = _make_conn()
    conn.execute("INSERT INTO raw_orders VALUES ('2025-01-01', 10.0)")
    conn.commit()

    failures = run_reconciliation(
        conn, "run-1", "raw_orders", "stg_orders", ["fct_sales"],
        "order_date", "2025-01",
    )
    assert any("FAILURE" in f for f in failures)


def test_warning_when_raw_is_empty():
    conn = _make_conn()
    failures = run_reconciliation(
        conn, "run-1", "raw_orders", "stg_orders", ["fct_sales"],
        "order_date", "2025-01",
    )
    assert any("WARNING" in f for f in failures)


def test_sum_mismatch_produces_failure():
    conn = _make_conn()
    conn.execute("INSERT INTO raw_orders VALUES ('2025-01-01', 100.0)")
    conn.execute("INSERT INTO stg_orders VALUES ('2025-01-01', 50.0)")
    conn.commit()

    failures = run_reconciliation(
        conn, "run-1", "raw_orders", "stg_orders", [],
        "order_date", "2025-01",
        sum_columns=["amount"],
    )
    assert any("sum mismatch" in f for f in failures)


def test_sum_match_produces_no_failure():
    conn = _make_conn()
    conn.execute("INSERT INTO raw_orders VALUES ('2025-01-01', 100.0)")
    conn.execute("INSERT INTO stg_orders VALUES ('2025-01-01', 100.0)")
    conn.commit()

    failures = run_reconciliation(
        conn, "run-1", "raw_orders", "stg_orders", [],
        "order_date", "2025-01",
        sum_columns=["amount"],
    )
    assert failures == []


def test_failures_written_to_error_log():
    conn = _make_conn()
    # raw is empty → warning is logged
    run_reconciliation(
        conn, "run-99", "raw_orders", "stg_orders", [],
        "order_date", "2025-01",
    )
    count = conn.execute(
        "SELECT COUNT(*) FROM etl_error_log WHERE run_id='run-99'"
    ).fetchone()[0]
    assert count >= 1
