"""Tests for core/etl_logging.py"""

import sqlite3
from datetime import datetime, timezone

import pytest

from core.schema_base import create_log_tables
from core.etl_logging import log_run, log_error


def _make_conn():
    conn = sqlite3.connect(":memory:")
    create_log_tables(conn)
    return conn


def test_log_run_inserts_row():
    conn = _make_conn()
    start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2025, 1, 1, 0, 1, 0, tzinfo=timezone.utc)
    log_run(conn, "run-1", "sales", "2025-01", "success", 100, start, end)
    cursor = conn.execute("SELECT COUNT(*) FROM etl_run_log")
    assert cursor.fetchone()[0] == 1


def test_log_run_stores_correct_values():
    conn = _make_conn()
    start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2025, 1, 1, 0, 1, 0, tzinfo=timezone.utc)
    log_run(conn, "run-42", "my_domain", "2025-03", "failure", 0, start, end, notes="oops")
    row = conn.execute("SELECT * FROM etl_run_log").fetchone()
    assert row[0] == "run-42"
    assert row[1] == "my_domain"
    assert row[2] == "2025-03"
    assert row[3] == "failure"
    assert row[4] == 0
    assert row[7] == "oops"


def test_log_error_inserts_row():
    conn = _make_conn()
    log_error(conn, "run-1", "ingestion", "Something went wrong")
    cursor = conn.execute("SELECT COUNT(*) FROM etl_error_log")
    assert cursor.fetchone()[0] == 1


def test_log_error_stores_correct_values():
    conn = _make_conn()
    log_error(conn, "run-99", "staging", "bad column")
    row = conn.execute("SELECT run_id, layer, error_message FROM etl_error_log").fetchone()
    assert row[0] == "run-99"
    assert row[1] == "staging"
    assert row[2] == "bad column"


def test_log_error_records_timestamp():
    conn = _make_conn()
    log_error(conn, "run-1", "facts", "err")
    row = conn.execute("SELECT timestamp FROM etl_error_log").fetchone()
    assert row[0] is not None
    assert "T" in row[0]
