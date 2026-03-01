"""Tests for core/schema_base.py"""

import sqlite3

import pytest

from core.schema_base import create_log_tables


def test_creates_etl_run_log():
    conn = sqlite3.connect(":memory:")
    create_log_tables(conn)
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='etl_run_log'")
    assert cursor.fetchone() is not None


def test_creates_etl_error_log():
    conn = sqlite3.connect(":memory:")
    create_log_tables(conn)
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='etl_error_log'")
    assert cursor.fetchone() is not None


def test_idempotent_on_second_call():
    conn = sqlite3.connect(":memory:")
    create_log_tables(conn)
    create_log_tables(conn)  # should not raise


def test_etl_run_log_columns():
    conn = sqlite3.connect(":memory:")
    create_log_tables(conn)
    cursor = conn.execute("PRAGMA table_info(etl_run_log)")
    columns = {row[1] for row in cursor.fetchall()}
    assert columns == {"run_id", "domain", "partition", "status", "rows_loaded", "start_time", "end_time", "notes"}


def test_etl_error_log_columns():
    conn = sqlite3.connect(":memory:")
    create_log_tables(conn)
    cursor = conn.execute("PRAGMA table_info(etl_error_log)")
    columns = {row[1] for row in cursor.fetchall()}
    assert columns == {"run_id", "layer", "error_message", "timestamp"}
