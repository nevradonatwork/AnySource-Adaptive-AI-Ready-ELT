"""Tests for core/ingestion.py"""

import os
import sqlite3
import tempfile

import pandas as pd
import pytest

from core.ingestion import load_raw, _archive_path


@pytest.fixture()
def temp_dirs():
    with tempfile.TemporaryDirectory() as base:
        input_dir = os.path.join(base, "input")
        archive_dir = os.path.join(base, "archive")
        os.makedirs(input_dir)
        yield input_dir, archive_dir


def _make_conn_with_raw_table(columns: list[str]) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    col_defs = ", ".join(f'"{c}" TEXT' for c in columns) + ', "_loaded_at" TEXT'
    conn.execute(f'CREATE TABLE raw_orders ({col_defs})')
    return conn


def test_load_raw_inserts_rows(temp_dirs):
    input_dir, archive_dir = temp_dirs
    csv_path = os.path.join(input_dir, "orders.csv")
    pd.DataFrame({"order_id": ["1", "2"], "amount": ["10.0", "20.0"]}).to_csv(csv_path, index=False)

    conn = _make_conn_with_raw_table(["order_id", "amount"])
    rows_inserted = load_raw(conn, csv_path, "raw_orders", archive_dir)

    assert rows_inserted == 2
    count = conn.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
    assert count == 2


def test_load_raw_adds_loaded_at(temp_dirs):
    input_dir, archive_dir = temp_dirs
    csv_path = os.path.join(input_dir, "orders.csv")
    pd.DataFrame({"order_id": ["1"]}).to_csv(csv_path, index=False)

    conn = _make_conn_with_raw_table(["order_id"])
    load_raw(conn, csv_path, "raw_orders", archive_dir)

    row = conn.execute("SELECT _loaded_at FROM raw_orders").fetchone()
    assert row[0] is not None
    assert "T" in row[0]


def test_load_raw_moves_file_to_archive(temp_dirs):
    input_dir, archive_dir = temp_dirs
    csv_path = os.path.join(input_dir, "orders.csv")
    pd.DataFrame({"order_id": ["1"]}).to_csv(csv_path, index=False)

    conn = _make_conn_with_raw_table(["order_id"])
    load_raw(conn, csv_path, "raw_orders", archive_dir)

    assert not os.path.exists(csv_path)
    archived = os.listdir(archive_dir)
    assert len(archived) == 1
    assert archived[0].startswith("orders_")
    assert archived[0].endswith(".csv")


def test_load_raw_raises_if_file_missing(temp_dirs):
    _, archive_dir = temp_dirs
    conn = _make_conn_with_raw_table(["order_id"])
    with pytest.raises(FileNotFoundError):
        load_raw(conn, "/nonexistent/path.csv", "raw_orders", archive_dir)


def test_archive_path_format():
    result = _archive_path("/some/path/orders_jan.csv", "/archive")
    filename = os.path.basename(result)
    assert filename.startswith("orders_jan_")
    assert filename.endswith("Z.csv")
    assert len(filename) == len("orders_jan_") + len("20250101T000000Z") + len(".csv")
