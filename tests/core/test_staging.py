"""Tests for core/staging.py"""

import sqlite3

import pandas as pd
import pytest

from core.staging import load_staging


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE raw_orders (
            order_id    TEXT,
            order_date  TEXT,
            amount      TEXT,
            _loaded_at  TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE stg_orders (
            order_id    TEXT,
            order_date  TEXT,
            amount      TEXT
        )
    """)
    return conn


def _identity(df: pd.DataFrame) -> pd.DataFrame:
    return df


def test_load_staging_writes_rows():
    conn = _make_conn()
    conn.execute("INSERT INTO raw_orders VALUES ('1','2025-01-05','10.0','2025-01-01T00:00:00Z')")
    conn.execute("INSERT INTO raw_orders VALUES ('2','2025-01-06','20.0','2025-01-01T00:00:00Z')")
    conn.commit()

    rows = load_staging(conn, "raw_orders", "stg_orders", "order_date", "2025-01", _identity, _identity)
    assert rows == 2
    count = conn.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
    assert count == 2


def test_load_staging_strips_loaded_at():
    conn = _make_conn()
    conn.execute("INSERT INTO raw_orders VALUES ('1','2025-01-05','10.0','ts')")
    conn.commit()

    load_staging(conn, "raw_orders", "stg_orders", "order_date", "2025-01", _identity, _identity)
    cols = [row[1] for row in conn.execute("PRAGMA table_info(stg_orders)").fetchall()]
    assert "_loaded_at" not in cols


def test_load_staging_deduplicates():
    conn = _make_conn()
    conn.execute("INSERT INTO raw_orders VALUES ('1','2025-01-05','10.0','ts')")
    conn.execute("INSERT INTO raw_orders VALUES ('1','2025-01-05','10.0','ts')")
    conn.commit()

    rows = load_staging(conn, "raw_orders", "stg_orders", "order_date", "2025-01", _identity, _identity)
    assert rows == 1


def test_load_staging_truncates_existing_partition():
    conn = _make_conn()
    conn.execute("INSERT INTO stg_orders VALUES ('old','2025-01-01','5.0')")
    conn.execute("INSERT INTO raw_orders VALUES ('new','2025-01-02','9.0','ts')")
    conn.commit()

    load_staging(conn, "raw_orders", "stg_orders", "order_date", "2025-01", _identity, _identity)
    rows = conn.execute("SELECT order_id FROM stg_orders WHERE order_date LIKE '2025-01%'").fetchall()
    ids = [r[0] for r in rows]
    assert "old" not in ids
    assert "new" in ids


def test_load_staging_returns_zero_for_empty_partition():
    conn = _make_conn()
    rows = load_staging(conn, "raw_orders", "stg_orders", "order_date", "2025-02", _identity, _identity)
    assert rows == 0


def test_load_staging_applies_transform():
    conn = _make_conn()
    conn.execute("INSERT INTO raw_orders VALUES ('  1  ','2025-01-05','10.0','ts')")
    conn.commit()

    def strip_transform(df: pd.DataFrame) -> pd.DataFrame:
        df["order_id"] = df["order_id"].str.strip()
        return df

    load_staging(conn, "raw_orders", "stg_orders", "order_date", "2025-01", strip_transform, _identity)
    row = conn.execute("SELECT order_id FROM stg_orders").fetchone()
    assert row[0] == "1"
