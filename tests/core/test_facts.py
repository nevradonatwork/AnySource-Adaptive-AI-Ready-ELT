"""Tests for core/facts.py"""

import sqlite3

import pytest

from core.facts import load_facts


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE stg_orders (
            order_id    TEXT,
            order_date  TEXT,
            region      TEXT,
            amount      REAL
        )
    """)
    conn.execute("""
        CREATE TABLE fct_sales (
            order_date    TEXT,
            region        TEXT,
            total_amount  REAL,
            order_count   INTEGER
        )
    """)
    return conn


_FACTS_CONFIG = [
    {
        "target_table": "fct_sales",
        "source_table": "stg_orders",
        "group_by": ["order_date", "region"],
        "aggregations": {
            "total_amount": ("amount", "SUM"),
            "order_count": ("order_id", "COUNT"),
        },
        "partition_column": "order_date",
    }
]


def test_load_facts_aggregates_correctly():
    conn = _make_conn()
    # Same order_date for EU rows so they collapse into one group
    conn.execute("INSERT INTO stg_orders VALUES ('1','2025-01-05','EU',10.0)")
    conn.execute("INSERT INTO stg_orders VALUES ('2','2025-01-05','EU',20.0)")
    conn.execute("INSERT INTO stg_orders VALUES ('3','2025-01-05','US',5.0)")
    conn.commit()

    total = load_facts(conn, _FACTS_CONFIG, "2025-01")
    assert total == 2  # 2 groups: EU and US (same date)

    eu = conn.execute("SELECT total_amount, order_count FROM fct_sales WHERE region='EU'").fetchone()
    assert eu[0] == 30.0
    assert eu[1] == 2

    us = conn.execute("SELECT total_amount FROM fct_sales WHERE region='US'").fetchone()
    assert us[0] == 5.0


def test_load_facts_replaces_existing_partition():
    conn = _make_conn()
    conn.execute("INSERT INTO fct_sales VALUES ('2025-01-01','EU',999.0,99)")
    conn.execute("INSERT INTO stg_orders VALUES ('1','2025-01-05','EU',10.0)")
    conn.commit()

    load_facts(conn, _FACTS_CONFIG, "2025-01")
    rows = conn.execute("SELECT total_amount FROM fct_sales WHERE region='EU'").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 10.0


def test_load_facts_returns_zero_for_empty_partition():
    conn = _make_conn()
    total = load_facts(conn, _FACTS_CONFIG, "2025-02")
    assert total == 0


def test_load_facts_unsupported_function_raises():
    conn = _make_conn()
    conn.execute("INSERT INTO stg_orders VALUES ('1','2025-01-05','EU',10.0)")
    conn.commit()

    bad_config = [
        {
            "target_table": "fct_sales",
            "source_table": "stg_orders",
            "group_by": ["order_date"],
            "aggregations": {"total_amount": ("amount", "MEDIAN")},
            "partition_column": "order_date",
        }
    ]
    with pytest.raises(ValueError, match="Unsupported aggregation function"):
        load_facts(conn, bad_config, "2025-01")
