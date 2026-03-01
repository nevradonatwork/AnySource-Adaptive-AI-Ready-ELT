"""
End-to-end integration test for the sales_europe domain.

Uses an in-memory SQLite database — never touches the real domain DB.
Exercises the full pipeline: raw → staging → dimensions → facts → reconciliation.
"""

import sqlite3
from io import StringIO

import pandas as pd
import pytest

from core.schema_base import create_log_tables
from core.ingestion import load_raw
from core.staging import load_staging
from core.dimensions import upsert_dimension
from core.facts import load_facts
from core.reconciliation import run_reconciliation

from domains.sales_europe.schema import (
    RAW_TABLE, STAGING_TABLE, DIMENSION_CONFIGS,
    RECONCILIATION_SUM_COLUMNS, create_tables,
)
from domains.sales_europe.transformations import apply as transform
from domains.sales_europe.validation import validate
from domains.sales_europe.facts_config import FACTS_CONFIG


SAMPLE_CSV = """\
order_id,customer_id,customer_email,region,order_date,product_id,quantity,unit_price,amount
1001,101,alice@example.com,EU,2025-01-05,PRD-A,2,49.99,99.98
1002,102,bob@example.com,US,2025-01-06,PRD-B,1,199.00,199.00
1003,101,alice@example.com,EU,2025-01-10,PRD-C,3,29.99,89.97
1004,103,carol@example.com,EU,2025-01-12,PRD-A,1,49.99,49.99
1005,102,bob@example.com,US,2025-01-15,PRD-B,2,199.00,398.00
1006,104,dave@example.com,APAC,2025-01-20,PRD-D,5,9.99,49.95
1007,101,alice@example.com,EU,2025-01-22,PRD-A,1,49.99,49.99
1008,105,eve@example.com,LATAM,2025-01-25,PRD-C,2,29.99,59.98
1009,103,carol@example.com,EU,2025-01-28,PRD-A,1,49.99,49.99
1010,102,bob@example.com,US,2025-01-30,PRD-B,3,199.00,597.00
"""

PARTITION = "2025-01"
RUN_ID = "test-run-001"


@pytest.fixture()
def conn():
    """In-memory DB with all tables and log tables created."""
    c = sqlite3.connect(":memory:")
    create_log_tables(c)
    create_tables(c)
    yield c
    c.close()


def _load_csv_into_raw(conn):
    """Helper: insert sample CSV rows directly into raw_orders."""
    df = pd.read_csv(StringIO(SAMPLE_CSV), dtype=str)
    df["_loaded_at"] = "2025-02-01T00:00:00Z"
    cols = list(df.columns)
    col_list = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(["?"] * len(cols))
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
    conn.executemany(
        f'INSERT INTO "{RAW_TABLE}" ({col_list}) VALUES ({placeholders})', rows
    )
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Layer tests
# ---------------------------------------------------------------------------

class TestRawLayer:
    def test_raw_row_count(self, conn):
        n = _load_csv_into_raw(conn)
        assert n == 10

    def test_raw_has_loaded_at(self, conn):
        _load_csv_into_raw(conn)
        row = conn.execute("SELECT _loaded_at FROM raw_orders LIMIT 1").fetchone()
        assert row[0] is not None


class TestStagingLayer:
    def test_staging_row_count(self, conn):
        _load_csv_into_raw(conn)
        n = load_staging(conn, RAW_TABLE, STAGING_TABLE, "order_date", PARTITION, transform, validate)
        assert n == 10

    def test_staging_region_uppercased(self, conn):
        _load_csv_into_raw(conn)
        load_staging(conn, RAW_TABLE, STAGING_TABLE, "order_date", PARTITION, transform, validate)
        regions = conn.execute("SELECT DISTINCT region FROM stg_orders").fetchall()
        for (r,) in regions:
            assert r == r.upper()

    def test_staging_email_lowercased(self, conn):
        _load_csv_into_raw(conn)
        load_staging(conn, RAW_TABLE, STAGING_TABLE, "order_date", PARTITION, transform, validate)
        emails = conn.execute("SELECT customer_email FROM stg_orders").fetchall()
        for (e,) in emails:
            assert e == e.lower()

    def test_staging_amount_is_numeric(self, conn):
        _load_csv_into_raw(conn)
        load_staging(conn, RAW_TABLE, STAGING_TABLE, "order_date", PARTITION, transform, validate)
        row = conn.execute("SELECT amount FROM stg_orders WHERE order_id='1001'").fetchone()
        assert float(row[0]) == pytest.approx(99.98)

    def test_staging_no_loaded_at_column(self, conn):
        _load_csv_into_raw(conn)
        load_staging(conn, RAW_TABLE, STAGING_TABLE, "order_date", PARTITION, transform, validate)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(stg_orders)").fetchall()}
        assert "_loaded_at" not in cols


class TestDimensionsLayer:
    def test_dim_customer_row_count(self, conn):
        _load_csv_into_raw(conn)
        load_staging(conn, RAW_TABLE, STAGING_TABLE, "order_date", PARTITION, transform, validate)
        cfg = DIMENSION_CONFIGS[0]
        inserted = upsert_dimension(
            conn, STAGING_TABLE, cfg["table"],
            cfg["natural_key"], cfg["tracked_columns"],
            "order_date", PARTITION,
        )
        # 5 unique customers in sample CSV
        assert inserted == 5

    def test_dim_customer_all_current(self, conn):
        _load_csv_into_raw(conn)
        load_staging(conn, RAW_TABLE, STAGING_TABLE, "order_date", PARTITION, transform, validate)
        cfg = DIMENSION_CONFIGS[0]
        upsert_dimension(
            conn, STAGING_TABLE, cfg["table"],
            cfg["natural_key"], cfg["tracked_columns"],
            "order_date", PARTITION,
        )
        not_current = conn.execute(
            "SELECT COUNT(*) FROM dim_customer WHERE is_current != 1"
        ).fetchone()[0]
        assert not_current == 0

    def test_dim_customer_scd2_expires_old_on_change(self, conn):
        """Simulate email change for customer 101 in a second partition."""
        _load_csv_into_raw(conn)
        load_staging(conn, RAW_TABLE, STAGING_TABLE, "order_date", PARTITION, transform, validate)
        cfg = DIMENSION_CONFIGS[0]
        upsert_dimension(
            conn, STAGING_TABLE, cfg["table"],
            cfg["natural_key"], cfg["tracked_columns"],
            "order_date", PARTITION,
        )

        # Insert a second-month staging row with changed email for customer 101
        conn.execute("""
            INSERT INTO stg_orders
            (order_id, customer_id, customer_email, region, order_date, product_id, quantity, unit_price, amount)
            VALUES ('2001', '101', 'alice_new@example.com', 'EU', '2025-02-01', 'PRD-A', 1, 49.99, 49.99)
        """)
        conn.commit()

        upsert_dimension(
            conn, STAGING_TABLE, cfg["table"],
            cfg["natural_key"], cfg["tracked_columns"],
            "order_date", "2025-02",
        )

        rows = conn.execute(
            "SELECT is_current, customer_email FROM dim_customer WHERE customer_id='101' ORDER BY valid_from"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] == 0                          # old row expired
        assert rows[0][1] == "alice@example.com"
        assert rows[1][0] == 1                          # new row active
        assert rows[1][1] == "alice_new@example.com"


class TestFactsLayer:
    def _setup(self, conn):
        _load_csv_into_raw(conn)
        load_staging(conn, RAW_TABLE, STAGING_TABLE, "order_date", PARTITION, transform, validate)

    def test_fct_sales_regions_present(self, conn):
        self._setup(conn)
        load_facts(conn, FACTS_CONFIG, PARTITION)
        regions = {r[0] for r in conn.execute("SELECT DISTINCT region FROM fct_sales").fetchall()}
        assert regions == {"EU", "US", "APAC", "LATAM"}

    def test_fct_sales_eu_total_amount(self, conn):
        self._setup(conn)
        load_facts(conn, FACTS_CONFIG, PARTITION)
        # EU orders: 99.98 + 89.97 + 49.99 + 49.99 + 49.99 = 339.92
        eu_total = conn.execute(
            "SELECT SUM(total_amount) FROM fct_sales WHERE region='EU'"
        ).fetchone()[0]
        assert eu_total == pytest.approx(339.92, rel=1e-3)

    def test_fct_sales_us_order_count(self, conn):
        self._setup(conn)
        load_facts(conn, FACTS_CONFIG, PARTITION)
        # US orders: 1002, 1005, 1010 → 3 orders across different dates
        us_count = conn.execute(
            "SELECT SUM(order_count) FROM fct_sales WHERE region='US'"
        ).fetchone()[0]
        assert us_count == 3

    def test_fct_sales_total_quantity(self, conn):
        self._setup(conn)
        load_facts(conn, FACTS_CONFIG, PARTITION)
        # Total quantity across all regions: 2+1+3+1+2+5+1+2+1+3 = 21
        total_qty = conn.execute(
            "SELECT SUM(total_quantity) FROM fct_sales"
        ).fetchone()[0]
        assert total_qty == pytest.approx(21.0)

    def test_fct_sales_partition_replace(self, conn):
        self._setup(conn)
        load_facts(conn, FACTS_CONFIG, PARTITION)
        # Running again should replace, not double-count
        load_facts(conn, FACTS_CONFIG, PARTITION)
        total_qty = conn.execute("SELECT SUM(total_quantity) FROM fct_sales").fetchone()[0]
        assert total_qty == pytest.approx(21.0)


class TestReconciliationLayer:
    def _full_pipeline(self, conn):
        _load_csv_into_raw(conn)
        load_staging(conn, RAW_TABLE, STAGING_TABLE, "order_date", PARTITION, transform, validate)
        load_facts(conn, FACTS_CONFIG, PARTITION)

    def test_no_reconciliation_failures_on_clean_data(self, conn):
        self._full_pipeline(conn)
        failures = run_reconciliation(
            conn, RUN_ID, RAW_TABLE, STAGING_TABLE, ["fct_sales"],
            "order_date", PARTITION, sum_columns=RECONCILIATION_SUM_COLUMNS,
        )
        assert failures == []

    def test_reconciliation_detects_sum_mismatch(self, conn):
        _load_csv_into_raw(conn)
        # Intentionally write wrong amount to staging
        conn.execute("""
            INSERT INTO stg_orders
            (order_id, customer_id, customer_email, region, order_date, product_id, quantity, unit_price, amount)
            VALUES ('9999', '999', 'x@x.com', 'EU', '2025-01-01', 'PRD-X', 1, 1.0, 1.0)
        """)
        conn.commit()
        # Raw has 10 rows summing to ~1643.85, staging has different total
        failures = run_reconciliation(
            conn, RUN_ID, RAW_TABLE, STAGING_TABLE, [],
            "order_date", PARTITION, sum_columns=["amount"],
        )
        assert any("sum mismatch" in f for f in failures)
