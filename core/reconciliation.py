"""
core/reconciliation.py

Cross-layer data quality checks: compares row counts and key numeric sums
across raw → staging → facts for a given partition.

Failures are logged to etl_error_log and returned as a list of messages.
This module does NOT raise exceptions — it records and continues.
"""

import sqlite3
from typing import Any

from core.etl_logging import log_error


ReconciliationResult = dict[str, Any]


def _row_count(conn: sqlite3.Connection, table: str, partition_key: str, partition_value: str) -> int:
    cursor = conn.execute(
        f'SELECT COUNT(*) FROM "{table}" WHERE "{partition_key}" LIKE ?',
        (f"{partition_value}%",),
    )
    row = cursor.fetchone()
    return row[0] if row else 0


def _column_sum(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    partition_key: str,
    partition_value: str,
) -> float:
    cursor = conn.execute(
        f'SELECT COALESCE(SUM(CAST("{column}" AS REAL)), 0) '
        f'FROM "{table}" WHERE "{partition_key}" LIKE ?',
        (f"{partition_value}%",),
    )
    row = cursor.fetchone()
    return float(row[0]) if row else 0.0


def run_reconciliation(
    conn: sqlite3.Connection,
    run_id: str,
    raw_table: str,
    staging_table: str,
    fact_tables: list[str],
    partition_key: str,
    partition_value: str,
    sum_columns: list[str] | None = None,
) -> list[str]:
    """
    Compare row counts across raw → staging and optionally verify column sums.

    Parameters
    ----------
    conn            : open SQLite connection
    run_id          : current pipeline run identifier
    raw_table       : e.g. "raw_orders"
    staging_table   : e.g. "stg_orders"
    fact_tables     : list of fct_* tables to include in row-count checks
    partition_key   : column used to scope comparisons
    partition_value : partition being checked
    sum_columns     : optional list of numeric columns to sum-check
                      between raw and staging

    Returns a list of reconciliation failure messages (empty = all clear).
    """
    failures: list[str] = []

    raw_count = _row_count(conn, raw_table, partition_key, partition_value)
    stg_count = _row_count(conn, staging_table, partition_key, partition_value)

    if raw_count == 0:
        msg = (
            f"RECONCILIATION WARNING: raw table '{raw_table}' has 0 rows "
            f"for partition {partition_value}"
        )
        failures.append(msg)
        log_error(conn, run_id, "reconciliation", msg)

    if stg_count == 0 and raw_count > 0:
        msg = (
            f"RECONCILIATION FAILURE: staging '{staging_table}' has 0 rows "
            f"but raw '{raw_table}' has {raw_count} rows "
            f"for partition {partition_value}"
        )
        failures.append(msg)
        log_error(conn, run_id, "reconciliation", msg)

    for fact_table in fact_tables:
        fct_count = _row_count(conn, fact_table, partition_key, partition_value)
        if fct_count == 0 and stg_count > 0:
            msg = (
                f"RECONCILIATION WARNING: fact table '{fact_table}' has 0 rows "
                f"but staging has {stg_count} rows "
                f"for partition {partition_value}"
            )
            failures.append(msg)
            log_error(conn, run_id, "reconciliation", msg)

    if sum_columns:
        for col in sum_columns:
            try:
                raw_sum = _column_sum(conn, raw_table, col, partition_key, partition_value)
                stg_sum = _column_sum(conn, staging_table, col, partition_key, partition_value)
                if abs(raw_sum - stg_sum) > 0.01:
                    msg = (
                        f"RECONCILIATION FAILURE: column '{col}' sum mismatch — "
                        f"raw={raw_sum:.4f}, staging={stg_sum:.4f} "
                        f"for partition {partition_value}"
                    )
                    failures.append(msg)
                    log_error(conn, run_id, "reconciliation", msg)
            except Exception as exc:
                msg = f"RECONCILIATION ERROR checking sum of '{col}': {exc}"
                failures.append(msg)
                log_error(conn, run_id, "reconciliation", msg)

    return failures
