"""
core/dimensions.py

Dimension upserts with SCD Type 2.

When a tracked attribute changes:
  - The existing active row is expired: valid_to = today, is_current = 0
  - A new row is inserted: valid_from = today, valid_to = NULL, is_current = 1

SCD2 configuration (natural_key, tracked_columns) lives in the domain's schema.py
and is passed in as parameters — this module stays domain-agnostic.
"""

import sqlite3
from datetime import date
from typing import Any

import pandas as pd


def upsert_dimension(
    conn: sqlite3.Connection,
    staging_table: str,
    dim_table: str,
    natural_key: list[str],
    tracked_columns: list[str],
    partition_key: str,
    partition_value: str,
) -> int:
    """
    Upsert rows from staging_table into dim_table using SCD Type 2 logic.

    Parameters
    ----------
    conn             : open SQLite connection
    staging_table    : source staging table name
    dim_table        : target dimension table name
    natural_key      : columns that uniquely identify a business entity
                       (e.g. ["customer_id"])
    tracked_columns  : columns whose changes trigger a new SCD2 version
                       (e.g. ["email", "region"])
    partition_key    : column used to filter staging data
    partition_value  : partition being processed

    Returns the number of new rows inserted into the dimension table.
    """
    today = date.today().isoformat()

    query = f'SELECT * FROM "{staging_table}" WHERE "{partition_key}" LIKE ?'
    stg_df = pd.read_sql_query(query, conn, params=(f"{partition_value}%",))

    if stg_df.empty:
        return 0

    inserted = 0

    for _, stg_row in stg_df.iterrows():
        key_conditions = " AND ".join(
            [f'"{k}" = ?' for k in natural_key]
        )
        key_values: list[Any] = [stg_row[k] for k in natural_key]

        current_rows = pd.read_sql_query(
            f'SELECT * FROM "{dim_table}" WHERE {key_conditions} AND is_current = 1',
            conn,
            params=key_values,
        )

        if current_rows.empty:
            _insert_dim_row(conn, dim_table, stg_row, today, None, 1)
            inserted += 1
            continue

        current_row = current_rows.iloc[0]

        changed = any(
            str(current_row.get(col, "")) != str(stg_row.get(col, ""))
            for col in tracked_columns
            if col in stg_row.index
        )

        if changed:
            _expire_dim_row(conn, dim_table, natural_key, key_values, today)
            _insert_dim_row(conn, dim_table, stg_row, today, None, 1)
            inserted += 1

    conn.commit()
    return inserted


def _expire_dim_row(
    conn: sqlite3.Connection,
    dim_table: str,
    natural_key: list[str],
    key_values: list[Any],
    today: str,
) -> None:
    key_conditions = " AND ".join([f'"{k}" = ?' for k in natural_key])
    conn.execute(
        f"""
        UPDATE "{dim_table}"
        SET valid_to = ?, is_current = 0
        WHERE {key_conditions} AND is_current = 1
        """,
        [today] + key_values,
    )


def _get_table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """Return the set of column names that exist in a table."""
    cursor = conn.execute(f'PRAGMA table_info("{table}")')
    return {row[1] for row in cursor.fetchall()}


def _insert_dim_row(
    conn: sqlite3.Connection,
    dim_table: str,
    row: pd.Series,
    valid_from: str,
    valid_to: str | None,
    is_current: int,
) -> None:
    dim_cols = _get_table_columns(conn, dim_table)

    data = row.to_dict()
    data["valid_from"] = valid_from
    data["valid_to"] = valid_to
    data["is_current"] = is_current

    scd_cols = {"valid_from", "valid_to", "is_current"}
    # Only include source columns that actually exist in the dim table
    source_cols = [c for c in data if c not in scd_cols and c in dim_cols]
    columns = source_cols + ["valid_from", "valid_to", "is_current"]

    col_list = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(["?"] * len(columns))
    values = [data[c] for c in columns]

    conn.execute(
        f'INSERT INTO "{dim_table}" ({col_list}) VALUES ({placeholders})',
        values,
    )
