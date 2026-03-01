"""
core/staging.py

Staging layer: reads raw_<table>, applies domain transformations and
validation, deduplicates, and writes to stg_<table>.

Staging is truncate-and-reload per partition — not append-only.
"""

import sqlite3
from typing import Callable

import pandas as pd


def load_staging(
    conn: sqlite3.Connection,
    raw_table: str,
    staging_table: str,
    partition_key: str,
    partition_value: str,
    transform_fn: Callable[[pd.DataFrame], pd.DataFrame],
    validate_fn: Callable[[pd.DataFrame], pd.DataFrame],
) -> int:
    """
    Read the raw table for the given partition, apply transformations and
    validation, deduplicate, then truncate-and-reload the staging table.

    Parameters
    ----------
    conn            : open SQLite connection
    raw_table       : name of the source raw table (e.g. "raw_orders")
    staging_table   : name of the target staging table (e.g. "stg_orders")
    partition_key   : column used to filter by partition (e.g. "order_date")
    partition_value : the partition to process (e.g. "2025-01")
    transform_fn    : domain transformations/py apply() — receives a DataFrame,
                      returns a transformed DataFrame
    validate_fn     : domain validation.py validate() — receives a DataFrame,
                      returns the same DataFrame (logs warnings internally)

    Returns the number of rows written to the staging table.
    """
    query = f'SELECT * FROM "{raw_table}" WHERE "{partition_key}" LIKE ?'
    df = pd.read_sql_query(query, conn, params=(f"{partition_value}%",))

    if df.empty:
        return 0

    df = df.drop(columns=["_loaded_at"], errors="ignore")

    df = transform_fn(df)
    df = validate_fn(df)

    df = df.drop_duplicates()

    cursor = conn.cursor()
    cursor.execute(
        f'DELETE FROM "{staging_table}" WHERE "{partition_key}" LIKE ?',
        (f"{partition_value}%",),
    )

    columns = list(df.columns)
    placeholders = ", ".join(["?"] * len(columns))
    col_list = ", ".join(f'"{c}"' for c in columns)

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cursor.executemany(
        f'INSERT INTO "{staging_table}" ({col_list}) VALUES ({placeholders})',
        rows,
    )
    conn.commit()

    return len(rows)
