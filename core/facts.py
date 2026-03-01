"""
core/facts.py

Fact loading driven entirely by domains/<domain>/facts_config.py.

Each entry in facts_config.FACTS_CONFIG defines:
  - target_table     : name of the fct_* table to write to
  - source_table     : staging or dimension table to aggregate from
  - group_by         : list of columns to group by
  - aggregations     : dict mapping output_column -> (source_column, function)
                       supported functions: SUM, COUNT, AVG, MIN, MAX
  - partition_column : column used to scope the partition filter

Existing rows for the same partition are replaced (delete + insert).
"""

import sqlite3
from typing import Any

import pandas as pd


FactsConfigEntry = dict[str, Any]


def load_facts(
    conn: sqlite3.Connection,
    facts_config: list[FactsConfigEntry],
    partition_value: str,
) -> int:
    """
    Process every fact table definition in facts_config for the given partition.
    Returns the total number of rows written across all fact tables.
    """
    total_inserted = 0

    for entry in facts_config:
        target_table = entry["target_table"]
        source_table = entry["source_table"]
        group_by: list[str] = entry["group_by"]
        aggregations: dict[str, tuple[str, str]] = entry["aggregations"]
        partition_column: str = entry["partition_column"]

        query = (
            f'SELECT * FROM "{source_table}" '
            f'WHERE "{partition_column}" LIKE ?'
        )
        df = pd.read_sql_query(query, conn, params=(f"{partition_value}%",))

        if df.empty:
            continue

        agg_spec: dict[str, Any] = {}
        for out_col, (src_col, func) in aggregations.items():
            func_lower = func.lower()
            if func_lower == "sum":
                agg_spec[src_col] = "sum"
            elif func_lower == "count":
                agg_spec[src_col] = "count"
            elif func_lower == "avg":
                agg_spec[src_col] = "mean"
            elif func_lower == "min":
                agg_spec[src_col] = "min"
            elif func_lower == "max":
                agg_spec[src_col] = "max"
            else:
                raise ValueError(f"Unsupported aggregation function: {func}")

        numeric_src_cols = list(agg_spec.keys())
        for col in numeric_src_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        aggregated = df.groupby(group_by, as_index=False).agg(agg_spec)

        rename_map: dict[str, str] = {}
        for out_col, (src_col, _) in aggregations.items():
            if src_col != out_col:
                rename_map[src_col] = out_col
        aggregated = aggregated.rename(columns=rename_map)

        conn.execute(
            f'DELETE FROM "{target_table}" WHERE "{partition_column}" LIKE ?',
            (f"{partition_value}%",),
        )

        columns = list(aggregated.columns)
        col_list = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(["?"] * len(columns))
        rows = [tuple(row) for row in aggregated.itertuples(index=False, name=None)]

        conn.executemany(
            f'INSERT INTO "{target_table}" ({col_list}) VALUES ({placeholders})',
            rows,
        )
        conn.commit()

        total_inserted += len(rows)

    return total_inserted
