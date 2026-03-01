"""
core/ingestion.py

Raw layer: append-only CSV load into raw_<table>.
After a successful load the source file is moved to input/archive/ with a
UTC timestamp suffix: <filename>_<YYYYMMDDTHHMMSSZ>.csv.

Raw tables are immutable once written — this module never updates or deletes.
"""

import os
import shutil
import sqlite3
from datetime import datetime, timezone
from typing import Any

import pandas as pd


def _archive_path(source_path: str, archive_dir: str) -> str:
    """Build the destination path for an archived source file."""
    basename = os.path.basename(source_path)
    name, ext = os.path.splitext(basename)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return os.path.join(archive_dir, f"{name}_{timestamp}{ext}")


def load_raw(
    conn: sqlite3.Connection,
    source_path: str,
    raw_table: str,
    archive_dir: str,
) -> int:
    """
    Load a CSV file into raw_<table> using append-only inserts.

    Returns the number of rows inserted.
    Moves the source file to archive_dir after a successful load.
    Raises FileNotFoundError if source_path does not exist.
    """
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source file not found: {source_path}")

    df = pd.read_csv(source_path, dtype=str)
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df["_loaded_at"] = loaded_at

    rows: list[tuple[Any, ...]] = [tuple(row) for row in df.itertuples(index=False, name=None)]
    columns = list(df.columns)
    placeholders = ", ".join(["?"] * len(columns))
    col_list = ", ".join(f'"{c}"' for c in columns)

    conn.executemany(
        f'INSERT INTO "{raw_table}" ({col_list}) VALUES ({placeholders})',
        rows,
    )
    conn.commit()

    dest = _archive_path(source_path, archive_dir)
    os.makedirs(archive_dir, exist_ok=True)
    shutil.move(source_path, dest)

    return len(rows)
