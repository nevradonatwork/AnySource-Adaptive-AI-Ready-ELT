"""
core/schema_base.py

Creates the two shared log tables that must exist in every domain database.
Called once by the wizard at domain creation time.
These schemas are fixed — do not add domain-specific columns.
"""

import sqlite3


def create_log_tables(conn: sqlite3.Connection) -> None:
    """Create etl_run_log and etl_error_log tables if they do not exist."""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS etl_run_log (
            run_id        TEXT      NOT NULL,
            domain        TEXT      NOT NULL,
            partition     TEXT      NOT NULL,
            status        TEXT      NOT NULL CHECK (status IN ('success', 'failure')),
            rows_loaded   INTEGER,
            start_time    TIMESTAMP NOT NULL,
            end_time      TIMESTAMP,
            notes         TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS etl_error_log (
            run_id        TEXT      NOT NULL,
            layer         TEXT      NOT NULL,
            error_message TEXT      NOT NULL,
            timestamp     TIMESTAMP NOT NULL
        )
    """)

    conn.commit()
