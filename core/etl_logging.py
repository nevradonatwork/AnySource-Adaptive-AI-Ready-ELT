"""
core/etl_logging.py

Writes run summaries to etl_run_log and individual errors to etl_error_log.
Every pipeline run produces exactly one etl_run_log row.
"""

import sqlite3
from datetime import datetime, timezone


def log_run(
    conn: sqlite3.Connection,
    run_id: str,
    domain: str,
    partition: str,
    status: str,
    rows_loaded: int,
    start_time: datetime,
    end_time: datetime,
    notes: str = "",
) -> None:
    """Insert one row into etl_run_log for the completed pipeline run."""
    conn.execute(
        """
        INSERT INTO etl_run_log
            (run_id, domain, partition, status, rows_loaded, start_time, end_time, notes)
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            domain,
            partition,
            status,
            rows_loaded,
            start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            notes,
        ),
    )
    conn.commit()


def log_error(
    conn: sqlite3.Connection,
    run_id: str,
    layer: str,
    error_message: str,
) -> None:
    """Insert one row into etl_error_log for a single error event."""
    conn.execute(
        """
        INSERT INTO etl_error_log (run_id, layer, error_message, timestamp)
        VALUES (?, ?, ?, ?)
        """,
        (
            run_id,
            layer,
            error_message,
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        ),
    )
    conn.commit()
