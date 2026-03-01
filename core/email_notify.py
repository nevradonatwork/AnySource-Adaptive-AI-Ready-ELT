"""
core/email_notify.py

Sends a run-status summary email at the end of each pipeline run.
All SMTP credentials come from environment variables — never hardcoded.

If the email fails, the error is logged but the pipeline exit code is
driven by the ETL result, not the notification result.

Required environment variables:
    ETL_SMTP_HOST
    ETL_SMTP_PORT
    ETL_SMTP_USER
    ETL_SMTP_PASSWORD
    ETL_NOTIFY_FROM
    ETL_NOTIFY_TO
"""

import os
import smtplib
import sqlite3
from datetime import datetime
from email.mime.text import MIMEText
from typing import Any

from core.etl_logging import log_error


def send_notification(
    conn: sqlite3.Connection,
    run_id: str,
    domain: str,
    partition: str,
    status: str,
    rows_loaded: int,
    start_time: datetime,
    end_time: datetime,
    reconciliation_failures: list[str],
) -> None:
    """
    Send a pipeline run summary email.

    Silently swallows SMTP failures after logging them to etl_error_log.
    """
    smtp_host = os.environ.get("ETL_SMTP_HOST", "")
    smtp_port_str = os.environ.get("ETL_SMTP_PORT", "587")
    smtp_user = os.environ.get("ETL_SMTP_USER", "")
    smtp_password = os.environ.get("ETL_SMTP_PASSWORD", "")
    notify_from = os.environ.get("ETL_NOTIFY_FROM", "")
    notify_to = os.environ.get("ETL_NOTIFY_TO", "")

    if not all([smtp_host, smtp_user, smtp_password, notify_from, notify_to]):
        log_error(
            conn,
            run_id,
            "email_notify",
            "Email notification skipped: one or more SMTP environment variables are not set.",
        )
        return

    duration_seconds = (end_time - start_time).total_seconds()
    status_emoji = "SUCCESS" if status == "success" else "FAILURE"

    recon_section = ""
    if reconciliation_failures:
        recon_lines = "\n".join(f"  - {f}" for f in reconciliation_failures)
        recon_section = f"\nReconciliation issues:\n{recon_lines}\n"

    body = (
        f"ETL Run Summary — {status_emoji}\n"
        f"{'=' * 40}\n"
        f"Run ID    : {run_id}\n"
        f"Domain    : {domain}\n"
        f"Partition : {partition}\n"
        f"Status    : {status}\n"
        f"Rows      : {rows_loaded}\n"
        f"Started   : {start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
        f"Finished  : {end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
        f"Duration  : {duration_seconds:.1f}s\n"
        f"{recon_section}"
    )

    msg = MIMEText(body)
    msg["Subject"] = f"[ETL] {status_emoji} — {domain} / {partition}"
    msg["From"] = notify_from
    msg["To"] = notify_to

    try:
        smtp_port = int(smtp_port_str)
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(notify_from, [notify_to], msg.as_string())
    except Exception as exc:
        log_error(conn, run_id, "email_notify", f"Failed to send email: {exc}")
