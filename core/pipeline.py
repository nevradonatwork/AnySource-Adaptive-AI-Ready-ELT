"""
core/pipeline.py

The orchestrator. Runs every pipeline layer in the fixed, non-negotiable order:

  1.  Validate source file exists
  2.  ingestion  → raw_* (append-only)
  3.  Move source file to archive
  4.  staging    → stg_* (truncate-reload per partition)
  5.  dimensions → dim_* (SCD2 upserts)
  6.  facts      → fct_* (partition-replace)
  7.  reconciliation
  8.  Refresh reporting views
  9.  etl_logging → write etl_run_log row
  10. email_notify → send notification

If any step raises an unhandled exception:
  - the run is marked "failure" in etl_run_log
  - the notification is sent
  - the exception is re-raised so the caller sees a non-zero exit

Domain modules are imported dynamically at runtime using importlib so the
core engine never hard-imports from any domain folder.
"""

import importlib
import importlib.util
import os
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

from core import etl_logging, email_notify
from core.ingestion import load_raw
from core.staging import load_staging
from core.dimensions import upsert_dimension
from core.facts import load_facts
from core.reconciliation import run_reconciliation


def _load_domain_module(domain_name: str, module_file: str) -> ModuleType:
    """Dynamically import a module from a domain folder."""
    module_path = Path("domains") / domain_name / module_file
    spec = importlib.util.spec_from_file_location(
        f"domains.{domain_name}.{module_file.replace('.py', '')}",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load domain module: {module_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


def run_pipeline(domain_name: str, partition_value: str) -> None:
    """
    Execute the full ETL pipeline for a domain and partition.

    Parameters
    ----------
    domain_name     : e.g. "sales_europe"
    partition_value : e.g. "2025-01" (format depends on PARTITION_GRANULARITY)
    """
    run_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)
    status = "success"
    rows_loaded = 0
    reconciliation_failures: list[str] = []
    conn: sqlite3.Connection | None = None

    cfg = _load_domain_module(domain_name, "config.py")
    db_path: str = cfg.DATABASE_PATH
    input_path: str = cfg.INPUT_PATH
    archive_path: str = cfg.ARCHIVE_PATH
    partition_key: str = cfg.PARTITION_KEY

    schema_mod = _load_domain_module(domain_name, "schema.py")
    validation_mod = _load_domain_module(domain_name, "validation.py")
    transforms_mod = _load_domain_module(domain_name, "transformations.py")
    facts_cfg_mod = _load_domain_module(domain_name, "facts_config.py")

    try:
        source_files = [
            f for f in os.listdir(input_path)
            if f.endswith(".csv") and not f.startswith(".")
        ]
        if not source_files:
            raise FileNotFoundError(
                f"No CSV files found in {input_path} for domain '{domain_name}'"
            )
        source_path = os.path.join(input_path, source_files[0])

        conn = sqlite3.connect(db_path)

        schema_mod.create_tables(conn)

        raw_table: str = schema_mod.RAW_TABLE
        staging_table: str = schema_mod.STAGING_TABLE

        rows_loaded = load_raw(
            conn=conn,
            source_path=source_path,
            raw_table=raw_table,
            archive_dir=archive_path,
        )

        rows_loaded = load_staging(
            conn=conn,
            raw_table=raw_table,
            staging_table=staging_table,
            partition_key=partition_key,
            partition_value=partition_value,
            transform_fn=transforms_mod.apply,
            validate_fn=validation_mod.validate,
        )

        dim_configs: list[dict] = getattr(schema_mod, "DIMENSION_CONFIGS", [])
        for dim_cfg in dim_configs:
            upsert_dimension(
                conn=conn,
                staging_table=staging_table,
                dim_table=dim_cfg["table"],
                natural_key=dim_cfg["natural_key"],
                tracked_columns=dim_cfg["tracked_columns"],
                partition_key=partition_key,
                partition_value=partition_value,
            )

        facts_config: list[dict] = facts_cfg_mod.FACTS_CONFIG
        load_facts(conn=conn, facts_config=facts_config, partition_value=partition_value)

        fact_tables = [entry["target_table"] for entry in facts_config]
        sum_columns: list[str] = getattr(schema_mod, "RECONCILIATION_SUM_COLUMNS", [])
        reconciliation_failures = run_reconciliation(
            conn=conn,
            run_id=run_id,
            raw_table=raw_table,
            staging_table=staging_table,
            fact_tables=fact_tables,
            partition_key=partition_key,
            partition_value=partition_value,
            sum_columns=sum_columns,
        )

        views_sql_path = Path("domains") / domain_name / "reporting_views.sql"
        if views_sql_path.exists():
            views_sql = views_sql_path.read_text()
            for statement in views_sql.split(";"):
                statement = statement.strip()
                if statement:
                    conn.execute(statement)
            conn.commit()

    except Exception as exc:
        status = "failure"
        if conn:
            etl_logging.log_error(conn, run_id, "pipeline", str(exc))
        end_time = datetime.now(timezone.utc)
        if conn:
            etl_logging.log_run(
                conn=conn,
                run_id=run_id,
                domain=domain_name,
                partition=partition_value,
                status=status,
                rows_loaded=rows_loaded,
                start_time=start_time,
                end_time=end_time,
                notes=str(exc),
            )
            email_notify.send_notification(
                conn=conn,
                run_id=run_id,
                domain=domain_name,
                partition=partition_value,
                status=status,
                rows_loaded=rows_loaded,
                start_time=start_time,
                end_time=end_time,
                reconciliation_failures=reconciliation_failures,
            )
            conn.close()
        raise

    end_time = datetime.now(timezone.utc)
    recon_note = f"{len(reconciliation_failures)} reconciliation issue(s)" if reconciliation_failures else ""

    etl_logging.log_run(
        conn=conn,
        run_id=run_id,
        domain=domain_name,
        partition=partition_value,
        status=status,
        rows_loaded=rows_loaded,
        start_time=start_time,
        end_time=end_time,
        notes=recon_note,
    )

    email_notify.send_notification(
        conn=conn,
        run_id=run_id,
        domain=domain_name,
        partition=partition_value,
        status=status,
        rows_loaded=rows_loaded,
        start_time=start_time,
        end_time=end_time,
        reconciliation_failures=reconciliation_failures,
    )

    conn.close()

    if reconciliation_failures:
        print(f"Pipeline completed with {len(reconciliation_failures)} reconciliation warning(s):")
        for f in reconciliation_failures:
            print(f"  {f}")
    else:
        print(f"Pipeline completed successfully. {rows_loaded} rows loaded for {domain_name}/{partition_value}.")
