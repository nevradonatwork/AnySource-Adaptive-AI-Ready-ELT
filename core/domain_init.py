"""
core/domain_init.py

All wizard logic called by setup_domain.py.

Responsibilities:
  - Prompt the user for domain parameters
  - Create the domain directory structure
  - Load or auto-generate data_dictionary.csv
  - Generate config.py and skeleton domain files
  - Create and initialise the SQLite database with log tables

This module is domain-agnostic: it only generates file stubs. The generated
files are templates — the developer fills in the business logic.
"""

import csv
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from core.schema_base import create_log_tables


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_wizard() -> str:
    """
    Interactive wizard that scaffolds a new domain.
    Returns the domain name that was created.
    """
    print("\n=== AnySource ELT — New Domain Setup ===\n")

    domain_name = _prompt_domain_name()
    has_dict = _ask_yes_no("Do you already have a data dictionary? (yes / no): ")

    if has_dict:
        dict_path = input("  → Path to your data dictionary (CSV or Excel): ").strip()
        dd_df = _load_provided_dictionary(dict_path)
        source_columns = _extract_source_columns(dd_df)
        print(f"  ✓ Data dictionary loaded. {len(source_columns)} source columns found.")
    else:
        csv_path = input("  → Path to your source CSV file: ").strip()
        dd_df, source_columns = _generate_dictionary_from_csv(csv_path, domain_name)
        print(
            f"  ✓ Data dictionary auto-generated from CSV. "
            f"{len(source_columns)} source columns detected.\n"
            f"    Staging, dimension, and fact rows added as empty templates for you to fill in."
        )

    partition_key = input(
        "\n> Partition key column (the column that drives your snapshot, e.g. order_date): "
    ).strip()
    granularity = _prompt_granularity()

    _create_domain(
        domain_name=domain_name,
        dd_df=dd_df,
        source_columns=source_columns,
        partition_key=partition_key,
        granularity=granularity,
    )

    _print_summary(domain_name)
    return domain_name


# ---------------------------------------------------------------------------
# Directory & file creation
# ---------------------------------------------------------------------------

def _create_domain(
    domain_name: str,
    dd_df: pd.DataFrame,
    source_columns: list[dict],
    partition_key: str,
    granularity: str,
) -> None:
    base = Path("domains") / domain_name
    db_dir = base / "db"
    input_dir = base / "input"
    archive_dir = input_dir / "archive"

    for directory in [base, db_dir, input_dir, archive_dir]:
        directory.mkdir(parents=True, exist_ok=True)

    dd_df = _ensure_dd_columns(dd_df)
    dd_df.to_csv(base / "data_dictionary.csv", index=False)

    _write_config(base, domain_name, partition_key, granularity)
    _write_schema_skeleton(base, domain_name, source_columns, partition_key)
    _write_validation_skeleton(base, source_columns)
    _write_transformations_skeleton(base)
    _write_facts_config_skeleton(base)
    _write_reporting_views_skeleton(base, domain_name)

    db_path = db_dir / f"{domain_name}.db"
    conn = sqlite3.connect(db_path)
    create_log_tables(conn)
    conn.close()


# ---------------------------------------------------------------------------
# Data dictionary helpers
# ---------------------------------------------------------------------------

_REQUIRED_DD_COLUMNS = [
    "layer", "table_name", "column_name", "data_type",
    "nullable", "description", "example_value", "notes",
]


def _ensure_dd_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in _REQUIRED_DD_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[_REQUIRED_DD_COLUMNS]


def _load_provided_dictionary(path: str) -> pd.DataFrame:
    if path.endswith((".xlsx", ".xls")):
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)
    df = df.fillna("")
    return _ensure_dd_columns(df)


def _extract_source_columns(df: pd.DataFrame) -> list[dict]:
    source_rows = df[df["layer"].str.lower() == "source"]
    return source_rows.to_dict("records")


def _generate_dictionary_from_csv(csv_path: str, domain_name: str) -> tuple[pd.DataFrame, list[dict]]:
    df = pd.read_csv(csv_path, dtype=str)
    table_name = _infer_table_name(csv_path)

    rows = []
    source_columns = []

    for col in df.columns:
        sample = df[col].dropna()
        data_type = _infer_type(sample)
        nullable = "NO" if df[col].isna().sum() == 0 and (df[col] == "").sum() == 0 else "YES"
        example = sample.iloc[0] if not sample.empty else ""

        record = {
            "layer": "source",
            "table_name": table_name,
            "column_name": col,
            "data_type": data_type,
            "nullable": nullable,
            "description": "",
            "example_value": example,
            "notes": "Auto-detected. Please review.",
        }
        rows.append(record)
        source_columns.append(record)

    rows.append({
        "layer": "staging", "table_name": f"stg_{table_name}",
        "column_name": "", "data_type": "", "nullable": "",
        "description": "", "example_value": "",
        "notes": "Fill in your staging columns here.",
    })
    rows.append({
        "layer": "dimensions", "table_name": "dim_",
        "column_name": "", "data_type": "", "nullable": "",
        "description": "", "example_value": "",
        "notes": "Fill in your dimension columns here.",
    })
    rows.append({
        "layer": "facts", "table_name": "fct_",
        "column_name": "", "data_type": "", "nullable": "",
        "description": "", "example_value": "",
        "notes": "Fill in your fact columns here.",
    })

    dd_df = pd.DataFrame(rows, columns=_REQUIRED_DD_COLUMNS)
    return dd_df, source_columns


def _infer_table_name(csv_path: str) -> str:
    name = Path(csv_path).stem
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_").lower()
    return name


def _infer_type(sample: pd.Series) -> str:
    if sample.empty:
        return "TEXT"
    try:
        sample.astype(int)
        return "INTEGER"
    except (ValueError, TypeError):
        pass
    try:
        sample.astype(float)
        return "FLOAT"
    except (ValueError, TypeError):
        pass
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    if sample.head(20).apply(lambda v: bool(date_pattern.match(str(v)))).all():
        return "DATE"
    return "TEXT"


# ---------------------------------------------------------------------------
# File generators
# ---------------------------------------------------------------------------

def _write_config(base: Path, domain_name: str, partition_key: str, granularity: str) -> None:
    content = f'''\
"""
Auto-generated by setup_domain.py. Review before running the pipeline.
"""

DOMAIN_NAME           = "{domain_name}"
DATABASE_NAME         = "{domain_name}.db"
DATABASE_PATH         = "domains/{domain_name}/db/{domain_name}.db"
INPUT_PATH            = "domains/{domain_name}/input"
ARCHIVE_PATH          = "domains/{domain_name}/input/archive"
PARTITION_KEY         = "{partition_key}"
PARTITION_GRANULARITY = "{granularity}"   # monthly | weekly | daily
'''
    (base / "config.py").write_text(content)


def _write_schema_skeleton(
    base: Path,
    domain_name: str,
    source_columns: list[dict],
    partition_key: str,
) -> None:
    table_name = domain_name
    raw_cols = "\n".join(
        f'    "{r["column_name"]}"  TEXT,'
        for r in source_columns
        if r.get("column_name")
    )

    content = f'''\
"""
domains/{domain_name}/schema.py

All CREATE TABLE IF NOT EXISTS statements for this domain.
Fill in staging, dimension, and fact table definitions.
"""

import sqlite3

# Table names — imported by pipeline.py
RAW_TABLE     = "raw_{table_name}"
STAGING_TABLE = "stg_{table_name}"

# SCD2 dimension configurations.
# Each entry: {{ "table": "dim_X", "natural_key": [...], "tracked_columns": [...] }}
DIMENSION_CONFIGS: list[dict] = []

# Columns to sum-check during reconciliation (numeric columns only).
RECONCILIATION_SUM_COLUMNS: list[str] = []


def create_tables(conn: sqlite3.Connection) -> None:
    """Create all tables for this domain. Called by the pipeline before loading."""
    cursor = conn.cursor()

    # Raw table — matches source CSV columns exactly, plus _loaded_at
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_{table_name} (
{raw_cols}
            _loaded_at  TEXT
        )
    """)

    # Staging table — fill in cleaned columns
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stg_{table_name} (
            -- TODO: define your staging columns here
            "{partition_key}"  TEXT
        )
    """)

    # Dimension tables — add one block per dimension
    # Example:
    # cursor.execute("""
    #     CREATE TABLE IF NOT EXISTS dim_customer (
    #         customer_id  TEXT NOT NULL,
    #         ...
    #         valid_from   DATE NOT NULL,
    #         valid_to     DATE,
    #         is_current   INTEGER NOT NULL DEFAULT 1
    #     )
    # """)

    # Fact tables — add one block per fact table
    # Example:
    # cursor.execute("""
    #     CREATE TABLE IF NOT EXISTS fct_sales (
    #         {partition_key}  TEXT NOT NULL,
    #         ...
    #     )
    # """)

    conn.commit()
'''
    (base / "schema.py").write_text(
        content.replace("{raw_cols}", raw_cols)
               .replace("{partition_key}", partition_key)
               .replace("{table_name}", table_name)
               .replace("{domain_name}", domain_name)
    )


def _write_validation_skeleton(base: Path, source_columns: list[dict]) -> None:
    required_cols = [
        r["column_name"] for r in source_columns
        if r.get("column_name") and r.get("nullable", "YES").upper() == "NO"
    ]
    col_list = repr(required_cols)

    content = f'''\
"""
domains/<domain>/validation.py

Two types of checks:
  1. Required columns  — raise ValueError if a mandatory column is missing.
  2. Business rules    — log warnings for rule violations (do not raise).
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS: list[str] = {col_list}


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the DataFrame.
    - Raises ValueError for missing required columns.
    - Logs warnings for business rule violations.
    Returns the DataFrame unchanged (business rule violations are non-blocking).
    """
    _check_required_columns(df)
    _apply_business_rules(df)
    return df


def _check_required_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {{missing}}")


def _apply_business_rules(df: pd.DataFrame) -> None:
    """Add your domain business rule checks here. Use logger.warning() for violations."""
    pass  # TODO: implement business rules
'''
    (base / "validation.py").write_text(content)


def _write_transformations_skeleton(base: Path) -> None:
    content = '''\
"""
domains/<domain>/transformations.py

Pure transformation functions — no DB calls, no file I/O, no global state.
Each function takes a DataFrame and returns a transformed DataFrame.
All functions must be unit-testable without mocking.
"""

import pandas as pd


def apply(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all transformations to the staging DataFrame.
    Called by core/staging.py before writing to stg_*.
    """
    # TODO: add your transformations here, e.g.:
    # df = _strip_whitespace(df)
    # df = _normalise_dates(df)
    return df


# ---------------------------------------------------------------------------
# Individual transformation functions (add yours below)
# ---------------------------------------------------------------------------

def _strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading/trailing whitespace from all string columns."""
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())
    return df
'''
    (base / "transformations.py").write_text(content)


def _write_facts_config_skeleton(base: Path) -> None:
    content = '''\
"""
domains/<domain>/facts_config.py

Aggregation definitions consumed by core/facts.py.

Each entry in FACTS_CONFIG defines one fact table load:
  - target_table     : fct_* table name
  - source_table     : staging or dimension table to aggregate from
  - group_by         : list of columns to group by
  - aggregations     : dict of output_column -> (source_column, function)
                       Supported: SUM, COUNT, AVG, MIN, MAX
  - partition_column : column used to scope the partition delete+insert
"""

FACTS_CONFIG: list[dict] = [
    # TODO: define your fact table aggregations here.
    # Example:
    # {
    #     "target_table"    : "fct_sales",
    #     "source_table"    : "stg_orders",
    #     "group_by"        : ["order_date", "region"],
    #     "aggregations"    : {
    #         "total_amount"  : ("amount", "SUM"),
    #         "order_count"   : ("order_id", "COUNT"),
    #     },
    #     "partition_column": "order_date",
    # },
]
'''
    (base / "facts_config.py").write_text(content)


def _write_reporting_views_skeleton(base: Path, domain_name: str) -> None:
    content = f'''\
-- domains/{domain_name}/reporting_views.sql
--
-- CREATE VIEW IF NOT EXISTS vw_rep_* statements.
-- Executed by the pipeline after facts are loaded.
-- Only join dim_* and fct_* tables here — never raw_* or stg_*.

-- TODO: add your reporting views below.
-- Example:
-- CREATE VIEW IF NOT EXISTS vw_rep_monthly_sales AS
-- SELECT
--     f.order_date,
--     d.region_name,
--     f.total_amount,
--     f.order_count
-- FROM fct_sales f
-- JOIN dim_region d ON f.region_id = d.region_id AND d.is_current = 1;
'''
    (base / "reporting_views.sql").write_text(content)


# ---------------------------------------------------------------------------
# Input helpers
# ---------------------------------------------------------------------------

def _prompt_domain_name() -> str:
    while True:
        name = input("> Project name: ").strip().lower()
        name = re.sub(r"[^a-z0-9_]", "_", name)
        name = re.sub(r"_+", "_", name).strip("_")
        if name:
            return name
        print("  Domain name cannot be empty. Please try again.")


def _ask_yes_no(prompt: str) -> bool:
    while True:
        answer = input(prompt).strip().lower()
        if answer in ("yes", "y"):
            return True
        if answer in ("no", "n"):
            return False
        print("  Please answer 'yes' or 'no'.")


def _prompt_granularity() -> str:
    while True:
        value = input("> Partition granularity (monthly / weekly / daily): ").strip().lower()
        if value in ("monthly", "weekly", "daily"):
            return value
        print("  Please enter 'monthly', 'weekly', or 'daily'.")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def _print_summary(domain_name: str) -> None:
    base = f"domains/{domain_name}"
    print(f"""
Created:
  {base}/
  {base}/input/
  {base}/input/archive/
  {base}/db/
  {base}/data_dictionary.csv
  {base}/config.py          ← fully populated, review only
  {base}/schema.py          ← fill in staging / dimension / fact tables
  {base}/validation.py      ← fill in your business rules
  {base}/transformations.py ← fill in your cleaning logic
  {base}/facts_config.py    ← fill in your aggregation definitions
  {base}/reporting_views.sql← fill in your business SQL
  {base}/db/{domain_name}.db← database created with etl_run_log + etl_error_log

Next steps:
  1. Review data_dictionary.csv and fill in staging / dimension / fact rows.
  2. Define CREATE TABLE statements in schema.py.
  3. Add business rules in validation.py.
  4. Add cleaning functions in transformations.py.
  5. Define aggregations in facts_config.py.
  6. Write reporting views in reporting_views.sql.
  7. Drop your source CSV into {base}/input/ and run:
       python main.py --domain {domain_name} --partition <value>
""")
