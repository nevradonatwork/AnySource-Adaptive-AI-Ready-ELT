# CLAUDE.md — AnySource Adaptive AI-Ready ELT

This file is the canonical reference for AI assistants (Claude, Copilot, etc.) working in this repository.
Read this before touching any code.

---

## What this project is

A **modular, domain-agnostic ELT framework** built on SQLite. It accepts CSV files, processes them through four layered stages (raw → staging → dimensions → facts), and produces reconciled, versioned analytical tables and reporting views. Every domain runs in total isolation: its own database, its own config, its own business logic.

The framework has two entry points:

| Script | Purpose |
|---|---|
| `setup_domain.py` | Wizard that scaffolds a new domain from a data dictionary or a raw CSV |
| `main.py` | Runs the full ETL pipeline for an existing domain |

---

## Repository layout

```
AnySource-Adaptive-AI-Ready-ELT/
│
├── core/                          # The engine. Generic. Never domain-specific.
│   ├── schema_base.py             # Shared log tables: etl_run_log, etl_error_log
│   ├── ingestion.py               # Raw layer: append-only CSV load into raw_<table>
│   ├── staging.py                 # Staging: clean, deduplicate, validate
│   ├── dimensions.py              # Dimension upserts with SCD Type 2
│   ├── facts.py                   # Fact loading driven by domain facts_config.py
│   ├── reconciliation.py          # Cross-layer row/sum comparisons
│   ├── etl_logging.py             # Writes to etl_run_log and etl_error_log
│   ├── email_notify.py            # Sends run-status email notifications
│   ├── pipeline.py                # Orchestrator: calls every layer in order
│   └── domain_init.py             # Setup wizard logic (called by setup_domain.py)
│
├── domains/                       # One sub-folder per domain. Never touch another domain's folder.
│   └── <domain_name>/
│       ├── input/                 # Drop source CSV files here before running
│       │   └── archive/           # Pipeline moves processed files here automatically
│       ├── db/
│       │   └── <domain_name>.db   # SQLite database, created by the wizard
│       ├── config.py              # Paths, partition key, granularity — fully generated
│       ├── schema.py              # All CREATE TABLE statements for this domain
│       ├── validation.py          # Column presence checks and business rules
│       ├── transformations.py     # Cleaning and normalisation functions
│       ├── facts_config.py        # Aggregation definitions (what, how, on which columns)
│       ├── reporting_views.sql    # vw_rep_* views for downstream consumption
│       └── data_dictionary.csv    # Ground truth for column metadata across all layers
│
├── tests/
│   ├── core/                      # Unit and integration tests for core engine modules
│   └── domains/
│       └── <domain_name>/         # Domain-specific tests (schema, validation, transforms)
│
├── setup_domain.py                # Entry point: scaffold a new domain
├── main.py                        # Entry point: run the ETL pipeline
└── CLAUDE.md                      # This file
```

---

## Core engine files — responsibilities and rules

### `core/schema_base.py`
- Creates two tables that exist in **every** domain database: `etl_run_log` and `etl_error_log`.
- Called once by the wizard at domain creation time.
- **Do not** add domain-specific columns here; these tables are universal.

### `core/ingestion.py`
- Loads the source CSV into `raw_<table>` using **append-only** inserts.
- Never updates or deletes from raw tables. Raw is immutable once written.
- After a successful load, the source file is moved to `input/archive/` with a UTC timestamp suffix: `<filename>_<YYYYMMDDTHHMMSSZ>.csv`.

### `core/staging.py`
- Reads from `raw_<table>`, applies transformations from `domains/<domain>/transformations.py`, validates using `domains/<domain>/validation.py`, deduplicates, and writes to `stg_<table>`.
- Staging tables are **truncate-and-reload** per partition, not append-only.

### `core/dimensions.py`
- Upserts into `dim_*` tables.
- Supports SCD Type 2: when a tracked attribute changes, the existing row is expired (`valid_to` set to today) and a new row is inserted (`valid_from` = today, `valid_to` = NULL, `is_current` = 1).
- SCD2 configuration lives in `domains/<domain>/schema.py`.

### `core/facts.py`
- Driven entirely by `domains/<domain>/facts_config.py`.
- Reads from staging or dimension tables, applies the configured aggregations, and writes to `fct_*` tables.
- Fact tables are partition-keyed: existing rows for the same partition are replaced.

### `core/reconciliation.py`
- Compares row counts and key numeric sums across raw → staging → facts.
- Failures are logged to `etl_error_log` and surface in the email notification.
- Does **not** raise exceptions that stop the pipeline; it records and continues.

### `core/etl_logging.py`
- Every pipeline run gets one row in `etl_run_log` with: `run_id`, `domain`, `partition`, `status` (`success` / `failure`), `rows_loaded`, `start_time`, `end_time`, `notes`.
- Errors get rows in `etl_error_log` with: `run_id`, `layer`, `error_message`, `timestamp`.

### `core/email_notify.py`
- Sends a summary email at the end of each run.
- SMTP configuration comes from environment variables (never hardcoded).
- If email fails, the error is logged but the pipeline exit code is still driven by the ETL result.

### `core/pipeline.py`
- The orchestrator. Calls each layer in this fixed order:
  1. Validate source file exists
  2. `ingestion.py` → raw layer
  3. Move source file to archive
  4. `staging.py` → staging layer
  5. `dimensions.py` → dimension tables
  6. `facts.py` → fact tables
  7. `reconciliation.py`
  8. Refresh reporting views (execute SQL in `reporting_views.sql`)
  9. `etl_logging.py` → write run log
  10. `email_notify.py` → send notification
- If any step raises an unhandled exception, the run is marked `failure` in the log and the notification is sent before re-raising.

### `core/domain_init.py`
- Contains all wizard logic called by `setup_domain.py`.
- Creates the directory structure, generates all skeleton files, initialises the SQLite database.
- See "Creating a new domain" section below for the full flow.

---

## Domain files — responsibilities and rules

Each domain folder under `domains/` is self-contained. The core engine imports from these files at runtime.

### `config.py`
Fully generated by the wizard. Defines these constants — import them by name everywhere:

```python
DOMAIN_NAME           = "sales_europe"
DATABASE_NAME         = "sales_europe.db"
DATABASE_PATH         = "domains/sales_europe/db/sales_europe.db"
INPUT_PATH            = "domains/sales_europe/input"
ARCHIVE_PATH          = "domains/sales_europe/input/archive"
PARTITION_KEY         = "order_date"
PARTITION_GRANULARITY = "monthly"   # monthly | weekly | daily
```

**Never hardcode paths anywhere else.** Always import from `config.py`.

### `schema.py`
All `CREATE TABLE IF NOT EXISTS` statements for this domain:
- `raw_*` tables (matches source CSV columns exactly, plus `_loaded_at TIMESTAMP`)
- `stg_*` tables (cleaned columns, no `_loaded_at`)
- `dim_*` tables (with SCD2 columns: `valid_from`, `valid_to`, `is_current`)
- `fct_*` tables (aggregated measures, partition key column)

### `validation.py`
Two types of checks:
1. **Required columns** — raise `ValueError` if a mandatory column is missing from the source CSV.
2. **Business rules** — log warnings (not exceptions) for rule violations such as negative amounts, unknown region codes, etc.

### `transformations.py`
Pure functions that take a DataFrame row or Series and return cleaned values. No side effects. No database calls. Testable in isolation.

### `facts_config.py`
A list of fact table definitions. Each entry specifies:
- Target table name
- Source table (staging or dimension)
- Group-by columns
- Aggregation columns and functions (SUM, COUNT, AVG, etc.)
- Partition filter column

### `reporting_views.sql`
SQL `CREATE VIEW IF NOT EXISTS vw_rep_*` statements. Executed by the pipeline after facts are loaded. These are the only tables/views that downstream consumers (dashboards, reports) should query.

### `data_dictionary.csv`
The ground truth for all column metadata. Fixed schema:

```
layer, table_name, column_name, data_type, nullable, description, example_value, notes
```

Layers are: `source`, `staging`, `dimensions`, `facts`.

---

## Creating a new domain

Run the wizard:

```bash
python setup_domain.py
```

The wizard asks these questions in sequence:

```
> Project name: sales_europe

> Do you already have a data dictionary? (yes / no): yes
  → Path to your data dictionary (CSV or Excel): docs/sales_data_dictionary.xlsx
  ✓ Data dictionary loaded. 42 columns found across 4 layers.

  -- or if no --

> Do you already have a data dictionary? (yes / no): no
  → Path to your source CSV file: domains/sales_europe/input/orders_jan_2025.csv
  ✓ Data dictionary auto-generated from CSV. 8 source columns detected.
    Staging, dimension, and fact rows added as empty templates for you to fill in.

> Partition key column (the column that drives your snapshot, e.g. order_date): order_date
> Partition granularity (monthly / weekly / daily): monthly
```

**What the wizard creates automatically:**

1. `domains/sales_europe/`
2. `domains/sales_europe/input/`
3. `domains/sales_europe/input/archive/`
4. `domains/sales_europe/db/`
5. `domains/sales_europe/data_dictionary.csv` (loaded or auto-generated)
6. `domains/sales_europe/config.py` — fully populated, no manual edits needed
7. Skeleton files for `schema.py`, `validation.py`, `transformations.py`, `facts_config.py`, `reporting_views.sql`
8. `domains/sales_europe/db/sales_europe.db` — database created immediately
9. `etl_run_log` and `etl_error_log` tables inside the database
10. Summary printed to stdout listing what was created and what needs to be filled in

**What you fill in after the wizard:**

| File | What to fill in |
|---|---|
| `config.py` | Review only — normally nothing to change |
| `data_dictionary.csv` | Descriptions, notes, and template rows for staging/dimensions/facts |
| `schema.py` | Staging, dimension, and fact table definitions |
| `validation.py` | Business rules |
| `transformations.py` | Cleaning and normalisation logic |
| `facts_config.py` | Aggregation definitions |
| `reporting_views.sql` | Business SQL for reporting views |

---

## Auto-generated data dictionary format

When the user answers **no** to having a data dictionary, the wizard reads the source CSV and produces:

```csv
layer,table_name,column_name,data_type,nullable,description,example_value,notes
source,orders,order_id,INTEGER,NO,,1001,Auto-detected. Please review.
source,orders,customer_id,INTEGER,NO,,4421,Auto-detected. Please review.
source,orders,order_date,DATE,NO,,2025-01-15,Auto-detected. Please review.
source,orders,amount,FLOAT,YES,,149.99,Auto-detected. Please review.
source,orders,region,TEXT,YES,,Europe,Auto-detected. Please review.
staging,stg_orders,,,,,, Fill in your staging columns here.
dimensions,dim_,,,,,,Fill in your dimension columns here.
facts,fct_,,,,,,Fill in your fact columns here.
```

Rules for auto-detection:
- Column names come directly from the CSV header row.
- `data_type` is inferred: all-integer values → `INTEGER`, numeric with decimals → `FLOAT`, ISO date pattern → `DATE`, everything else → `TEXT`.
- `nullable` is `NO` if zero nulls/blanks in the sample, otherwise `YES`.
- `example_value` is the first non-null value in the column.
- If any required column is missing from a provided data dictionary, the wizard adds it as an empty column so the format is always consistent.

---

## Running the pipeline

```bash
python main.py --domain sales_europe --partition 2025-01
```

`--partition` format depends on `PARTITION_GRANULARITY`:
- `monthly` → `YYYY-MM` (e.g. `2025-01`)
- `weekly` → `YYYY-Www` (e.g. `2025-W04`)
- `daily` → `YYYY-MM-DD` (e.g. `2025-01-15`)

**Pipeline execution order** (fixed, non-negotiable):

1. Read `domains/<domain>/config.py`
2. Validate source file exists in `input/`
3. Load into `raw_*` table (append-only)
4. Move source file to `input/archive/` with UTC timestamp suffix
5. Clean and load into `stg_*` table
6. Upsert dimension tables (SCD2 where configured)
7. Load fact tables (driven by `facts_config.py`)
8. Run reconciliation
9. Refresh reporting views
10. Log to `etl_run_log`
11. Send email notification

---

## Tests

Tests live in `tests/`, mirroring the source structure.

```
tests/
├── core/          # Tests for core engine modules (ingestion, staging, dimensions, etc.)
└── domains/
    └── <domain>/  # Domain-specific tests
```

**Conventions:**
- Use `pytest`.
- Core tests use in-memory SQLite (`:memory:`) — never touch a real domain database.
- Domain tests use a temporary copy of the domain database, never the live one.
- Test files are named `test_<module>.py`.
- Each test function tests one behaviour. No multi-assertion mega-tests.
- Transformation functions in `transformations.py` must be fully unit-testable with no mocking required (they are pure functions).

Run all tests:

```bash
pytest tests/
```

Run tests for a specific module:

```bash
pytest tests/core/test_ingestion.py
pytest tests/domains/sales_europe/
```

---

## Key conventions and rules for AI assistants

### Never do these things

- **Do not hardcode any file path.** Always import from `domains/<domain>/config.py`.
- **Do not write to a raw table after initial load.** Raw is append-only and immutable per run.
- **Do not put domain-specific logic in `core/`.** Core must remain generic.
- **Do not put reusable engine logic in a domain folder.** Domain folders contain only configuration and business rules.
- **Do not query `raw_*` or `stg_*` tables from reporting views.** Reporting views (`vw_rep_*`) must only join `dim_*` and `fct_*` tables.
- **Do not add columns to `etl_run_log` or `etl_error_log`.** These schemas are fixed and shared across all domains.
- **Do not use pandas for database operations.** Use the `sqlite3` standard library for all DB interactions. Pandas is allowed only for CSV reading and transformations.
- **Do not modify another domain's folder** when working on a specific domain.

### Always do these things

- **Import config constants by name**, not by re-deriving paths.
- **Log every error** to `etl_error_log` before re-raising or suppressing.
- **Archive the source file** immediately after a successful raw load, before staging begins.
- **Use UTC timestamps** everywhere (in archive filenames, log tables, SCD2 dates).
- **Make transformation functions pure** — no DB calls, no file I/O, no global state.
- **Write a test** for every new transformation function, validation rule, or core behaviour.
- **Follow the fixed pipeline order** in `pipeline.py`. Do not reorder steps.

### Naming conventions

| Object | Pattern | Example |
|---|---|---|
| Raw table | `raw_<source_name>` | `raw_orders` |
| Staging table | `stg_<source_name>` | `stg_orders` |
| Dimension table | `dim_<entity>` | `dim_customer` |
| Fact table | `fct_<metric_set>` | `fct_sales` |
| Reporting view | `vw_rep_<topic>` | `vw_rep_monthly_sales` |
| Archive file | `<original>_<YYYYMMDDTHHMMSSZ>.csv` | `orders_jan_2025_20260301T060000Z.csv` |
| Domain folder | snake_case | `sales_europe` |
| Config constant | SCREAMING_SNAKE_CASE | `PARTITION_KEY` |

### SCD Type 2 column conventions

Every `dim_*` table that uses SCD2 must have these columns:

```sql
valid_from   DATE     NOT NULL,
valid_to     DATE,            -- NULL means currently active
is_current   INTEGER  NOT NULL DEFAULT 1  -- 1 = active, 0 = expired
```

### Partition key handling

The `PARTITION_KEY` column in the domain's staging and fact tables is the filter used to scope each pipeline run. When loading facts, always filter staging data to the current partition before aggregating.

---

## Environment variables

SMTP credentials for email notifications must come from environment variables. Never commit credentials.

```
ETL_SMTP_HOST
ETL_SMTP_PORT
ETL_SMTP_USER
ETL_SMTP_PASSWORD
ETL_NOTIFY_FROM
ETL_NOTIFY_TO
```

---

## Adding a new core module

If genuinely new cross-domain functionality is needed in `core/`:

1. Create `core/<module_name>.py`.
2. Keep it entirely domain-agnostic — accept domain config as parameters, never import from `domains/`.
3. Add tests in `tests/core/test_<module_name>.py` using in-memory SQLite.
4. Update `core/pipeline.py` only if the new module needs to run as a pipeline step.
5. Document the module's responsibility in this file under the "Core engine files" section.

---

## Adding a new domain

Always use the wizard (`python setup_domain.py`). Do not create domain folders manually. The wizard guarantees:
- Consistent directory structure across all domains.
- `etl_run_log` and `etl_error_log` tables are always present.
- `config.py` always has the required constants.
- `data_dictionary.csv` is always in the canonical format.

---

## Data flow summary

```
Source CSV  →  input/<file>.csv
                   │
                   ▼
             raw_<table>           (append-only, immutable)
                   │
                   ▼
             input/archive/<file>_<timestamp>.csv
                   │
                   ▼
             stg_<table>           (truncate-reload per partition)
                   │
          ┌────────┴────────┐
          ▼                 ▼
      dim_<entity>      fct_<metric>   (upsert / partition-replace)
          │                 │
          └────────┬────────┘
                   ▼
           vw_rep_<topic>            (read-only, for downstream consumers)
```

---

## Quick reference

```bash
# Scaffold a new domain
python setup_domain.py

# Run the full ETL pipeline
python main.py --domain <domain_name> --partition <partition_value>

# Run all tests
pytest tests/

# Run core tests only
pytest tests/core/

# Run domain-specific tests
pytest tests/domains/<domain_name>/
```
