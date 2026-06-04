# Snowflake Generic Data Platform — Project Specification

> **Version:** 0.2 — Implemented & Tested  
> **Goal:** A self-managing, code-generating data engineering platform that automatically produces bronze/silver dbt layers for any connected source, with zero hand-written transformation code.

---

## Table of Contents

1. [Local vs Cloud Strategy](#1-local-vs-cloud-strategy)
2. [Tech Stack](#2-tech-stack)
3. [Local Environment Setup](#3-local-environment-setup)
4. [Architecture Overview](#4-architecture-overview)
5. [Snowflake Layer Design](#5-snowflake-layer-design)
6. [Step 1 — Airbyte: Source Connection & Schema Discovery](#6-step-1--airbyte-source-connection--schema-discovery)
7. [Step 2 — Schema Registry](#7-step-2--schema-registry)
8. [Step 3 — Bronze Model Generator](#8-step-3--bronze-model-generator)
9. [Step 4 — Silver Model Generator](#9-step-4--silver-model-generator)
10. [Step 5 — Airflow: Dynamic DAG Engine](#10-step-5--airflow-dynamic-dag-engine)
11. [Step 6 — Schema Evolution Handler](#11-step-6--schema-evolution-handler)
12. [Step 7 — Observability & Data Quality](#12-step-7--observability--data-quality)
13. [Step 8 — Platform CLI / Onboarding UX](#13-step-8--platform-cli--onboarding-ux)
14. [Source Metadata Config (The Only Human Input)](#14-source-metadata-config-the-only-human-input)
15. [Repository Structure](#15-repository-structure)
16. [Local-to-Cloud Migration Guide](#16-local-to-cloud-migration-guide)
17. [Key Risks & Mitigations](#17-key-risks--mitigations)

---

## 1. Local vs Cloud Strategy

### Answer: Yes — a fully local stack is achievable and recommended for development.

All services in this stack have local/self-hosted versions that are functionally equivalent to their managed cloud counterparts. Snowflake is the **only exception** — there is no local Snowflake emulator worth using in production-equivalent workflows. Use your Snowflake account (trial or paid) as the single cloud dependency from day one.

### Local Service Equivalents

| Service | Local Version | Cloud Version | Config Difference |
|---|---|---|---|
| Airbyte | `abctl` (local Kubernetes inside Docker) | Airbyte Cloud | API base URL + auth only |
| Airflow | Docker Compose | Astronomer Cloud / MWAA | Connection strings only |
| dbt | dbt Core (inside Airflow container) | dbt Cloud | Job trigger method |
| Git | Local repo | GitHub / GitLab | Remote URL only |
| Observability | Elementary (local) | Elementary Cloud | Report destination |
| Snowflake | **No local equivalent** | Snowflake Cloud | N/A — use cloud |

### Local-to-Cloud Migration Effort

**Effort: Low (2–5 days for a full migration)**

The platform is designed around environment abstraction from the start. Every service connection is stored in environment variables or Airflow connections — never hardcoded. Moving to cloud means updating `.env` files and connection configs, not rewriting business logic.

The hardest migration is **Airflow** if you move from Docker Compose to Astronomer/MWAA, because DAG deployment mechanisms differ slightly. Everything else is a config swap.

---

## 2. Tech Stack

### Core Services

| Layer | Tool | Version | Purpose |
|---|---|---|---|
| Extract & Load | **Airbyte OSS** (`abctl`) | Latest stable | Connect any source, load raw data to Snowflake |
| Data Warehouse | **Snowflake** | Standard edition | All data layers: RAW, BRONZE, SILVER, PLATFORM |
| Transformations | **dbt Core** | 1.11 | Execute generated SQL models, run tests (inside Airflow container) |
| Orchestration | **Apache Airflow** | 2.10 (Docker Compose) | Dynamic DAGs, pipeline scheduling |
| Code Generation | **Python 3.11** (`sgdp` package) | — | Schema Registry syncer, model generators |
| Version Control | **Git + GitHub** | — | Generated model storage and audit trail |
| Data Quality | **Elementary** | Latest | dbt-native observability and alerting |
| Package Manager | **uv** | Latest | Manages Python 3.11 venv and dependencies |

### Supporting Libraries

| Library | Purpose |
|---|---|
| `apache-airflow-providers-airbyte` | Trigger Airbyte syncs as Airflow tasks |
| `Jinja2` | Template engine for SQL model generation |
| `PyYAML` | Parse source_metadata.yml |
| `snowflake-connector-python` | Read/write schema registry from Python |
| `gitpython` | Commit generated models programmatically |
| `dbt-snowflake` | dbt adapter for Snowflake |
| `dbt-utils` | Macro library: dedup, surrogate keys, tests |
| `dbt-expectations` | Extended test library for generated test coverage |
| `dbt-date` | Date utility macros |
| `elementary-data` | Data observability package |

### Local Infrastructure

Airflow runs via **Docker Compose** (`compose/docker-compose.yml`). Airbyte runs via `abctl` (local Kubernetes cluster inside Docker).

```
abctl local install          → Airbyte (UI at :8000, Kubernetes-managed)
docker compose -f compose/docker-compose.yml up
  ├── airflow-webserver      (UI at :8080)
  ├── airflow-scheduler
  ├── airflow-triggerer
  └── postgres               (Airflow metadata DB)
```

---

## 3. Local Environment Setup

### Prerequisites

Install these manually before running the bootstrap script.

- **Docker Desktop** 4.x+ (8 GB+ RAM recommended)
- **`uv`** — Python version + venv manager (downloads Python 3.11 automatically)
  - Windows: `powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"`
  - macOS/Linux: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- **`abctl`** — Airbyte CLI binary, add to PATH
  - Windows: download from GitHub releases
  - macOS: `brew install airbytehq/tap/abctl`
  - Linux/WSL2: `curl -LsfS https://get.airbyte.com | bash -s`
- **Git**
- A **Snowflake account** (trial is fine)

### First-Time Bootstrap

Run the setup script once — it handles the venv, dependencies, and Airbyte installation.

```bash
# From repo root — Git Bash, WSL2, or macOS/Linux
bash scripts/setup_env.sh
```

The script will:
1. Create `.venv` with Python 3.11 via `uv venv`
2. Install `scripts/requirements.txt` via `uv pip install`
3. Install the `platform` CLI via `uv pip install -e .`
4. Copy `compose/.env.example` → `compose/.env` (if not present)
5. Run `abctl local install` to deploy Airbyte (~5 min on first run)

### After the Bootstrap Script

**1. Get Airbyte API credentials:**
```bash
abctl local credentials
# Copy client-id and client-secret
```

**2. Fill in `compose/.env`:**
```bash
SNOWFLAKE_ACCOUNT=<org>-<account>
SNOWFLAKE_USER=PLATFORM_SVC
SNOWFLAKE_PASSWORD=<your-password>
SNOWFLAKE_ROLE=PLATFORM_ADMIN
SNOWFLAKE_WAREHOUSE=PLATFORM_WH
SNOWFLAKE_DATABASE=GENERIC_PLATFORM

AIRBYTE_CLIENT_ID=<from abctl local credentials>
AIRBYTE_CLIENT_SECRET=<from abctl local credentials>
```

**3. Activate the venv:**
```bash
source .venv/bin/activate        # macOS/Linux/WSL2
.venv\Scripts\activate           # Windows PowerShell
```

**4. Initialize Snowflake** (creates databases, schemas, RBAC):
```bash
python sgdp/scripts/init_snowflake.py
```

**5. Start Airflow:**
```bash
docker compose -f compose/docker-compose.yml up -d
# Wait ~60 seconds, then open http://localhost:8080
```

**6. Verify the full local stack:**
```bash
python sgdp/scripts/health_check.py
# Checks: Airbyte API, Snowflake connection, Airflow API, dbt connection
```

### Daily Startup / Shutdown

```powershell
# Windows
.\compose\start.bat     # start Airbyte + Airflow
.\compose\stop.bat      # stop Airflow, leave Airbyte running

# macOS/Linux/WSL2
make -C compose up
make -C compose down
```

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airbyte UI | http://localhost:8000 | `abctl local credentials` |
| Airflow UI | http://localhost:8080 | admin / admin (from `.env`) |

---

## 4. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        ANY DATA SOURCE                          │
│           (Postgres, Salesforce, Stripe, S3, REST APIs...)      │
└──────────────────────────┬──────────────────────────────────────┘
                           │ Airbyte connector
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    AIRBYTE (Extract & Load)                     │
│  - Reads source data                                            │
│  - Writes raw JSON + extracted columns to Snowflake RAW         │
│  - Exposes connection catalog via Config API                    │
└──────────────────────────┬──────────────────────────────────────┘
                           │ raw tables
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   SNOWFLAKE RAW DATABASE                        │
│  raw.<source_name>.<table_name>                                 │
│  Contains: _airbyte_raw_id, _airbyte_extracted_at, all columns  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
          ┌────────────────┼──────────────────┐
          ▼                ▼                  ▼
   Schema Registry   Model Generator    DAG Engine
   (Snowflake table) (Python + Jinja2)  (Airflow)
          │                │                  │
          └────────────────┴──────────────────┘
                           │ generated .sql files → git
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    dbt TRANSFORMATION LAYER                     │
│                                                                 │
│  BRONZE: models/bronze/<source>/<table>.sql                     │
│    - Thin select from RAW                                       │
│    - Add _loaded_at, _raw_id metadata                           │
│    - Rename/cast columns to standard types                      │
│                                                                 │
│  SILVER: models/silver/<source>/<table>.sql                     │
│    - Deduplication (using _raw_id + _extracted_at)              │
│    - Business key resolution                                    │
│    - SCD Type 2 or incremental (per source_metadata.yml)        │
│    - Auto-generated dbt tests (not-null PK, freshness, count)   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 PLATFORM CONTROL PLANE                          │
│                                                                 │
│  Schema Registry   ←→   Catalog Syncer (Airbyte API poll)       │
│  Change Detector   →    Evolution Handler (auto re-gen)         │
│  DAG Factory       →    Airflow Dynamic DAGs (1 per source)     │
│  Elementary        →    Data quality alerts + dashboards        │
└─────────────────────────────────────────────────────────────────┘
```

### The Golden Rule

> **Humans author zero dbt models and zero Airflow DAGs.**  
> The only human input is `source_metadata.yml` — one file per source with primary keys, load strategy, and owner. The platform generates everything else.

---

## 5. Snowflake Layer Design

### Database Structure

```sql
-- Four databases, clear separation of concerns
PLATFORM.PUBLIC.schema_registry      -- platform metadata
PLATFORM.PUBLIC.source_catalog       -- airbyte connection registry
PLATFORM.PUBLIC.run_log              -- pipeline execution history

RAW.<source_name>.<table_name>       -- airbyte lands data here (never query directly)

BRONZE.<source_name>.<table_name>    -- generated: typed, metadata-enriched
SILVER.<source_name>.<table_name>    -- generated: deduped, business-ready
```

### Warehouses

```sql
PLATFORM_WH   XS   -- catalog syncer, model generator, schema ops
LOADING_WH    S    -- airbyte writes (auto-suspend 60s)
TRANSFORM_WH  S    -- dbt runs (auto-suspend 60s, auto-resume)
ANALYST_WH    XS   -- analysts querying silver layer
```

### RBAC Roles

```sql
PLATFORM_ADMIN   -- platform services: read/write all databases
LOADER_ROLE      -- airbyte: write to RAW only
TRANSFORMER_ROLE -- dbt: read RAW, write BRONZE + SILVER
ANALYST_ROLE     -- read SILVER only
```

### Schema Registry Table (DDL)

```sql
CREATE TABLE PLATFORM.PUBLIC.schema_registry (
    id                  VARCHAR     NOT NULL DEFAULT UUID_STRING(),
    source_name         VARCHAR     NOT NULL,
    table_name          VARCHAR     NOT NULL,
    columns             VARIANT     NOT NULL,  -- JSON: [{name, type, nullable}]
    primary_keys        ARRAY,
    load_strategy       VARCHAR,               -- incremental | full_refresh | scd2
    schema_hash         VARCHAR,               -- MD5 of sorted column list
    is_active           BOOLEAN     DEFAULT TRUE,
    business_owner      VARCHAR,
    airbyte_connection_id VARCHAR,
    first_seen_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    last_seen_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    last_changed_at     TIMESTAMP_NTZ,
    change_history      VARIANT,               -- JSON array of past schema states
    PRIMARY KEY (source_name, table_name)
);
```

---

## 6. Step 1 — Airbyte: Source Connection & Schema Discovery

### What Airbyte Does in This Platform

Airbyte's job is **extract and load only**. It reads from any source and writes raw data to Snowflake. It does NOT normalize or transform data — that belongs entirely to dbt.

### Airbyte Configuration

When setting up a new connector in Airbyte (local UI at `http://localhost:8000`):

- **Destination**: Snowflake, database `RAW`, schema = source name (e.g. `stripe`)
- **Normalization**: Set to **Raw data (JSON)** — no normalization, no dbt basic normalization
- **Sync mode**: Per-stream, configured in `source_metadata.yml` (incremental append or full refresh)

Airbyte will write tables in this pattern:

```
RAW.stripe._airbyte_raw_invoices
RAW.stripe._airbyte_raw_customers
```

Each table contains:
- `_airbyte_raw_id` — unique ID per raw record
- `_airbyte_extracted_at` — when Airbyte synced the record
- `_airbyte_loaded_at` — when it landed in Snowflake
- `_airbyte_data` — full JSON blob of the source record
- Individual extracted columns (typed by Airbyte where possible)

### Catalog Syncer (Platform Service)

The Catalog Syncer polls the **Airbyte Config API** and populates the Schema Registry. This runs as an Airflow task before every source sync.

```python
# sgdp/catalog_syncer.py (simplified)

import requests
from sgdp.registry import upsert_schema_registry

def sync_catalog():
    connections = requests.get(
        f"{AIRBYTE_API_URL}/connections",
        auth=(AIRBYTE_USER, AIRBYTE_PASS)
    ).json()["connections"]

    for conn in connections:
        catalog = requests.get(
            f"{AIRBYTE_API_URL}/connections/{conn['connectionId']}/schema",
            auth=(AIRBYTE_USER, AIRBYTE_PASS)
        ).json()

        for stream in catalog["catalog"]["streams"]:
            upsert_schema_registry(
                source_name=conn["name"],
                table_name=stream["stream"]["name"],
                columns=stream["stream"]["jsonSchema"]["properties"],
                airbyte_connection_id=conn["connectionId"]
            )
```

The `upsert_schema_registry` function:
1. Computes a schema hash (MD5 of sorted column names + types)
2. Compares against the existing hash in Snowflake
3. If different: updates the registry, appends old schema to `change_history`, sets `last_changed_at`
4. If new table: inserts new registry entry, flags for model generation
5. Returns a diff report: `{new_tables, new_columns, removed_columns, type_changes}`

---

## 7. Step 2 — Schema Registry

### Purpose

The Schema Registry is the platform's single source of truth. Every downstream component — model generators, DAG engine, evolution handler — reads from it. Nothing is derived from live Airbyte state at runtime.

### Registry Operations

```python
# sgdp/registry.py

def get_active_sources() -> list[dict]:
    """Returns all active sources with their latest schema."""

def get_tables_for_source(source_name: str) -> list[dict]:
    """Returns all tables for a source with columns and metadata."""

def get_changed_tables_since(timestamp) -> list[dict]:
    """Returns tables whose schema changed after the given timestamp."""

def mark_model_generated(source_name: str, table_name: str, layer: str):
    """Records that a bronze/silver model was successfully generated."""

def soft_deprecate_column(source_name: str, table_name: str, column_name: str):
    """Marks a column as removed in source but kept in generated models."""
```

### Source Catalog Table (DDL)

```sql
CREATE TABLE PLATFORM.PUBLIC.source_catalog (
    source_name             VARCHAR     NOT NULL PRIMARY KEY,
    airbyte_connection_id   VARCHAR     NOT NULL,
    sync_schedule           VARCHAR,    -- cron expression
    bronze_schema           VARCHAR,    -- defaults to source_name
    silver_schema           VARCHAR,    -- defaults to source_name
    business_owner          VARCHAR,
    slack_alert_channel     VARCHAR,
    is_active               BOOLEAN     DEFAULT TRUE,
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    last_synced_at          TIMESTAMP_NTZ,
    model_generation_status VARCHAR     -- pending | generated | error
);
```

---

## 8. Step 3 — Bronze Model Generator

### What Bronze Models Look Like

Bronze is the thinnest possible transformation layer. It exposes Airbyte's raw columns, renames Airbyte metadata columns to platform-standard names, and adds nothing else.

```sql
-- GENERATED FILE — DO NOT EDIT MANUALLY
-- source: stripe | table: invoices
-- generated_at: 2024-01-15T10:30:00Z | schema_hash: a3f8b2c1

with source as (
    select * from {{ source('stripe', 'invoices') }}
),

bronze as (
    select
        -- platform metadata
        _airbyte_raw_id         as _raw_id,
        _airbyte_extracted_at   as _extracted_at,
        _airbyte_loaded_at      as _loaded_at,

        -- source columns (auto-generated from schema registry)
        id,
        amount_due,
        amount_paid,
        currency,
        customer,
        status,
        created,
        due_date,
        paid_at,
        subscription

    from source
)

select * from bronze
```

### Generator Implementation

```python
# sgdp/generators/bronze_generator.py

from jinja2 import Environment, FileSystemLoader
from sgdp.registry import get_tables_for_source
from sgdp.git_client import commit_generated_file

BRONZE_TEMPLATE = "templates/bronze_model.sql.j2"

def generate_bronze_models(source_name: str) -> list[str]:
    """
    Generates bronze .sql and sources.yml for all tables of a source.
    Commits files to git. Returns list of generated file paths.
    """
    tables = get_tables_for_source(source_name)
    generated_files = []

    env = Environment(loader=FileSystemLoader("sgdp/templates"))
    template = env.get_template("bronze_model.sql.j2")

    for table in tables:
        sql = template.render(
            source_name=source_name,
            table_name=table["table_name"],
            columns=table["columns"],
            schema_hash=table["schema_hash"],
            generated_at=datetime.utcnow().isoformat()
        )

        file_path = f"models/bronze/{source_name}/{table['table_name']}.sql"
        commit_generated_file(file_path, sql, message=f"[platform] regen bronze: {source_name}.{table['table_name']}")
        generated_files.append(file_path)

    generate_sources_yml(source_name, tables)
    return generated_files
```

### Generated `sources.yml`

```yaml
# GENERATED FILE — DO NOT EDIT MANUALLY
# source: stripe | generated_at: 2024-01-15T10:30:00Z

version: 2

sources:
  - name: stripe
    database: RAW
    schema: stripe
    tables:
      - name: invoices
        identifier: _airbyte_raw_invoices
        loaded_at_field: _airbyte_loaded_at
        freshness:
          warn_after: {count: 24, period: hour}
          error_after: {count: 48, period: hour}

      - name: customers
        identifier: _airbyte_raw_customers
        loaded_at_field: _airbyte_loaded_at
        freshness:
          warn_after: {count: 24, period: hour}
          error_after: {count: 48, period: hour}
```

### Bronze Schema Tests (auto-generated `schema.yml`)

```yaml
models:
  - name: stripe__invoices
    description: "Bronze layer for stripe.invoices — generated by platform"
    columns:
      - name: _raw_id
        tests: [not_null, unique]
      - name: _extracted_at
        tests: [not_null]
      - name: id
        tests: [not_null]   # auto-added if column is in primary_keys
```

---

## 9. Step 4 — Silver Model Generator

### What Silver Models Do

Silver is where business logic begins. The generator produces:

- **Deduplication** — remove Airbyte retry duplicates using `_raw_id` + `_extracted_at`
- **Type casting** — cast string columns to proper types based on column name heuristics + Airbyte schema
- **Incremental logic** — dbt `incremental` materialization for append-mode sources
- **SCD Type 2** — dbt `snapshot` for slowly-changing dimension sources
- **Full refresh** — for small reference tables

### Silver Model Example (incremental)

```sql
-- GENERATED FILE — DO NOT EDIT MANUALLY
-- source: stripe | table: invoices | strategy: incremental
-- generated_at: 2024-01-15T10:30:00Z

{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge'
    )
}}

with bronze as (
    select * from {{ ref('stripe__invoices') }}

    {% if is_incremental() %}
    where _extracted_at > (select max(_extracted_at) from {{ this }})
    {% endif %}
),

deduped as (
    {{
        dbt_utils.deduplicate(
            relation=ref('stripe__invoices'),
            partition_by='id',
            order_by='_extracted_at desc'
        )
    }}
),

silver as (
    select
        id,
        amount_due::number(18,2)        as amount_due,
        amount_paid::number(18,2)       as amount_paid,
        currency::varchar(3)            as currency,
        customer                        as customer_id,
        status,
        to_timestamp(created)           as created_at,
        due_date::date                  as due_date,
        to_timestamp(paid_at)           as paid_at,
        subscription                    as subscription_id,

        -- platform metadata
        _raw_id,
        _extracted_at,
        _loaded_at,
        current_timestamp()             as _platform_updated_at

    from deduped
)

select * from silver
```

### Source Metadata Config (Human Input)

The generator reads `source_metadata.yml` to decide which pattern to use per table:

```yaml
# source_metadata.yml — THE ONLY HUMAN-AUTHORED CONFIG PER SOURCE

stripe:
  invoices:
    primary_key: [id]
    load_strategy: incremental
    business_owner: finance-team
    description: "Stripe invoice records"

  customers:
    primary_key: [id]
    load_strategy: incremental
    business_owner: finance-team

  products:
    primary_key: [id]
    load_strategy: full_refresh    # small reference table
    business_owner: finance-team

salesforce:
  accounts:
    primary_key: [id]
    load_strategy: scd2
    scd2_columns: [name, billing_address, phone, owner_id]
    business_owner: sales-team
```

### Silver Generator Logic

```python
# sgdp/generators/silver_generator.py

STRATEGY_TEMPLATES = {
    "incremental": "templates/silver_incremental.sql.j2",
    "full_refresh": "templates/silver_full_refresh.sql.j2",
    "scd2":         "templates/silver_scd2.sql.j2",
}

def generate_silver_model(source_name: str, table_name: str) -> str:
    meta = load_source_metadata(source_name, table_name)
    registry_entry = get_registry_entry(source_name, table_name)

    strategy = meta.get("load_strategy", "incremental")
    template_path = STRATEGY_TEMPLATES[strategy]

    sql = render_template(template_path, {
        "source_name": source_name,
        "table_name": table_name,
        "columns": registry_entry["columns"],
        "primary_keys": meta["primary_key"],
        "load_strategy": strategy,
        "scd2_columns": meta.get("scd2_columns", []),
    })

    file_path = f"models/silver/{source_name}/{table_name}.sql"
    commit_generated_file(file_path, sql)
    generate_silver_schema_yml(source_name, table_name, meta)
    return file_path
```

---

## 10. Step 5 — Airflow: Dynamic DAG Engine

### Principle

Zero hand-written DAGs. The DAG factory reads `PLATFORM.PUBLIC.source_catalog` at Airflow parse time and generates one DAG per active source.

### Standard DAG Shape (per source)

```
trigger_airbyte_sync
        │
wait_for_airbyte_sync
        │
run_catalog_syncer          ← polls Airbyte API, updates Schema Registry
        │
   schema_changed?
    ├── YES → generate_models → dbt_compile → commit_to_git
    └── NO  → skip
        │
dbt_run (tag: <source_name>)    ← BashOperator inside Airflow container
        │
dbt_test (tag: <source_name>)
        │
notify_owner (on failure or schema change)
```

### DAG Factory

```python
# airflow/dags/platform_dag_factory.py

from airflow import DAG
from airflow.decorators import task
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from sgdp.registry import get_active_sources

def build_source_dag(source: dict) -> DAG:
    with DAG(
        dag_id=f"source__{source['source_name']}",
        schedule_interval=source["sync_schedule"],
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
        catchup=False,
        tags=["platform", "auto-generated", source["source_name"]],
    ) as dag:

        sync = AirbyteTriggerSyncOperator(
            task_id="trigger_airbyte_sync",
            airbyte_conn_id="airbyte_local",
            connection_id=source["airbyte_connection_id"],
            asynchronous=True,
        )

        @task
        def run_catalog_syncer(source_name):
            from sgdp.catalog_syncer import sync_source
            return sync_source(source_name)   # returns diff report

        @task.branch
        def check_schema_changed(diff_report):
            if diff_report["has_changes"]:
                return "generate_models"
            return "run_dbt"

        @task
        def generate_models(source_name):
            from sgdp.generators import generate_all_models
            generate_all_models(source_name)

        # dbt runs via BashOperator inside the Airflow container
        # (astronomer-cosmos is a future upgrade — currently shell-based)

        sync >> run_catalog_syncer(source["source_name"]) >> check_schema_changed() >> [generate_models(source["source_name"]), "run_dbt"]
        generate_models(source["source_name"]) >> "run_dbt"

    return dag

# Dynamically register all source DAGs
for source in get_active_sources():
    dag = build_source_dag(source)
    globals()[dag.dag_id] = dag
```

### Airflow Connections (local setup)

Connections are configured via environment variables in `compose/.env` or the Airflow UI at http://localhost:8080.

```bash
# Airbyte connection — set in compose/.env
AIRBYTE_CLIENT_ID=<from abctl local credentials>
AIRBYTE_CLIENT_SECRET=<from abctl local credentials>

# Snowflake credentials — set in compose/.env
SNOWFLAKE_ACCOUNT=<org>-<account>
SNOWFLAKE_USER=PLATFORM_SVC
SNOWFLAKE_PASSWORD=<password>
SNOWFLAKE_ROLE=PLATFORM_ADMIN
SNOWFLAKE_WAREHOUSE=PLATFORM_WH
```

---

## 11. Step 6 — Schema Evolution Handler

### Change Classification

When the Catalog Syncer detects a diff, it classifies every change:

| Change Type | Platform Response | Human Action Required? |
|---|---|---|
| New table | Generate bronze + silver models, add to DAG registry | No |
| New column | Add to bronze model, add to silver model with null-safe cast, re-commit | No |
| Removed column | Soft-deprecate (keep in model, null-fill), alert owner | Alert only |
| Type changed | Add explicit cast in bronze, alert owner | Alert only |
| Table removed | Mark inactive in registry, retain models, alert owner | Alert only |

### Evolution Handler

```python
# sgdp/evolution_handler.py

def handle_schema_changes(source_name: str, diff: dict):

    for table in diff["new_tables"]:
        generate_bronze_models(source_name, tables=[table])
        generate_silver_model(source_name, table)
        log_change(source_name, table, "new_table")

    for change in diff["new_columns"]:
        regenerate_bronze_model(source_name, change["table"])
        regenerate_silver_model(source_name, change["table"])
        log_change(source_name, change["table"], "new_column", change)

    for change in diff["removed_columns"]:
        soft_deprecate_column(source_name, change["table"], change["column"])
        # Bronze model: keep column, add null-fill comment
        # Silver model: keep column, cast as NULL with deprecation comment
        regenerate_bronze_model(source_name, change["table"], deprecated_columns=[change["column"]])
        alert_owner(source_name, change, severity="warning")

    for change in diff["type_changes"]:
        # Add explicit try_cast in bronze, alert owner
        regenerate_bronze_model(source_name, change["table"])
        alert_owner(source_name, change, severity="warning")
```

### Soft Deprecation Pattern in Generated SQL

```sql
-- DEPRECATED COLUMN: billing_email removed from source 2024-03-01
-- Kept for backwards compatibility. Will be removed in 90 days.
null::varchar    as billing_email,
```

---

## 12. Step 7 — Observability & Data Quality

### Elementary (dbt-native observability)

Elementary runs as a dbt package and generates a data observability report after every dbt run.

```yaml
# packages.yml
packages:
  - package: elementary-data/elementary
    version: 0.14.0
  - package: dbt-labs/dbt_utils
    version: 1.2.0
  - package: dbt-labs/dbt_expectations
    version: 0.10.0
```

### Auto-generated Tests per Layer

Every generated model gets these tests automatically:

**Bronze:**
- `not_null` + `unique` on `_raw_id`
- `not_null` on `_extracted_at`
- `not_null` on primary key columns (from registry)
- `elementary.volume_anomalies` — row count anomaly detection
- `elementary.freshness_anomalies` — data arrival time anomaly

**Silver:**
- Everything from Bronze
- `dbt_utils.expression_is_true` on business rule invariants where inferable
- `relationships` test for foreign keys where source metadata defines them
- `dbt_expectations.expect_column_values_to_be_between` for numeric ranges (where min/max observed in registry)

### Run Logging

```sql
CREATE TABLE PLATFORM.PUBLIC.run_log (
    run_id              VARCHAR     DEFAULT UUID_STRING() PRIMARY KEY,
    source_name         VARCHAR,
    dag_run_id          VARCHAR,
    airbyte_job_id      VARCHAR,
    dbt_run_status      VARCHAR,    -- success | error | partial
    rows_loaded_bronze  NUMBER,
    rows_loaded_silver  NUMBER,
    schema_changes      VARIANT,    -- JSON diff from this run
    started_at          TIMESTAMP_NTZ,
    completed_at        TIMESTAMP_NTZ,
    duration_seconds    NUMBER
);
```

---

## 13. Step 8 — Platform CLI / Onboarding UX

### Onboarding a New Source (Target: Under 5 Minutes)

```bash
# 1. Connect source in Airbyte UI (http://localhost:8000)
#    Point destination to RAW database, schema = source_name
#    Note the connection_id from the URL

# 2. Create source_metadata.yml entry
platform add-source \
  --name salesforce \
  --airbyte-connection-id abc-123-def \
  --schedule "0 */6 * * *" \
  --owner sales-team

# 3. Platform auto-generates source_metadata.yml stub:
#    > sources/salesforce/source_metadata.yml
#    > Pre-filled with discovered tables and inferred PKs
#    > Human reviews and confirms primary keys

# 4. Run initial sync + generation
platform sync salesforce --generate-models --run-dbt

# 5. Done — DAG is live, models are in git, Snowflake layers are populated
```

### Platform CLI Commands

```bash
platform init                                          # init Snowflake schemas + RBAC
platform add-source --name X --airbyte-connection-id Y # register a new source
platform sync <source> --generate-models --run-dbt     # manual trigger: airbyte + dbt
platform status                                        # show all sources, last sync, health
platform regen <source>                                # force regenerate all models for source
platform validate                                      # validate all source_metadata.yml files
platform docs                                          # serve dbt docs at http://localhost:8001
platform prune                                         # delete local generated data (Snowflake cleanup is manual)
```

---

## 14. Source Metadata Config (The Only Human Input)

This is the **complete specification** of what a human needs to provide to onboard a new source.

```yaml
# sources/<source_name>/source_metadata.yml

<source_name>:

  <table_name>:
    primary_key: [<column>, ...]       # required: list of PK columns
    load_strategy: incremental         # required: incremental | full_refresh | scd2
    business_owner: <team-or-email>    # required: for alerts
    description: "..."                 # optional
    sync_schedule: "0 */6 * * *"       # optional: overrides source-level schedule
    scd2_columns: [...]                # required only if load_strategy: scd2
    tags: [...]                        # optional: extra dbt tags
```

Everything else — SQL, tests, sources.yml, DAG — is generated.

---

## 15. Repository Structure

```
snowflake_generic_data_platform/
│
├── pyproject.toml                  # package definition: snowflake-generic-data-platform 0.1.0
├── README.md
├── STARTUP.md                      # startup + daily ops guide
├── snowflake_platform_spec.md      # this document
│
├── sgdp/                           # platform Python package (entry point: `platform` CLI)
│   ├── __init__.py
│   ├── cli.py                      # Click CLI: init, add-source, sync, status, regen, validate, docs, prune
│   ├── catalog_syncer.py           # Airbyte API → Schema Registry
│   ├── registry.py                 # Schema Registry read/write client
│   ├── evolution_handler.py        # schema change classification + response
│   ├── git_client.py               # commit generated files to git
│   ├── generators/
│   │   ├── bronze_generator.py
│   │   └── silver_generator.py
│   ├── templates/
│   │   ├── bronze_model.sql.j2
│   │   ├── bronze_schema.yml.j2
│   │   ├── silver_incremental.sql.j2
│   │   ├── silver_full_refresh.sql.j2
│   │   ├── silver_scd2.sql.j2
│   │   └── sources.yml.j2
│   └── scripts/
│       ├── init_snowflake.py       # one-time Snowflake setup
│       └── health_check.py
│
├── scripts/                        # bootstrap / env setup
│   ├── setup_env.sh                # first-time setup (uv venv + abctl install)
│   ├── setup_env.ps1               # Windows equivalent
│   └── requirements.txt            # dev/tooling deps
│
├── compose/                        # Airflow Docker Compose stack
│   ├── docker-compose.yml
│   ├── .env.example                # environment variable template
│   ├── .env                        # filled-in credentials (gitignored)
│   ├── Makefile                    # make up / make down shortcuts
│   ├── start.bat                   # Windows startup script
│   └── stop.bat                    # Windows shutdown script
│
├── airflow/                        # Airflow DAGs + Dockerfile
│   ├── Dockerfile
│   ├── requirements.txt            # Airflow provider deps (dbt-snowflake, airbyte provider)
│   └── dags/
│       └── platform_dag_factory.py # dynamic DAG engine
│
├── dbt_project/                    # dbt Core project
│   ├── dbt_project.yml
│   ├── packages.yml                # elementary, dbt_utils, dbt_expectations, dbt_date
│   ├── profiles.yml
│   ├── models/
│   │   ├── bronze/                 # GENERATED — do not edit manually
│   │   │   └── <source>/
│   │   │       ├── <table>.sql
│   │   │       ├── sources.yml
│   │   │       └── schema.yml
│   │   └── silver/                 # GENERATED — do not edit manually
│   │       └── <source>/
│   │           ├── <table>.sql
│   │           └── schema.yml
│   └── macros/
│       └── generate_schema_name.sql  # custom schema routing
│
└── sources/                        # human-authored config (the only one)
    └── <source_name>/
        ├── source_metadata.yml     # primary keys, load strategy, owner
        └── source_schema.yml       # generated schema snapshot (do not edit)
```

---

## 16. Local-to-Cloud Migration Guide

### Migration Effort by Service

#### Airbyte OSS → Airbyte Cloud

**Effort: 0.5 days**

1. Create an Airbyte Cloud account
2. Recreate connections in the Cloud UI (or use Terraform provider)
3. Update one environment variable: `AIRBYTE_API_URL=https://api.airbyte.com/v1`
4. Update Airflow connection `airbyte_local` with Cloud API token
5. That is the entire migration — the platform code does not change

#### Airflow (Docker Compose) → Astronomer Cloud / MWAA

**Effort: 1–2 days**

1. Create Astronomer Cloud workspace and deployment (or configure MWAA environment)
2. Push DAGs and `airflow/requirements.txt` to the deployment
3. Re-create Airflow connections (Airbyte, Snowflake) in cloud UI or via environment variables
4. Update `compose/.env` equivalents in cloud secrets manager
5. Note: Dynamic DAG pattern works identically in Astronomer Cloud and MWAA

#### dbt Core → dbt Cloud

**Effort: 1 day**

1. Connect dbt Cloud to your git repo
2. Create environments (dev, prod) pointing to Snowflake
3. Update the DAG engine: replace shell-based dbt tasks with dbt Cloud job trigger via the dbt Cloud API
4. Astronomer Cosmos can also wrap dbt Core tasks natively — a future upgrade path

#### What Does NOT Change

- All `sgdp/` Python code (catalog syncer, generators, evolution handler)
- All generated dbt models
- All `source_metadata.yml` files
- Snowflake structure, RBAC, and layer design
- The Schema Registry and its logic

### Cloud Migration Checklist

```
□ Update AIRBYTE_CLIENT_ID / AIRBYTE_CLIENT_SECRET for Airbyte Cloud
□ Update Airflow connection vars in cloud secrets manager
□ Re-create Airflow connections (Airbyte, Snowflake) in cloud UI
□ Push DAGs + airflow/requirements.txt to cloud Airflow deployment
□ Connect dbt Cloud to git repo (if migrating dbt)
□ Update DAG engine to use dbt Cloud job trigger (if migrating dbt)
□ Verify Elementary report destination (local → Elementary Cloud or S3)
□ Run platform validate in cloud environment
□ Run smoke test: platform sync <one_source>
```

---

## 17. Key Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| **Airbyte delivers duplicates** | High | Medium | Silver generator always emits `dbt_utils.deduplicate` step using `_raw_id` + `_extracted_at` as tiebreaker |
| **Schema drift breaks models** | High | High | Evolution handler detects, classifies, and regenerates on every sync. Soft deprecation prevents hard breaks. |
| **Primary key unknown at onboard** | Medium | High | Platform infers PK candidates (highest cardinality columns) and flags for human confirmation before first silver run |
| **DAG fan-out overloads Snowflake** | Medium | Medium | Airflow pool limits per source. All warehouses auto-suspend. Transform warehouse has concurrency limit. |
| **Generated code quality** | Low | Medium | All generated code committed to git. CI runs `dbt compile` on every generation commit. |
| **git commit race condition** | Low | Medium | Model generator uses file-level locking + branch-per-source-run pattern with auto-merge |
| **Airflow parse time grows** | Medium | Low | Dynamic DAG factory caches registry reads with 60s TTL. Source count > 200 → switch to DAG serialization. |

---

*End of specification. Generated models live in `dbt_project/models/`. Human config lives in `sources/`. Platform code lives in `sgdp/`. Everything else is generated.*
