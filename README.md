# Snowflake Generic Data Platform

A self-managing, code-generating data engineering platform that automatically builds bronze and silver dbt transformation layers for any connected data source — with zero hand-written SQL or pipeline code.

Connect a source. Provide five lines of config. The platform does the rest.

---

## How It Works

```
Any Source  →  Airbyte (EL)  →  Snowflake RAW  →  dbt Bronze  →  dbt Silver
                   ↑
         Schema Registry (Snowflake)
                   ↑
         Catalog Syncer  →  Model Generator  →  Git
                   ↑
         Airflow Dynamic DAGs  (one per source, auto-generated)
```

When a new source is connected via Airbyte, the platform:

1. Discovers the source schema via the Airbyte Config API
2. Stores it in the Schema Registry (Snowflake)
3. Generates bronze and silver dbt models and commits them to this repo
4. Registers an Airflow DAG for the source automatically
5. Keeps all models in sync as source schemas evolve

The only thing a human writes is `source_metadata.yml` — primary keys, load strategy, and owner. Everything else is generated.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Extract & Load | Airbyte OSS (local) / Airbyte Cloud |
| Data Warehouse | Snowflake |
| Transformations | dbt Core |
| Orchestration | Apache Airflow via Astro CLI |
| Code Generation | Python 3.11 + Jinja2 |
| Data Quality | Elementary |
| Version Control | Git / GitHub |

> All services run locally via Docker except Snowflake, which has no viable local equivalent. A free Snowflake trial account is sufficient for development.

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) 4.x+
- [Python](https://www.python.org/) 3.11+
- [abctl](https://github.com/airbytehq/abctl/releases/latest) — Airbyte's local installer CLI (handled by `setup_env.ps1`)
- A Snowflake account ([free trial](https://signup.snowflake.com/))
- A GitHub account with a personal access token (for model commits)

Run `.\scripts\setup_env.ps1` from the repo root — it installs Python, abctl, and all dependencies, and copies `.env.example` to `.env`.

---

## Quick Start

### 1. Clone and configure

```bash
git clone https://github.com/nemanjavojniciw/snowflake_generic_data_platform
cd snowflake-data-platform
cp .env.example .env
```

Edit `.env` with your Snowflake credentials and GitHub token (see [Configuration](#configuration)).

### 2. Start Airbyte locally

```powershell
abctl local install
```

First run downloads the Airbyte platform and starts a local Kubernetes cluster inside Docker — allow up to 20 minutes. Subsequent starts are fast.

Once installed, retrieve your credentials:

```powershell
abctl local credentials
```

Airbyte UI available at `http://localhost:8000`.

To stop: `abctl local uninstall`

### 3. Start Airflow

From the repo root:

```powershell
docker compose -f compose/docker-compose.yml up -d --build
```

Airflow UI available at `http://localhost:8080` — credentials `admin / admin`.

### 4. Install Python dependencies

```bash
pip install -r requirements.txt
cd dbt_project && dbt deps
```

### 5. Initialise Snowflake

```bash
python platform/scripts/init_snowflake.py
```

Creates all databases, schemas, warehouses, roles, and the Schema Registry table.

### 6. Verify everything is connected

```bash
python platform/scripts/health_check.py
```

---

## Onboarding a New Source

**Target: under 5 minutes from connection to live pipeline.**

**Step 1.** Connect the source in Airbyte (`http://localhost:8000`). Set the destination to Snowflake, database `RAW`, schema = your source name. Note the connection ID from the URL.

**Step 2.** Register the source with the platform:

```bash
platform add-source \
  --name stripe \
  --airbyte-connection-id abc-123-def \
  --schedule "0 */6 * * *" \
  --owner finance-team
```

This generates a `source_metadata.yml` stub pre-filled with discovered tables and inferred primary keys.

**Step 3.** Review and confirm `sources/stripe/source_metadata.yml`:

```yaml
stripe:
  invoices:
    primary_key: [id]
    load_strategy: incremental
    business_owner: finance-team

  customers:
    primary_key: [id]
    load_strategy: incremental
    business_owner: finance-team

  products:
    primary_key: [id]
    load_strategy: full_refresh
    business_owner: finance-team
```

**Step 4.** Run the initial sync:

```bash
platform sync stripe --generate-models --run-dbt
```

Done. Bronze and silver models are committed to this repo, the Airflow DAG is live, and Snowflake is populated.

---

## Snowflake Layer Design

```
PLATFORM  — schema registry, run logs, source catalog (platform metadata)
RAW       — airbyte landing zone, one schema per source (never query directly)
BRONZE    — generated: typed, metadata-enriched views of raw data
SILVER    — generated: deduplicated, business-key resolved, analytically ready
```

Tables follow the naming pattern `<layer>.<source_name>.<table_name>`, for example `silver.stripe.invoices`.

---

## Repository Structure

```
snowflake-data-platform/
│
├── README.md
├── compose/
│   ├── docker-compose.yml          # Airflow local stack
│   ├── .env.example                # environment variable template
│   └── .env                        # your local credentials (git-ignored)
├── scripts/
│   ├── setup_env.ps1               # Windows bootstrap script
│   └── requirements.txt            # Python dependencies
│
├── platform/                       # platform engine (Python)
│   ├── catalog_syncer.py           # polls Airbyte API → Schema Registry
│   ├── registry.py                 # Schema Registry client
│   ├── evolution_handler.py        # handles schema drift automatically
│   ├── git_client.py               # commits generated files to this repo
│   ├── generators/
│   │   ├── bronze_generator.py
│   │   ├── silver_generator.py
│   │   └── sources_yml_generator.py
│   └── templates/                  # Jinja2 SQL templates
│       ├── bronze_model.sql.j2
│       ├── silver_incremental.sql.j2
│       ├── silver_full_refresh.sql.j2
│       └── silver_scd2.sql.j2
│
├── airflow/                        # Astro CLI project
│   └── dags/
│       └── platform_dag_factory.py # dynamic DAG engine — no hand-written DAGs
│
├── dbt_project/                    # dbt Core project
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── bronze/                 # ⚠ GENERATED — do not edit manually
│   │   └── silver/                 # ⚠ GENERATED — do not edit manually
│   └── macros/
│       └── platform_macros.sql
│
└── sources/                        # ✅ human-authored config lives here only
    └── <source_name>/
        └── source_metadata.yml
```

---

## Configuration

All configuration lives in `.env`. Copy `.env.example` to get started.

```bash
# Snowflake
SNOWFLAKE_ACCOUNT=youraccount.region
SNOWFLAKE_USER=platform_svc
SNOWFLAKE_PASSWORD=yourpassword
SNOWFLAKE_ROLE=PLATFORM_ADMIN
SNOWFLAKE_WAREHOUSE=PLATFORM_WH

# Airbyte (local) — get username/password by running: abctl local credentials
AIRBYTE_API_URL=http://localhost:8000/api/v1
AIRBYTE_USERNAME=<from abctl local credentials>
AIRBYTE_PASSWORD=<from abctl local credentials>

# Git (for committing generated models)
GIT_REPO_URL=https://github.com/yourorg/snowflake-data-platform
GIT_TOKEN=ghp_yourtokenhere
GIT_BRANCH=main
```

---

## Platform CLI

```bash
platform add-source       # register a new source and generate metadata stub
platform sync <source>    # manually trigger airbyte sync + dbt run
platform status           # show all sources, last sync time, and health
platform regen <source>   # force-regenerate all models for a source
platform validate         # run dbt compile across all generated models
platform docs             # serve dbt docs locally
```

---

## Schema Evolution

When a source schema changes, the platform handles it automatically on the next sync:

| Change | Platform Response |
|---|---|
| New table | Generates bronze + silver models, adds to DAG |
| New column | Adds column to generated models, re-commits |
| Removed column | Soft-deprecates with null-fill, alerts owner |
| Type changed | Adds explicit cast in bronze, alerts owner |

No pipeline ever breaks silently on schema drift. Source owners are notified via the configured alert channel.

---

## Local to Cloud Migration

The platform is designed for a zero-rewrite migration to managed cloud services. All connections are environment variables — nothing is hardcoded.

| Service | Migration effort | What changes |
|---|---|---|
| Airbyte OSS → Airbyte Cloud | ~half a day | One environment variable (`AIRBYTE_API_URL`) |
| Astro CLI → Astronomer Cloud | 1–2 days | `astro deploy` + re-create connections in UI |
| dbt Core → dbt Cloud | 1 day | DAG engine updated to trigger dbt Cloud jobs via API |

Platform Python code, generated dbt models, `source_metadata.yml` files, and Snowflake structure are all unchanged by a cloud migration.

---

## Generated Code

Models in `dbt_project/models/bronze/` and `dbt_project/models/silver/` are generated by the platform and committed here for full auditability. Every file contains a header indicating when it was generated and from which schema hash.

**Do not edit generated files manually.** Changes will be overwritten on the next schema sync. To modify generation behaviour, edit the Jinja templates in `platform/templates/`.

---

## Contributing

The only directories intended for human authoring are:

- `sources/` — source metadata config
- `platform/templates/` — SQL generation templates
- `platform/` — platform engine Python code
- `airflow/dags/platform_dag_factory.py` — DAG engine

Everything under `dbt_project/models/` is generated. Pull requests editing those files will not be accepted.