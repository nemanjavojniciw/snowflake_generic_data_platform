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

1. Discovers the source schema via the Airbyte API
2. Stores it in the Schema Registry (Snowflake)
3. Generates bronze and silver dbt models and commits them to this repo
4. Registers an Airflow DAG for the source automatically
5. Keeps all models in sync as source schemas evolve

The only thing a human writes is `source_metadata.yml` — primary keys, load strategy, and owner. Everything else is generated.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Extract & Load | Airbyte OSS via `abctl` (local Kubernetes) |
| Data Warehouse | Snowflake |
| Transformations | dbt Core |
| Orchestration | Apache Airflow (Docker Compose, LocalExecutor) |
| Code Generation | Python 3.11 + Jinja2 |
| Data Quality | Elementary |
| Version Control | Git / GitHub |

Airbyte runs on a local Kubernetes cluster managed by `abctl`. Airflow runs via Docker Compose. Snowflake is the only external cloud service.

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) 4.x+
- [Python](https://www.python.org/) 3.11+
- [abctl](https://github.com/airbytehq/abctl/releases/latest) — Airbyte's local Kubernetes installer
- A Snowflake account ([free trial](https://signup.snowflake.com/))
- A GitHub account with a personal access token (for model commits)

Run `.\scripts\setup_env.ps1` from the repo root — it installs `uv`, Python 3.11, Node.js, Docker Desktop, and `abctl` automatically.

---

## Quick Start

### 1. Clone and bootstrap

```powershell
git clone https://github.com/nemanjavojniciw/snowflake_generic_data_platform
cd snowflake_generic_data_platform
.\scripts\setup_env.ps1
```

The setup script installs all prerequisites and copies `compose\.env.example` to `compose\.env`.

### 2. Configure credentials

Edit `compose\.env` with your Snowflake credentials and GitHub token (see [Configuration](#configuration)).

### 3. Start the platform

```powershell
.\start.ps1
```

This starts Airflow (Docker Compose) and Airbyte (`abctl local install`) in parallel.

- First Airbyte run downloads the platform and starts a local Kubernetes cluster — allow up to 20 minutes. Subsequent starts are fast.
- Add `-Build` to rebuild the Airflow image: `.\start.ps1 -Build`

Once up:

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| Airbyte | http://localhost:8000 | run `abctl local credentials` |

To stop Airbyte: `abctl local uninstall`  
To stop Airflow: `docker compose -f compose/docker-compose.yml down`

### 4. Install Python dependencies

```powershell
.\.venv\Scripts\Activate.ps1
pip install -e .
cd dbt_project && dbt deps
```

### 5. Initialise Snowflake

```powershell
python sgdp/scripts/init_snowflake.py
```

Creates all databases, schemas, warehouses, roles, and the Schema Registry table.

---

## Onboarding a New Source

**Target: under 5 minutes from connection to live pipeline.**

**Step 1.** Connect the source in Airbyte (`http://localhost:8000`). Set the destination to Snowflake, database `GENERIC_AIRBYTE_LANDING`, schema = your source name. Note the connection ID from the URL.

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

All data lives in the `GENERIC_AIRBYTE_LANDING` database:

```
GENERIC_AIRBYTE_LANDING
├── PUBLIC          — Airbyte raw landing zone (never query directly)
├── GENERIC_BRONZE  — generated: typed, metadata-enriched views of raw data
└── GENERIC_SILVER  — generated: deduplicated, business-key resolved, analytically ready
```

Tables follow the naming pattern `<layer>_<source_name>_<table_name>`, for example:
- `GENERIC_BRONZE.bronze_stripe_invoices`
- `GENERIC_SILVER.silver_stripe_invoices`

---

## Repository Structure

```
snowflake_generic_data_platform/
│
├── README.md
├── start.ps1                           # start Airflow + Airbyte in parallel
├── pyproject.toml                      # platform package definition
│
├── scripts/
│   ├── setup_env.ps1                   # Windows bootstrap (installs all prereqs)
│   └── requirements.txt               # Python dependencies
│
├── compose/
│   ├── docker-compose.yml              # Airflow stack (LocalExecutor + Postgres)
│   ├── .env.example                    # environment variable template
│   └── .env                           # your local credentials (git-ignored)
│
├── sgdp/                               # platform engine (Python)
│   ├── catalog_syncer.py               # polls Airbyte API → Schema Registry
│   ├── registry.py                     # Schema Registry client
│   ├── evolution_handler.py            # handles schema drift automatically
│   ├── git_client.py                   # commits generated files to this repo
│   ├── cli.py                          # `platform` CLI entrypoint
│   ├── generators/
│   │   ├── bronze_generator.py
│   │   └── silver_generator.py
│   ├── templates/                      # Jinja2 SQL templates
│   │   ├── bronze_model.sql.j2
│   │   ├── bronze_schema.yml.j2
│   │   ├── silver_incremental.sql.j2
│   │   ├── silver_full_refresh.sql.j2
│   │   ├── silver_scd2.sql.j2
│   │   └── sources.yml.j2
│   └── scripts/
│       └── init_snowflake.py           # one-time Snowflake setup
│
├── airflow/
│   └── dags/
│       └── platform_dag_factory.py     # dynamic DAG engine — no hand-written DAGs
│
├── dbt_project/                        # dbt Core project
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml                    # uses env vars from compose/.env
│   ├── macros/
│   │   └── generate_schema_name.sql   # overrides dbt default schema naming
│   └── models/
│       ├── bronze/                     # ⚠ GENERATED — do not edit manually
│       └── silver/                     # ⚠ GENERATED — do not edit manually
│
└── sources/                            # ✅ human-authored config lives here only
    └── <source_name>/
        └── source_metadata.yml
```

---

## Configuration

All configuration lives in `compose/.env`. Copy `compose/.env.example` to get started.

```bash
# Snowflake
SNOWFLAKE_ACCOUNT=youraccount.region
SNOWFLAKE_USER=platform_svc
SNOWFLAKE_PASSWORD=yourpassword
SNOWFLAKE_ROLE=PLATFORM_ADMIN
SNOWFLAKE_WAREHOUSE=TRANSFORM_WH

# Airbyte (local) — get client ID and secret from: abctl local credentials
AIRBYTE_API_URL=http://localhost:8000/api/public/v1
AIRBYTE_CLIENT_ID=<from abctl local credentials>
AIRBYTE_CLIENT_SECRET=<from abctl local credentials>

# Git (for committing generated models)
GIT_REPO_URL=https://github.com/yourorg/snowflake_generic_data_platform
GIT_TOKEN=ghp_yourtokenhere
GIT_BRANCH=main
```

The `platform` CLI and Airflow containers both read from this file automatically — no need to export env vars manually.

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

Run dbt directly only after loading env vars (the CLI handles this automatically):

```powershell
# Load compose/.env into current shell before running dbt directly
Get-Content compose\.env | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
        [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), 'Process')
    }
}
cd dbt_project && dbt run
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

---

## Generated Code

Models in `dbt_project/models/bronze/` and `dbt_project/models/silver/` are generated by the platform and committed here for full auditability.

**Do not edit generated files manually.** Changes will be overwritten on the next schema sync. To modify generation behaviour, edit the Jinja templates in `sgdp/templates/`.

---

## Contributing

The only directories intended for human authoring are:

- `sources/` — source metadata config
- `sgdp/templates/` — SQL generation templates
- `sgdp/` — platform engine Python code
- `airflow/dags/platform_dag_factory.py` — DAG engine

Everything under `dbt_project/models/` is generated. Pull requests editing those files will not be accepted.
