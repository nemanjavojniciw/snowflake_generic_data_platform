# Snowflake Generic Data Platform — Startup Guide

## Stack Overview

| Component | Tool | Access |
|-----------|------|--------|
| Extract & Load | Airbyte OSS via `abctl` | http://localhost:8000 |
| Orchestration | Apache Airflow 2.10 (Docker Compose) | http://localhost:8080 |
| Warehouse | Snowflake | Cloud |
| Transformations | dbt-core 1.11 (inside Airflow container) | — |
| Platform CLI | `platform` command (uv-managed venv) | terminal |

---

## Prerequisites

Install these before running the bootstrap script. **No system Python required** — `uv` downloads and manages Python 3.11 for the project venv automatically.

### 1. Docker Desktop
Download and install from https://www.docker.com/products/docker-desktop/

After installation, start Docker Desktop and verify the daemon is running:
```powershell
docker info
```

### 2. uv (Python + venv manager)
`uv` manages the project's Python version and virtualenv. It downloads Python 3.11 itself — no system Python installation needed.

**Windows (PowerShell):**
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

**macOS / Linux / WSL2:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Verify:
```bash
uv --version
```

### 3. abctl (Airbyte CLI)
`abctl` manages Airbyte OSS using a local Kubernetes cluster inside Docker.

**Windows** — download the latest binary from:
https://github.com/airbytehq/abctl/releases/latest

Add the extracted binary to a directory on your `PATH` (e.g. `C:\tools\`).

**macOS (Homebrew):**
```bash
brew install airbytehq/tap/abctl
```

**Linux / WSL2:**
```bash
curl -LsfS https://get.airbyte.com | bash -s
```

Verify:
```bash
abctl version
```

### 4. nvm + Node.js 18

**macOS / Linux / WSL2:**
```bash
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
source ~/.bashrc   # or ~/.zshrc
nvm install 18
nvm use 18
```

**Windows (native):** use [nvm-windows](https://github.com/coreybutler/nvm-windows/releases).

### 5. Git
Install from https://git-scm.com/ if not already present.

---

## First-Time Setup

Run the bootstrap script once. It handles the venv, dependencies, and Airbyte installation.

```bash
# From repo root — Git Bash, WSL2, or macOS/Linux terminal
bash scripts/setup_env.sh
```

The script will:
- Check `uv` is installed (and offer to install it if not)
- Create `.venv` with Python 3.11 via `uv venv --python 3.11` (no system Python needed)
- Install `scripts/requirements.txt` via `uv pip install`
- Install the `platform` CLI via `uv pip install -e .`
- Install Node.js 18 via nvm (if not already present)
- Check Docker Desktop is running
- Copy `compose/.env.example` → `compose/.env` (if `.env` does not exist)
- Run `abctl local install` to deploy Airbyte (~5 min on first run)

### After the script finishes

**1. Get Airbyte API credentials:**
```bash
abctl local credentials
```
Copy the `client-id` and `client-secret` values.

**2. Fill in `compose/.env`:**
```
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

**4. Initialise Snowflake** (creates databases, schemas, RBAC):
```bash
python sgdp/scripts/init_snowflake.py
```

**5. Start Airflow:**
```bash
docker compose -f compose/docker-compose.yml up -d
```
Wait ~60 seconds for `airflow-init` to complete, then open http://localhost:8080.

**6. Health check:**
```bash
python sgdp/scripts/health_check.py
```

---

## Daily Startup

Airbyte must be started before Airflow (DAGs connect to its API).

### Windows
```powershell
.\compose\start.bat
```

### macOS / Linux / WSL2
```bash
docker start airbyte-abctl-control-plane
docker compose -f compose/docker-compose.yml up -d
```

Or via the Makefile shortcut:
```bash
make -C compose up
```

---

## Daily Shutdown

### Windows
```powershell
.\compose\stop.bat
```

### macOS / Linux / WSL2
```bash
docker compose -f compose/docker-compose.yml down
docker stop airbyte-abctl-control-plane
```

Or:
```bash
make -C compose down
```

---

## Service URLs & Credentials

| Service | URL | Credentials |
|---------|-----|-------------|
| Airbyte UI | http://localhost:8000 | Run `abctl local credentials` |
| Airflow UI | http://localhost:8080 | admin / admin (set in `compose/.env`) |

---

## abctl Reference

| Command | Description |
|---------|-------------|
| `abctl local install` | First-time Airbyte deployment (~5 min) |
| `abctl local status` | Show cluster info and Helm chart versions |
| `abctl local credentials` | Print email, password, client-id, client-secret |
| `abctl local credentials --email X --password Y` | Change Airbyte UI login |
| `abctl local deployments --restart` | Restart Airbyte pods without reinstalling |
| `abctl local uninstall` | Stop Airbyte, remove cluster (data preserved) |
| `abctl local uninstall --persisted` | Stop and delete all data |
| `abctl version` | Print abctl version |

---

## Platform CLI Reference

Activate the venv before using `platform` commands:
```bash
source .venv/bin/activate    # macOS/Linux/WSL2
.venv\Scripts\activate       # Windows PowerShell
```

| Command | Description |
|---------|-------------|
| `platform init` | Initialise Snowflake schemas and RBAC |
| `platform add-source --name X --airbyte-connection-id Y` | Register a new source |
| `platform sync X --generate-models --run-dbt` | Sync catalog and generate/run dbt models |
| `platform status` | Show all registered sources and their state |
| `platform regen X` | Force-regenerate bronze/silver models for source X |
| `platform validate` | Validate all source_metadata.yml files |
| `platform docs` | Serve dbt docs at http://localhost:8001 |

---

## Troubleshooting

**Airflow containers restart immediately:**
Check that `compose/.env` is fully populated — containers exit if Snowflake variables are empty.

**`abctl local install` fails:**
Ensure Docker Desktop is running and has enough resources (8 GB+ RAM recommended).
Check status: `abctl local status`

**`platform` command not found:**
The venv is not active, or `uv pip install -e .` was not run.
```bash
source .venv/bin/activate
uv pip install -e .
```

**`uv venv` fails with "no Python 3.11 found":**
Run `uv python install 3.11` to let uv download it, then rerun `uv venv .venv --python 3.11`.

**`docker compose up -d` from repo root fails:**
The compose file uses relative paths and must be found. Use the `-f` flag from repo root:
```bash
docker compose -f compose/docker-compose.yml up -d
```
