FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | UV_INSTALL_DIR=/usr/local/bin sh

# ── dbt + elementary in an isolated virtualenv ───────────────────────────────
# dbt's pins (sqlparse, google libs) conflict with Airflow's, so they get their
# own environment. Cosmos / the platform DAGs invoke dbt and edr as subprocesses
# via /opt/dbt_venv/bin, which is added to PATH below.
RUN uv venv /opt/dbt_venv --python 3.11 \
    && VIRTUAL_ENV=/opt/dbt_venv uv pip install --no-cache \
        dbt-snowflake==1.11.5 \
        elementary-data==0.24.0 \
    && chown -R airflow: /opt/dbt_venv

# Make dbt/edr resolvable on PATH without shadowing Airflow's own python.
ENV PATH="${PATH}:/opt/dbt_venv/bin"

# ── Airflow-side packages ────────────────────────────────────────────────────
# Installed as the airflow user so they land in /home/airflow/.local/... where
# Airflow's interpreter actually looks (NOT /usr/local, which it does not import).
USER airflow
COPY requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
    -r /tmp/airflow-requirements.txt
