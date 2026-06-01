FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | UV_INSTALL_DIR=/usr/local/bin sh

COPY requirements.txt /tmp/airflow-requirements.txt
RUN uv pip install --system --no-cache -r /tmp/airflow-requirements.txt

USER airflow
