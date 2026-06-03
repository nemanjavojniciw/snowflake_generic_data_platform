"""
Platform DAG Factory

Dynamically generates one Airflow DAG per active source in source_catalog.

Each DAG follows this pattern:
  trigger_airbyte_sync
    ↓
  wait_for_airbyte_sync
    ↓
  run_catalog_syncer (detect schema changes)
    ↓
  schema_changed?
    ├─ YES → generate_models → dbt_compile → [to dbt_run]
    └─ NO  → [skip to dbt_run]
    ↓
  dbt_run (all models tagged with source_name)
    ↓
  dbt_test
    ↓
  notify_owner (on failure or critical changes)

Runs at Airflow parse time — reads source_catalog every time scheduler parses DAGs.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.utils.task_group import TaskGroup

# Import platform services
import sys
from pathlib import Path

# Add platform to path — sgdp is mounted at /opt/airflow/sgdp
sys.path.insert(0, str(Path(__file__).parent.parent))

from sgdp.registry import SchemaRegistry
from sgdp.catalog_syncer import CatalogSyncer
from sgdp.evolution_handler import EvolutionHandler

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# Cache registry reads to avoid repeated DB hits during parse
_REGISTRY_CACHE = None
_CACHE_TIME = None
CACHE_TTL_SECONDS = 60


def get_registry():
    """Get registry with caching."""
    global _REGISTRY_CACHE, _CACHE_TIME
    now = datetime.utcnow().timestamp()

    if _REGISTRY_CACHE is None or (now - _CACHE_TIME) > CACHE_TTL_SECONDS:
        _REGISTRY_CACHE = SchemaRegistry()
        _CACHE_TIME = now

    return _REGISTRY_CACHE


# ──────────────────────────────────────────────────────────────────────────────
# Task Functions
# ──────────────────────────────────────────────────────────────────────────────


def build_source_dag(source: dict) -> DAG:
    """
    Build a complete DAG for a single source.

    Args:
        source: Dict from SchemaRegistry.get_active_sources()

    Returns:
        Configured DAG object
    """
    source_name = source["source_name"]
    airbyte_conn_id = source["airbyte_connection_id"]
    sync_schedule = source.get("sync_schedule", "0 */6 * * *")  # Default: every 6 hours

    dag_id = f"source__{source_name}"

    with DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        schedule_interval=sync_schedule,
        catchup=False,
        tags=["platform", "auto-generated", source_name],
        description=f"Auto-generated platform DAG for {source_name}",
        start_date=datetime(2024, 1, 1),
    ) as dag:

        # ────────────────────────────────────────────────────────────────────────────
        # 1. Trigger Airbyte sync
        # ────────────────────────────────────────────────────────────────────────────

        trigger_sync = AirbyteTriggerSyncOperator(
            task_id="trigger_airbyte_sync",
            airbyte_conn_id="airbyte_local",
            connection_id=airbyte_conn_id,
            asynchronous=False,  # block until Airbyte sync completes before running dbt
        )

        # ────────────────────────────────────────────────────────────────────────────
        # 2. Run catalog syncer (detect schema changes)
        # ────────────────────────────────────────────────────────────────────────────

        @task
        def sync_catalog(source_name: str):
            """Poll Airbyte catalog and sync to schema registry."""
            syncer = CatalogSyncer()
            diff_report = syncer.sync_source(source_name)
            return diff_report

        catalog_sync_result = sync_catalog(source_name)

        # ────────────────────────────────────────────────────────────────────────────
        # 3. Branch: schema changed?
        # ────────────────────────────────────────────────────────────────────────────

        @task.branch
        def check_schema_changed(diff_report: dict) -> str:
            """Return task ID to execute next."""
            if diff_report.get("has_changes"):
                return "generate_models.regenerate"
            return "dbt_run"

        branch_result = check_schema_changed(catalog_sync_result)

        # ────────────────────────────────────────────────────────────────────────────
        # 4. Generate models (if schema changed)
        # ────────────────────────────────────────────────────────────────────────────

        @task
        def regenerate(source_name: str):
            """Regenerate bronze and silver models."""
            handler = EvolutionHandler()
            syncer = CatalogSyncer()

            # Get fresh diff
            diff_report = syncer.sync_source(source_name)

            if diff_report.get("has_changes"):
                summary = handler.handle_changes(source_name, diff_report)
                return summary
            return {"source_name": source_name, "actions": []}

        with TaskGroup(group_id="generate_models") as generate_group:
            regen_task = regenerate(source_name)

        # ────────────────────────────────────────────────────────────────────────────
        # 5. dbt run and test
        # ────────────────────────────────────────────────────────────────────────────

        @task
        def dbt_run_source(source_name: str):
            """Run dbt for this source."""
            # In real setup, would use Cosmos DbtTaskGroup or shell operator
            # For now, return a placeholder
            import subprocess

            result = subprocess.run(
                [
                    "dbt", "run",
                    "--select", f"tag:{source_name}",
                    "--project-dir", "/opt/dbt",
                    "--profiles-dir", "/opt/dbt",
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                raise AirflowException(f"dbt run failed: {result.stderr}")

            return result.stdout

        @task
        def dbt_test_source(source_name: str):
            """Test dbt models for this source."""
            import subprocess

            result = subprocess.run(
                [
                    "dbt", "test",
                    "--select", f"tag:{source_name}",
                    "--project-dir", "/opt/dbt",
                    "--profiles-dir", "/opt/dbt",
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                raise AirflowException(f"dbt test failed: {result.stderr}")

            return result.stdout

        dbt_run_task = dbt_run_source(source_name)
        dbt_test_task = dbt_test_source(source_name)

        # ────────────────────────────────────────────────────────────────────────────
        # 6. Notify owner (on failure)
        # ────────────────────────────────────────────────────────────────────────────

        @task
        def notify_on_failure(source_name: str):
            """Alert business owner if something failed."""
            owner = Variable.get(f"{source_name}_business_owner", default="admin@platform")
            # In real setup, would integrate with Slack, PagerDuty, etc.
            print(f"Would alert {owner} of failure/changes for {source_name}")

        notify = notify_on_failure(source_name)

        # ────────────────────────────────────────────────────────────────────────────
        # DAG dependencies
        # ────────────────────────────────────────────────────────────────────────────

        trigger_sync >> catalog_sync_result >> branch_result
        branch_result >> generate_group >> dbt_run_task
        branch_result >> dbt_run_task  # Also link NO branch
        dbt_run_task >> dbt_test_task >> notify

    return dag


# ──────────────────────────────────────────────────────────────────────────────
# Dynamically Register DAGs
# ──────────────────────────────────────────────────────────────────────────────

# Read active sources from registry at parse time
try:
    registry = get_registry()
    sources = registry.get_active_sources()

    for source in sources:
        dag = build_source_dag(source)
        # Register in Airflow's global namespace
        globals()[dag.dag_id] = dag

except Exception as e:
    print(f"⚠️  Error building platform DAGs: {e}")
    print("   Ensure Snowflake is accessible and schema_registry is populated.")
