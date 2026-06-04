#!/usr/bin/env python3
"""
Platform CLI — User-friendly interface for the data platform.

Commands:
  platform add-source     Register a new Airbyte source
  platform sync           Manually trigger Airbyte + dbt pipeline
  platform status         Show all sources and their health
  platform regen          Force regenerate models for a source
  platform deprecate      Soft-retire a table
  platform validate       Run dbt compile + test
  platform docs           Serve dbt docs locally

Usage:
  platform --help
  platform add-source --name stripe --airbyte-connection-id abc-123
  platform sync stripe
  platform status
"""

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import click
import yaml
from dotenv import load_dotenv

# Load compose/.env automatically so credentials don't need to be set manually in the shell.
# existing shell env vars take priority (override=False is the default).
PROJECT_ROOT = Path(__file__).parent.parent
DBT_DIR = PROJECT_ROOT / "dbt_project"

_env_file = PROJECT_ROOT / "compose" / ".env"
load_dotenv(_env_file)

from sgdp.catalog_syncer import CatalogSyncer
from sgdp.evolution_handler import EvolutionHandler
from sgdp.generators.bronze_generator import BronzeGenerator
from sgdp.generators.silver_generator import SilverGenerator
from sgdp.registry import SchemaRegistry

# Color codes
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
BOLD = "\033[1m"
RESET = "\033[0m"


def print_success(msg: str):
    """Print success message."""
    click.echo(f"{GREEN}✓{RESET} {msg}")


def print_info(msg: str):
    """Print info message."""
    click.echo(f"{BLUE}ℹ{RESET} {msg}")


def print_warn(msg: str):
    """Print warning message."""
    click.echo(f"{YELLOW}⚠{RESET} {msg}")


def print_error(msg: str):
    """Print error message."""
    click.echo(f"{RED}✗{RESET} {msg}")


def print_header(msg: str):
    """Print section header."""
    click.echo(f"\n{BOLD}{msg}{RESET}")


def _validate_sync_frequency(cron_schedule: str):
    """Warn if sync frequency is too high (hourly)."""
    parts = cron_schedule.split()
    if len(parts) != 5:
        print_warn(f"Invalid cron expression: {cron_schedule}")
        return

    minute, hour, day, month, dow = parts

    # Check for hourly (hour field is * or */1)
    if hour == "*" or hour == "*/1":
        print_warn("⚠️  Hourly syncs cost ~$144/month vs daily at ~$24/month!")
        print_warn("    Only use hourly if business-critical. Set --schedule explicitly to confirm.")
        raise click.ClickException("Hourly syncs require explicit confirmation. See warning above.")

    # Check for 4+ times daily
    if hour.startswith("*/") and int(hour.split("/")[1]) <= 6:
        times_daily = 24 // int(hour.split("/")[1])
        cost_per_month = times_daily * 6  # Rough estimate: 6 credits per sync
        print_warn(f"⚠️  {times_daily}x daily syncs = ~${cost_per_month}/month. Consider 1-2x daily instead.")


# ──────────────────────────────────────────────────────────────────────────────
# Main CLI Group
# ──────────────────────────────────────────────────────────────────────────────


@click.group()
@click.version_option()
def cli():
    """Snowflake Generic Data Platform CLI"""
    pass


# ──────────────────────────────────────────────────────────────────────────────
# Command: add-source
# ──────────────────────────────────────────────────────────────────────────────


@cli.command()
@click.option("--name", required=True, help="Source name (e.g., 'stripe')")
@click.option("--airbyte-connection-id", required=True, help="Airbyte connection UUID")
@click.option("--schedule", default="0 0 * * *", help="Sync schedule (cron format, default: daily at midnight UTC)")
@click.option("--owner", default=None, help="Business owner email/team")
def add_source(name: str, airbyte_connection_id: str, schedule: str, owner: str):
    """
    Register a new Airbyte source.

    Creates source_metadata.yml stub for manual configuration of primary keys.

    Default schedule: daily (0 0 * * * = 00:00 UTC)
    Hourly syncs cost ~$144/month vs daily at ~$24/month — only use if needed.

    Example:
      platform add-source --name stripe --airbyte-connection-id abc-123-def
      platform add-source --name stripe --airbyte-connection-id abc-123-def --schedule "0 */12 * * *"
    """
    try:
        print_header(f"Registering source: {name}")

        # Validate sync frequency
        _validate_sync_frequency(schedule)

        registry = SchemaRegistry()

        # Register in source_catalog
        registry.register_source(
            source_name=name,
            airbyte_connection_id=airbyte_connection_id,
            sync_schedule=schedule,
            business_owner=owner or f"{name}-team",
        )
        print_success(f"Registered source: {name}")

        # Patch Airbyte connection namespace so data lands in the right schema
        print_info("Setting Airbyte connection namespace...")
        syncer = CatalogSyncer()
        syncer.set_connection_namespace(airbyte_connection_id, name)
        print_success(f"Namespace set → GENERIC_AIRBYTE_LANDING.{name}")

        # Discover schema from Airbyte source connector (no sync required)
        print_info("Discovering schema from source connector...")
        stream_catalog = syncer.get_stream_catalog(airbyte_connection_id)

        sources_dir = Path("sources") / name
        sources_dir.mkdir(parents=True, exist_ok=True)
        metadata_path = sources_dir / "source_metadata.yml"

        # If discovery returned nothing, fall back to stream names from configured catalog
        if not stream_catalog:
            print_warn("Schema discovery returned no column info — fetching stream names only.")
            diff_report = syncer.sync_source(name)
            stream_catalog = {t: {"columns": {}, "source_pk": [], "sync_modes": []} for t in diff_report.get("all_tables", [])}

        # Build source_metadata.yml
        # Use source-defined primary key when available; fall back to "id"
        source_meta: dict = {}
        for table, info in stream_catalog.items():
            col_names = sorted(info["columns"].keys())
            suggested_pk = info["source_pk"] or ["id"]
            entry: dict = {
                "primary_key": suggested_pk,
                "load_strategy": "incremental",
                "business_owner": owner or f"{name}-team",
                "description": f"Table: {table}",
            }
            if col_names:
                entry["_columns"] = col_names
            source_meta[table] = entry

        with open(metadata_path, "w") as f:
            yaml.dump({name: source_meta}, f, default_flow_style=False, sort_keys=False)

        print_success(f"Created {metadata_path}")

        # Write source_schema.yml with full type info
        table_schema = {t: info["columns"] for t, info in stream_catalog.items()}
        source_pk_map = {t: info["source_pk"] for t, info in stream_catalog.items()}
        _write_source_schema(name, table_schema, source_pk_map, sources_dir)
        print_success(f"Created sources/{name}/source_schema.yml")

        # ── Try to populate columns immediately from Snowflake landing zone ──
        # If this source was synced before (even under a previous name or before a prune),
        # GENERIC_AIRBYTE_LANDING.<name> already has the tables and columns — grab them now.
        print_info("Checking Snowflake landing zone for existing column data...")
        try:
            _populate_columns_from_landing(name, syncer)
            # Reload stream_catalog column info from what was just written so the summary is accurate
            with open(metadata_path) as f:
                written_meta = yaml.safe_load(f) or {}
            for table, entry in written_meta.get(name, {}).items():
                if table in stream_catalog:
                    cols = entry.get("_columns", [])
                    stream_catalog[table]["columns"] = {c: {} for c in cols}
        except Exception:
            pass  # landing zone empty — columns will appear after trigger-sync

        # ── Print final state ─────────────────────────────────────────────────
        has_columns = any(info["columns"] for info in stream_catalog.values())
        has_pk = any(info["source_pk"] for info in stream_catalog.values())

        print_header("Source schema")
        max_tbl = max((len(t) for t in stream_catalog), default=10)
        for table, info in sorted(stream_catalog.items()):
            col_names = sorted(info["columns"].keys())
            pk = info["source_pk"]
            pk_str = f"pk={','.join(pk)}" if pk else "pk=?"
            col_str = ", ".join(col_names) if col_names else "(run trigger-sync to discover columns)"
            click.echo(f"  {table:<{max_tbl}}  [{pk_str}]  {col_str}")
        click.echo("")

        if has_columns:
            print_success("Columns populated from existing landing zone data.")
            print_info(f"Step 1: review sources/{name}/source_metadata.yml  (confirm primary_key)")
            print_info(f"Step 2: platform sync {name} --generate-models --run-dbt")
        else:
            print_warn("Landing zone empty — columns will be populated after the first Airbyte sync.")
            if not has_pk:
                print_warn("primary_key defaulted to [id] — update source_metadata.yml before generating models.")
            print_info(f"Step 1: platform trigger-sync {name}        (runs Airbyte, populates columns)")
            print_info(f"Step 2: review sources/{name}/source_metadata.yml  (confirm primary_key)")
            print_info(f"Step 3: platform sync {name} --generate-models --run-dbt")

    except Exception as e:
        print_error(f"Failed to add source: {e}")
        sys.exit(1)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers: source_schema.yml writer + column population
# ──────────────────────────────────────────────────────────────────────────────


def _write_source_schema(
    source_name: str,
    table_schema: dict,   # {table: {col: {type, nullable}}}
    source_pk_map: dict,  # {table: [pk_col, ...]}  — empty dict is fine
    sources_dir: Path,
) -> None:
    """
    Write (or overwrite) source_schema.yml — the auto-generated column reference.
    Never edited by hand; safe to delete and regenerate at any time.

    Format:
      <source_name>:
        <table>:
          source_defined_primary_key: [...]
          columns:
            <col>: {type: ..., nullable: ...}
    """
    schema: dict = {source_name: {}}
    for table_name, columns in sorted(table_schema.items()):
        schema[source_name][table_name] = {
            "source_defined_primary_key": source_pk_map.get(table_name, []),
            "columns": {
                col: {"type": info.get("type", "string"), "nullable": info.get("nullable", True)}
                for col, info in sorted(columns.items())
            },
        }

    schema_path = sources_dir / "source_schema.yml"
    with open(schema_path, "w") as f:
        yaml.dump(schema, f, default_flow_style=False, sort_keys=False)


def _refresh_source_schema_from_registry(source_name: str, sources_dir: Path) -> None:
    """
    Re-write source_schema.yml using whatever column data the registry already holds.
    Called after trigger-sync (Snowflake types) or regen so the file stays current.
    """
    registry = SchemaRegistry()
    table_rows = registry.get_tables_for_source(source_name)
    if not table_rows:
        return

    table_schema: dict = {}
    for row in table_rows:
        raw = row.get("columns", {})
        cols = json.loads(raw) if isinstance(raw, str) else (raw or {})
        table_schema[row["table_name"]] = cols

    _write_source_schema(source_name, table_schema, {}, sources_dir)


def _populate_columns_from_landing(source_name: str, syncer: "CatalogSyncer") -> None:
    """
    After an Airbyte sync, reads column schemas from GENERIC_AIRBYTE_LANDING and
    writes them into source_metadata.yml as a `_columns:` list under each table.

    This lets the user see all real column names before editing primary_key and
    running dbt. The leading underscore signals "auto-generated — do not hand-edit";
    generators ignore this key.
    """
    print_info("Reading column schemas from Snowflake landing zone...")
    diff = syncer.sync_from_landing(source_name)

    if not diff["all_tables"]:
        print_warn("No tables found in landing zone — nothing to update.")
        return

    # Pull full column detail from registry (sync_from_landing already wrote them there)
    registry = SchemaRegistry()
    table_rows = registry.get_tables_for_source(source_name)

    # Build {table_name: [col, ...]} sorted alphabetically
    col_map: dict[str, list[str]] = {}
    for row in table_rows:
        raw = row.get("columns", {})
        cols = json.loads(raw) if isinstance(raw, str) else (raw or {})
        col_map[row["table_name"]] = sorted(cols.keys())

    # Update source_metadata.yml
    metadata_path = Path("sources") / source_name / "source_metadata.yml"
    if not metadata_path.exists():
        print_warn(f"{metadata_path} not found — run platform add-source first.")
        return

    with open(metadata_path) as f:
        metadata = yaml.safe_load(f) or {}

    source_meta = metadata.setdefault(source_name, {})
    for table_name, col_names in col_map.items():
        if table_name not in source_meta:
            # New table discovered after initial add-source
            source_meta[table_name] = {
                "primary_key": ["id"],
                "load_strategy": "incremental",
                "business_owner": f"{source_name}-team",
                "description": f"Table: {table_name}",
            }
        source_meta[table_name]["_columns"] = col_names

    with open(metadata_path, "w") as f:
        yaml.dump(metadata, f, default_flow_style=False, sort_keys=False)

    print_success(f"Updated {metadata_path} with discovered columns")

    # Refresh source_schema.yml with the accurate Snowflake types from landing
    sources_dir = Path("sources") / source_name
    _refresh_source_schema_from_registry(source_name, sources_dir)
    print_success(f"Updated sources/{source_name}/source_schema.yml with Snowflake types")

    # Print a readable column summary
    print_header("Discovered columns per table")
    max_name = max((len(t) for t in col_map), default=10)
    for table_name, col_names in sorted(col_map.items()):
        click.echo(f"  {table_name:<{max_name}}  {', '.join(col_names)}")

    click.echo("")
    print_warn("Set primary_key for each table in source_metadata.yml, then run:")
    print_info(f"  platform sync {source_name} --generate-models --run-dbt")


# ──────────────────────────────────────────────────────────────────────────────
# Command: trigger-sync
# ──────────────────────────────────────────────────────────────────────────────


@cli.command()
@click.argument("source_name")
@click.option("--no-wait", is_flag=True, help="Fire and forget — don't wait for completion")
def trigger_sync(source_name: str, no_wait: bool):
    """
    Trigger the Airbyte sync for a source and wait for it to finish.

    Example:
      platform trigger-sync fakedata
      platform trigger-sync fakedata --no-wait
    """
    try:
        print_header(f"Triggering Airbyte sync: {source_name}")

        registry = SchemaRegistry()
        sources = registry.get_active_sources()
        source = next((s for s in sources if s["source_name"] == source_name), None)

        if not source:
            print_error(f"Source '{source_name}' not found. Run: platform add-source first.")
            sys.exit(1)

        conn_id = source["airbyte_connection_id"]
        syncer = CatalogSyncer()
        syncer.trigger_sync(conn_id, wait=not no_wait)

        if not no_wait:
            print_success("Airbyte sync complete")
            _populate_columns_from_landing(source_name, syncer)

    except Exception as e:
        print_error(f"Trigger failed: {e}")
        sys.exit(1)


# ──────────────────────────────────────────────────────────────────────────────
# Command: sync
# ──────────────────────────────────────────────────────────────────────────────


@cli.command()
@click.argument("source_name")
@click.option("--generate-models", is_flag=True, help="Force regenerate models")
@click.option("--run-dbt", is_flag=True, help="Run dbt after sync")
def sync(source_name: str, generate_models: bool, run_dbt: bool):
    """
    Manually trigger: Airbyte sync → catalog sync → generate models → dbt.

    Example:
      platform sync stripe --generate-models --run-dbt
    """
    try:
        print_header(f"Syncing source: {source_name}")

        # Step 1: Sync catalog from Snowflake landing zone (Airbyte must have run first)
        print_info("Reading schema from GENERIC_AIRBYTE_LANDING...")
        syncer = CatalogSyncer()
        diff_report = syncer.sync_from_landing(source_name)

        if diff_report["has_changes"]:
            print_info("Schema changes detected!")
            for table in diff_report.get("new_tables", []):
                print_info(f"  New table: {table}")
            for table, cols in diff_report.get("new_columns_by_table", {}).items():
                print_info(f"  New columns in {table}: {', '.join(cols)}")

        # Step 2: Generate models (if needed)
        if generate_models or diff_report["has_changes"]:
            print_header("Generating models")
            if generate_models and not diff_report["has_changes"]:
                # Force regen even with no schema changes (e.g. after deleting model files)
                files = (
                    BronzeGenerator().generate_source(source_name)
                    + SilverGenerator().generate_source(source_name)
                )
                print_success(f"Generated {len(files)} files")
            else:
                handler = EvolutionHandler()
                summary = handler.handle_changes(source_name, diff_report)
                if summary["regenerated_models"]:
                    print_success(f"Generated {len(summary['regenerated_models'])} files")

        # Step 3: Run dbt (if requested)
        if run_dbt or generate_models:
            print_header("Running dbt")

            dbt_base_args = [
                "--project-dir", str(DBT_DIR),
                "--profiles-dir", str(DBT_DIR),
            ]

            # Ensure Elementary's internal tables exist before running source models.
            # Elementary is incremental — this is a no-op after first initialization.
            elem = subprocess.run(
                ["dbt", "run", "--select", "elementary", "--quiet", *dbt_base_args],
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
            )
            if elem.returncode != 0:
                print_warn("Elementary initialisation failed — data quality tracking may be unavailable")

            result = subprocess.run(
                ["dbt", "run", "--select", f"tag:{source_name}", *dbt_base_args],
                cwd=str(PROJECT_ROOT),
            )

            if result.returncode == 0:
                print_success("dbt run completed")
            else:
                print_error("dbt run failed")
                sys.exit(1)

            # Run tests
            print_info("Running dbt tests...")
            result = subprocess.run(
                ["dbt", "test", "--select", f"tag:{source_name}", *dbt_base_args],
                cwd=str(PROJECT_ROOT),
            )

            if result.returncode == 0:
                print_success("dbt tests passed")
            else:
                print_warn("Some dbt tests failed")

        print_header("Sync complete!")

    except Exception as e:
        print_error(f"Sync failed: {e}")
        sys.exit(1)


# ──────────────────────────────────────────────────────────────────────────────
# Command: status
# ──────────────────────────────────────────────────────────────────────────────


@cli.command()
def status():
    """
    Show all registered sources and their health.

    Example:
      platform status
    """
    try:
        print_header("Platform Status")

        registry = SchemaRegistry()
        sources = registry.get_active_sources()

        if not sources:
            print_warn("No active sources registered")
            print_info("Register your first source: platform add-source --name <source_name> --airbyte-connection-id <id>")
            return

        click.echo(f"\n{'Source':<20} {'Tables':<10} {'Last Synced':<20} {'Owner':<20}")
        click.echo("-" * 70)

        for source in sources:
            source_name = source["source_name"]
            tables = registry.get_tables_for_source(source_name)
            last_synced = source.get("last_synced_at", "Never")
            owner = source.get("business_owner", "Unassigned")

            click.echo(
                f"{source_name:<20} {len(tables):<10} {str(last_synced):<20} {owner:<20}"
            )

        print_header("Schema Registry")
        total_tables = sum(len(registry.get_tables_for_source(s["source_name"])) for s in sources)
        print_info(f"Total sources: {len(sources)}")
        print_info(f"Total tables: {total_tables}")

    except Exception as e:
        print_error(f"Status check failed: {e}")
        sys.exit(1)


# ──────────────────────────────────────────────────────────────────────────────
# Command: regen
# ──────────────────────────────────────────────────────────────────────────────


@cli.command()
@click.argument("source_name")
@click.option("--layer", type=click.Choice(["bronze", "silver", "all"]), default="all")
def regen(source_name: str, layer: str):
    """
    Force regenerate dbt models for a source.

    Useful if you manually edited source_metadata.yml or need to fix something.

    Example:
      platform regen stripe --layer silver
    """
    try:
        print_header(f"Regenerating {layer} models for {source_name}")

        if layer in ["bronze", "all"]:
            print_info("Generating bronze models...")
            gen = BronzeGenerator()
            files = gen.generate_source(source_name)
            print_success(f"Generated {len(files)} bronze files")

        if layer in ["silver", "all"]:
            print_info("Generating silver models...")
            gen = SilverGenerator()
            files = gen.generate_source(source_name)
            print_success(f"Generated {len(files)} silver files")

        # Refresh source_schema.yml so types stay current with the registry
        sources_dir = Path("sources") / source_name
        if sources_dir.exists():
            _refresh_source_schema_from_registry(source_name, sources_dir)
            print_success(f"Refreshed sources/{source_name}/source_schema.yml")

        print_success("Regeneration complete!")

    except Exception as e:
        print_error(f"Regeneration failed: {e}")
        sys.exit(1)


# ──────────────────────────────────────────────────────────────────────────────
# Command: deprecate
# ──────────────────────────────────────────────────────────────────────────────


@cli.command()
@click.argument("source_name")
@click.argument("table_name")
def deprecate(source_name: str, table_name: str):
    """
    Soft-retire a table (keep in models with null fills, mark inactive).

    Example:
      platform deprecate stripe invoices_v1
    """
    try:
        print_header(f"Deprecating {source_name}.{table_name}")

        registry = SchemaRegistry()

        # Mark inactive in registry
        sql = """
            UPDATE GENERIC_PLATFORM.PUBLIC.schema_registry
            SET is_active = FALSE
            WHERE source_name = %s AND table_name = %s
        """
        registry._execute(sql, (source_name, table_name))

        # Regenerate models to add null fills
        print_info("Regenerating models with deprecation markers...")
        gen_bronze = BronzeGenerator()
        gen_silver = SilverGenerator()

        gen_bronze.generate_source(source_name)
        gen_silver.generate_source(source_name)

        print_success(f"Deprecated {source_name}.{table_name}")
        print_info("Models updated with null fills for deprecated table")

    except Exception as e:
        print_error(f"Deprecation failed: {e}")
        sys.exit(1)


# ──────────────────────────────────────────────────────────────────────────────
# Command: validate
# ──────────────────────────────────────────────────────────────────────────────


@cli.command()
def validate():
    """
    Validate all platform models: dbt compile + test.

    Example:
      platform validate
    """
    try:
        print_header("Validating platform")

        # dbt compile
        print_info("Running dbt compile...")
        result = subprocess.run(
            ["dbt", "compile", "--project-dir", "dbt_project", "--profiles-dir", "dbt_project"],
            cwd=".",
        )

        if result.returncode != 0:
            print_error("dbt compile failed")
            sys.exit(1)

        print_success("dbt compile passed")

        # dbt test
        print_info("Running dbt tests...")
        result = subprocess.run(
            ["dbt", "test", "--project-dir", "dbt_project", "--profiles-dir", "dbt_project"],
            cwd=".",
        )

        if result.returncode != 0:
            print_warn("Some dbt tests failed")
        else:
            print_success("All dbt tests passed")

    except Exception as e:
        print_error(f"Validation failed: {e}")
        sys.exit(1)


# ──────────────────────────────────────────────────────────────────────────────
# Command: docs
# ──────────────────────────────────────────────────────────────────────────────


@cli.command()
def docs():
    """
    Generate and serve dbt documentation.

    Opens http://localhost:8001 in your browser.

    Example:
      platform docs
    """
    try:
        print_header("Generating dbt documentation")

        # dbt docs generate
        print_info("Generating docs...")
        result = subprocess.run(
            ["dbt", "docs", "generate", "--project-dir", "dbt_project", "--profiles-dir", "dbt_project"],
            cwd=".",
        )

        if result.returncode != 0:
            print_error("dbt docs generate failed")
            sys.exit(1)

        print_success("Docs generated")

        # dbt docs serve
        print_info("Starting docs server on http://localhost:8001")
        print_info("Press Ctrl+C to stop")

        subprocess.run(
            ["dbt", "docs", "serve", "--project-dir", "dbt_project", "--profiles-dir", "dbt_project", "--port", "8001"],
            cwd=".",
        )

    except KeyboardInterrupt:
        print_info("Docs server stopped")
    except Exception as e:
        print_error(f"Docs command failed: {e}")
        sys.exit(1)


# ──────────────────────────────────────────────────────────────────────────────
# Command: prune
# ──────────────────────────────────────────────────────────────────────────────


@cli.command()
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@click.option("--dry-run", is_flag=True, help="Show what would be deleted without deleting anything")
def prune(yes: bool, dry_run: bool):
    """
    Reset the local platform to a clean slate.

    Removes all generated artefacts so you can start fresh with new sources:
      - sources/<name>/           source_metadata.yml, source_schema.yml
      - dbt_project/models/bronze/<name>/
      - dbt_project/models/silver/<name>/
      - dbt_project/target/       compiled SQL, manifest, run results
      - airflow/logs/             scheduler and DAG logs

    Examples:
      platform prune
      platform prune --yes
      platform prune --dry-run
    """
    import shutil

    root = PROJECT_ROOT

    # ── Collect what will be removed ─────────────────────────────────────────
    targets: list[tuple[str, Path]] = []

    sources_root = root / "sources"
    for child in sorted(sources_root.iterdir()):
        if child.is_dir():
            targets.append(("source dir", child))

    bronze_root = root / "dbt_project" / "models" / "bronze"
    for child in sorted(bronze_root.iterdir()):
        if child.is_dir():
            targets.append(("bronze models", child))

    silver_root = root / "dbt_project" / "models" / "silver"
    for child in sorted(silver_root.iterdir()):
        if child.is_dir():
            targets.append(("silver models", child))

    dbt_target = root / "dbt_project" / "target"
    if dbt_target.exists():
        targets.append(("dbt target", dbt_target))

    airflow_logs = root / "airflow" / "logs"
    if airflow_logs.exists():
        for child in sorted(airflow_logs.iterdir()):
            targets.append(("airflow logs", child))

    # ── Print preview ─────────────────────────────────────────────────────────
    if not targets:
        print_info("Nothing to prune — platform is already clean.")
        return

    print_header("Will remove")
    for label, path in targets:
        rel = path.relative_to(root)
        click.echo(f"  [{label:<14}]  {rel}{'/' if path.is_dir() else ''}")

    if dry_run:
        click.echo("")
        print_info("Dry run — nothing deleted.")
        return

    # ── Confirm ───────────────────────────────────────────────────────────────
    click.echo("")
    if not yes:
        click.confirm("  This cannot be undone. Proceed?", default=False, abort=True)

    # ── Delete ────────────────────────────────────────────────────────────────
    print_header("Pruning")
    deleted_dirs = deleted_files = 0

    for _, path in targets:
        if not path.exists():
            continue
        try:
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
                deleted_dirs += 1
            else:
                path.unlink(missing_ok=True)
                deleted_files += 1
        except Exception as e:
            print_warn(f"Could not remove {path.relative_to(root)}: {e}")

    # Re-create empty directories and restore .gitkeep so the structure stays in git
    for directory in [sources_root, bronze_root, silver_root]:
        directory.mkdir(parents=True, exist_ok=True)
        gitkeep = directory / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.touch()

    print_success(f"Removed {deleted_dirs} directories and {deleted_files} files")
    print_info("Re-add sources with: platform add-source --name <name> --airbyte-connection-id <id>")


# ──────────────────────────────────────────────────────────────────────────────
# Command: init (hidden but useful)
# ──────────────────────────────────────────────────────────────────────────────


@cli.command(hidden=True)
def init():
    """Initialize Snowflake schema (runs init_snowflake.py)."""
    try:
        print_header("Initializing Snowflake")

        result = subprocess.run(
            ["python", "sgdp/scripts/init_snowflake.py"],
            cwd=".",
        )

        if result.returncode == 0:
            print_success("Snowflake initialized")
        else:
            print_error("Initialization failed")
            sys.exit(1)

    except Exception as e:
        print_error(f"Init failed: {e}")
        sys.exit(1)


# ──────────────────────────────────────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────────────────────────────────────


def main():
    """Main entry point."""
    try:
        cli()
    except KeyboardInterrupt:
        print_info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
