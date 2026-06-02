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

from platform.catalog_syncer import CatalogSyncer
from platform.evolution_handler import EvolutionHandler
from platform.generators.bronze_generator import BronzeGenerator
from platform.generators.silver_generator import SilverGenerator
from platform.registry import SchemaRegistry

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

        # Create source_metadata.yml stub
        sources_dir = Path("sources") / name
        sources_dir.mkdir(parents=True, exist_ok=True)

        metadata_path = sources_dir / "source_metadata.yml"

        # Fetch schema from Airbyte to populate stub
        print_info("Fetching schema from Airbyte...")
        syncer = CatalogSyncer()
        diff_report = syncer.sync_source(name)

        # Create stub with discovered tables
        stub = {
            name: {
                table: {
                    "primary_key": ["id"],  # Default guess
                    "load_strategy": "incremental",
                    "business_owner": owner or f"{name}-team",
                    "description": f"Table: {table}",
                }
                for table in diff_report.get("all_tables", [])
            }
        }

        with open(metadata_path, "w") as f:
            yaml.dump(stub, f, default_flow_style=False, sort_keys=False)

        print_success(f"Created {metadata_path}")
        print_warn("⚠️  Review and confirm primary_keys in the file!")
        print_info(f"Then run: platform sync {name}")

    except Exception as e:
        print_error(f"Failed to add source: {e}")
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

        # Step 1: Sync catalog
        print_info("Syncing catalog from Airbyte...")
        syncer = CatalogSyncer()
        diff_report = syncer.sync_source(source_name)

        if diff_report["has_changes"]:
            print_info("Schema changes detected!")
            for table in diff_report.get("new_tables", []):
                print_info(f"  New table: {table}")
            for table, cols in diff_report.get("new_columns_by_table", {}).items():
                print_info(f"  New columns in {table}: {', '.join(cols)}")

        # Step 2: Generate models (if needed)
        if generate_models or diff_report["has_changes"]:
            print_header("Generating models")
            handler = EvolutionHandler()
            summary = handler.handle_changes(source_name, diff_report)

            if summary["regenerated_models"]:
                print_success(f"Generated {len(summary['regenerated_models'])} files")

        # Step 3: Run dbt (if requested)
        if run_dbt or generate_models:
            print_header("Running dbt")
            result = subprocess.run(
                [
                    "dbt",
                    "run",
                    "--select",
                    f"tag:{source_name}",
                    "--project-dir",
                    "dbt_project",
                ],
                cwd=".",
            )

            if result.returncode == 0:
                print_success("dbt run completed")
            else:
                print_error("dbt run failed")
                sys.exit(1)

            # Run tests
            print_info("Running dbt tests...")
            result = subprocess.run(
                [
                    "dbt",
                    "test",
                    "--select",
                    f"tag:{source_name}",
                    "--project-dir",
                    "dbt_project",
                ],
                cwd=".",
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
            ["dbt", "compile", "--project-dir", "dbt_project"],
            cwd=".",
        )

        if result.returncode != 0:
            print_error("dbt compile failed")
            sys.exit(1)

        print_success("dbt compile passed")

        # dbt test
        print_info("Running dbt tests...")
        result = subprocess.run(
            ["dbt", "test", "--project-dir", "dbt_project"],
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
            ["dbt", "docs", "generate", "--project-dir", "dbt_project"],
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
            ["dbt", "docs", "serve", "--project-dir", "dbt_project", "--port", "8001"],
            cwd=".",
        )

    except KeyboardInterrupt:
        print_info("Docs server stopped")
    except Exception as e:
        print_error(f"Docs command failed: {e}")
        sys.exit(1)


# ──────────────────────────────────────────────────────────────────────────────
# Command: init (hidden but useful)
# ──────────────────────────────────────────────────────────────────────────────


@cli.command(hidden=True)
def init():
    """Initialize Snowflake schema (runs init_snowflake.py)."""
    try:
        print_header("Initializing Snowflake")

        result = subprocess.run(
            ["python", "platform/scripts/init_snowflake.py"],
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
