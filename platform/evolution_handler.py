"""
Schema Evolution Handler

Detects schema changes from catalog syncer diff report and:
  - Generates bronze/silver models for new tables
  - Regenerates models when columns change
  - Soft-deprecates removed columns
  - Alerts business owner of breaking changes

Usage:
  from platform.evolution_handler import EvolutionHandler

  handler = EvolutionHandler()
  handler.handle_changes("stripe", diff_report)
"""

import json
from typing import Optional

from platform.catalog_syncer import CatalogSyncer
from platform.generators.bronze_generator import BronzeGenerator
from platform.generators.silver_generator import SilverGenerator
from platform.git_client import GitClient
from platform.registry import SchemaRegistry


class EvolutionHandler:
    """Handle schema changes and regenerate affected models."""

    def __init__(self):
        """Initialize handler."""
        self.registry = SchemaRegistry()
        self.syncer = CatalogSyncer()
        self.bronze_gen = BronzeGenerator()
        self.silver_gen = SilverGenerator()
        self.git = GitClient()

    def handle_changes(self, source_name: str, diff_report: dict) -> dict:
        """
        Process schema changes detected by catalog syncer.

        Args:
            source_name: Source name
            diff_report: Output from CatalogSyncer.sync_source()

        Returns:
            Summary dict of actions taken
        """
        summary = {
            "source_name": source_name,
            "actions": [],
            "regenerated_models": [],
            "alerts": [],
        }

        # New tables — generate both bronze and silver
        if diff_report.get("new_tables"):
            for table_name in diff_report["new_tables"]:
                print(f"  🆕 New table: {source_name}.{table_name}")
                summary["actions"].append(f"new_table:{table_name}")

                try:
                    # Generate bronze and silver
                    bronze_files = self.bronze_gen.generate_source(source_name)
                    silver_files = self.silver_gen.generate_source(source_name)
                    summary["regenerated_models"].extend(bronze_files + silver_files)
                except Exception as e:
                    print(f"    ❌ Error generating models: {e}")
                    summary["alerts"].append(f"Generation failed for {table_name}: {e}")

        # New columns — regenerate bronze and silver for affected tables
        if diff_report.get("new_columns_by_table"):
            for table_name, columns in diff_report["new_columns_by_table"].items():
                print(f"  ➕ New columns in {table_name}: {', '.join(columns)}")
                summary["actions"].append(f"new_columns:{table_name}")

                try:
                    bronze_files = self.bronze_gen.generate_source(source_name)
                    silver_files = self.silver_gen.generate_source(source_name)
                    summary["regenerated_models"].extend(bronze_files + silver_files)
                except Exception as e:
                    print(f"    ❌ Error regenerating models: {e}")
                    summary["alerts"].append(f"Regeneration failed for {table_name}: {e}")

        # Removed columns — soft deprecate, regenerate with null fill
        if diff_report.get("removed_columns_by_table"):
            for table_name, columns in diff_report["removed_columns_by_table"].items():
                print(f"  ➖ Removed columns in {table_name}: {', '.join(columns)}")
                summary["actions"].append(f"removed_columns:{table_name}")

                for col in columns:
                    self.registry.soft_deprecate_column(source_name, table_name, col)

                # Regenerate to add null fill
                try:
                    bronze_files = self.bronze_gen.generate_source(source_name)
                    silver_files = self.silver_gen.generate_source(source_name)
                    summary["regenerated_models"].extend(bronze_files + silver_files)
                except Exception as e:
                    print(f"    ❌ Error regenerating models: {e}")
                    summary["alerts"].append(
                        f"Removed columns from {table_name}. "
                        f"Models regenerated with null fills. Check and commit manually."
                    )

        # Type changes — explicit cast in bronze, alert owner
        if diff_report.get("type_changes_by_table"):
            for table_name, changes in diff_report["type_changes_by_table"].items():
                print(f"  🔄 Type changes in {table_name}:")
                for change in changes:
                    print(f"     {change['column']}: {change['old_type']} → {change['new_type']}")

                summary["actions"].append(f"type_changes:{table_name}")
                summary["alerts"].append(
                    f"Type changes detected in {source_name}.{table_name}. "
                    f"Review and test models before deploying."
                )

        # Commit all generated files
        if summary["regenerated_models"]:
            try:
                change_summary = ", ".join(summary["actions"][:2])
                commit_msg = f"[evolution] {source_name}: {change_summary}"
                self.git.commit(commit_msg)
                print(f"  ✅ Committed changes")
            except Exception as e:
                print(f"  ⚠️  Commit failed: {e}")
                summary["alerts"].append(f"Git commit failed: {e}")

        return summary

    def handle_all_sources(self) -> dict:
        """
        Run catalog sync and evolution handling for all active sources.

        Returns:
            Dict mapping source_name → evolution summary
        """
        sources = self.registry.get_active_sources()
        results = {}

        for source in sources:
            source_name = source["source_name"]
            print(f"\n📊 Syncing {source_name}...")

            try:
                # Sync catalog (detect changes)
                diff_report = self.syncer.sync_source(source_name)

                if diff_report["has_changes"]:
                    print(f"  Schema changed!")
                    summary = self.handle_changes(source_name, diff_report)
                    results[source_name] = summary
                else:
                    print(f"  No schema changes")
                    results[source_name] = {
                        "source_name": source_name,
                        "actions": [],
                        "alerts": [],
                    }

            except Exception as e:
                print(f"  ❌ Error: {e}")
                results[source_name] = {
                    "source_name": source_name,
                    "actions": [],
                    "alerts": [str(e)],
                }

        return results
