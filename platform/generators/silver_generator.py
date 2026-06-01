"""
Silver Model Generator

Generates business-ready dbt models with load strategy patterns:
  - incremental: append-mode with deduplication
  - full_refresh: replace entire table on each run
  - scd2: snapshot-based slowly-changing dimensions

Uses source_metadata.yml to determine strategy per table.

Usage:
  from platform.generators.silver_generator import SilverGenerator

  gen = SilverGenerator()
  files = gen.generate_source("stripe")
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml
from jinja2 import Environment, FileSystemLoader, TemplateNotFound

from platform.git_client import GitClient
from platform.registry import SchemaRegistry


class SilverGenerator:
    """Generate silver dbt models with configurable load strategies."""

    # Template path per load strategy
    STRATEGY_TEMPLATES = {
        "incremental": "silver_incremental.sql.j2",
        "full_refresh": "silver_full_refresh.sql.j2",
        "scd2": "silver_scd2.sql.j2",
    }

    def __init__(self, repo_path: Optional[str] = None):
        """
        Initialize generator.

        Args:
            repo_path: Path to dbt + git repo. If None, uses current directory.
        """
        self.repo_path = Path(repo_path or ".")
        self.templates_dir = self.repo_path / "platform" / "templates"
        self.sources_dir = self.repo_path / "sources"
        self.registry = SchemaRegistry()
        self.git = GitClient(str(self.repo_path))

        # Jinja2 environment
        self.env = Environment(
            loader=FileSystemLoader(str(self.templates_dir)),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def _load_template(self, filename: str):
        """Load a Jinja2 template."""
        try:
            return self.env.get_template(filename)
        except TemplateNotFound:
            raise RuntimeError(f"Template not found: {filename} (looked in {self.templates_dir})")

    def _load_source_metadata(self, source_name: str) -> dict:
        """
        Load source_metadata.yml for a source.

        Returns:
            Dict of {table_name: {primary_key, load_strategy, ...}}
        """
        metadata_path = self.sources_dir / source_name / "source_metadata.yml"

        if not metadata_path.exists():
            return {}

        with open(metadata_path) as f:
            data = yaml.safe_load(f) or {}

        # Return just this source's metadata
        return data.get(source_name, {})

    def generate_source(self, source_name: str) -> list[str]:
        """
        Generate all silver models for a source.

        Args:
            source_name: Source name (e.g. 'stripe')

        Returns:
            List of generated file paths
        """
        tables = self.registry.get_tables_for_source(source_name)
        if not tables:
            raise ValueError(f"No tables found for source: {source_name}")

        metadata = self._load_source_metadata(source_name)
        generated_files = []
        now = datetime.utcnow().isoformat() + "Z"

        for table in tables:
            table_name = table["table_name"]
            table_meta = metadata.get(table_name, {})

            # Determine load strategy (default: incremental)
            strategy = table_meta.get("load_strategy", "incremental")
            if strategy not in self.STRATEGY_TEMPLATES:
                print(f"  ⚠️  Unknown strategy '{strategy}' for {table_name}, defaulting to 'incremental'")
                strategy = "incremental"

            model_file = self._generate_model(
                source_name=source_name,
                table=table,
                strategy=strategy,
                table_meta=table_meta,
                generated_at=now,
            )
            generated_files.append(model_file)

        # Generate schema.yml with tests
        schema_file = self._generate_schema_yml(source_name, tables, metadata, now)
        generated_files.append(schema_file)

        return generated_files

    def _generate_model(
        self,
        source_name: str,
        table: dict,
        strategy: str,
        table_meta: dict,
        generated_at: str,
    ) -> str:
        """Generate a single silver model file."""
        table_name = table["table_name"]
        columns = table["columns"]
        if isinstance(columns, str):
            columns = json.loads(columns)

        primary_keys = table_meta.get("primary_key", [])
        tracked_columns = table_meta.get("scd2_columns", [])

        template_name = self.STRATEGY_TEMPLATES[strategy]
        template = self._load_template(template_name)

        sql = template.render(
            source_name=source_name,
            table_name=table_name,
            columns=columns,
            primary_keys=primary_keys,
            tracked_columns=tracked_columns,
            generated_at=generated_at,
        )

        # Write to dbt_project/models/silver/
        model_dir = f"dbt_project/models/silver/{source_name}"
        model_path = f"{model_dir}/{table_name}.sql"

        self.git.write_file(model_path, sql)
        print(f"  ✅ {model_path} ({strategy})")
        return model_path

    def _generate_schema_yml(
        self,
        source_name: str,
        tables: list[dict],
        metadata: dict,
        generated_at: str,
    ) -> str:
        """Generate schema.yml with tests for silver models."""
        yml_content = f"""# GENERATED FILE — DO NOT EDIT MANUALLY
# source: {source_name} | generated_at: {generated_at}

version: 2

models:
"""

        for table in tables:
            table_name = table["table_name"]
            table_meta = metadata.get(table_name, {})
            columns = table["columns"]
            if isinstance(columns, str):
                columns = json.loads(columns)

            primary_keys = table_meta.get("primary_key", [])
            strategy = table_meta.get("load_strategy", "incremental")

            yml_content += f"""
  - name: {source_name}__{table_name}
    description: "Silver layer for {source_name}.{table_name} ({strategy}) — auto-generated by platform"
    columns:
"""

            # Add PK and metadata tests
            for pk in primary_keys:
                yml_content += f"""      - name: {pk}
        tests: [not_null, unique]
"""

            yml_content += """      - name: _raw_id
        tests: [unique, not_null]
      - name: _extracted_at
        tests: [not_null]
      - name: _loaded_at
        tests: [not_null]
      - name: _platform_updated_at
        tests: [not_null]
"""

            # Add other columns
            for col_name in columns:
                if col_name not in [
                    "_airbyte_raw_id",
                    "_airbyte_extracted_at",
                    "_airbyte_loaded_at",
                    "_airbyte_data",
                    "_bronze_created_at",
                    "_raw_id",
                    "_extracted_at",
                    "_loaded_at",
                    "_platform_updated_at",
                ] + primary_keys:
                    yml_content += f"      - name: {col_name}\n"

        schema_path = f"dbt_project/models/silver/{source_name}/schema.yml"
        self.git.write_file(schema_path, yml_content)
        print(f"  ✅ {schema_path}")
        return schema_path
