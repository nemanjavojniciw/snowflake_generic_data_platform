"""
Bronze Model Generator

Generates thin select-from-raw dbt models for all tables in a source.
Also generates sources.yml and schema.yml with auto-generated tests.

Bronze is purely structural — renames Airbyte metadata columns, passes through all source columns.

Usage:
  from platform.generators.bronze_generator import BronzeGenerator

  gen = BronzeGenerator()
  files = gen.generate_source("stripe")
  print(f"Generated {len(files)} files")
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

from jinja2 import Environment, FileSystemLoader, TemplateNotFound

from sgdp.git_client import GitClient
from sgdp.registry import SchemaRegistry


class BronzeGenerator:
    """Generate bronze dbt models and metadata."""

    def __init__(self, repo_path: Optional[str] = None):
        """
        Initialize generator.

        Args:
            repo_path: Path to dbt + git repo. If None, uses current directory.
        """
        self.repo_path = Path(repo_path or ".")
        self.templates_dir = Path(__file__).parent.parent / "templates"
        self.registry = SchemaRegistry()
        self.git = GitClient(str(self.repo_path))

        # Custom delimiters so dbt's {{ }} and {% %} pass through untouched
        self.env = Environment(
            loader=FileSystemLoader(str(self.templates_dir)),
            variable_start_string="[[",
            variable_end_string="]]",
            block_start_string="[%",
            block_end_string="%]",
            comment_start_string="[#",
            comment_end_string="#]",
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def _load_template(self, filename: str):
        """Load a Jinja2 template."""
        try:
            return self.env.get_template(filename)
        except TemplateNotFound:
            raise RuntimeError(f"Template not found: {filename} (looked in {self.templates_dir})")

    def generate_source(self, source_name: str) -> list[str]:
        """
        Generate all bronze models for a source.

        Args:
            source_name: Source name (e.g. 'stripe')

        Returns:
            List of generated file paths
        """
        tables = self.registry.get_tables_for_source(source_name)

        if not tables:
            raise ValueError(f"No tables found for source: {source_name}")

        generated_files = []
        now = datetime.utcnow().isoformat() + "Z"

        # Generate individual model files
        for table in tables:
            table_name = table["table_name"]

            # Model file
            model_file = self._generate_model(source_name, table, now)
            generated_files.append(model_file)

        # Generate sources.yml (all tables for this source)
        sources_file = self._generate_sources_yml(source_name, tables, now)
        generated_files.append(sources_file)

        # Generate schema.yml (all tables for this source)
        schema_file = self._generate_schema_yml(source_name, tables, now)
        generated_files.append(schema_file)

        return generated_files

    def _generate_model(self, source_name: str, table: dict, generated_at: str) -> str:
        """Generate a single bronze model file."""
        table_name = table["table_name"]
        columns = table["columns"]
        if isinstance(columns, str):
            import json
            columns = json.loads(columns)

        schema_hash = table.get("schema_hash", "unknown")

        template = self._load_template("bronze_model.sql.j2")
        sql = template.render(
            source_name=source_name,
            table_name=table_name,
            columns=columns,
            schema_hash=schema_hash,
            generated_at=generated_at,
        )

        # Write to dbt_project/models/bronze/
        model_dir = f"dbt_project/models/bronze/{source_name}"
        model_path = f"{model_dir}/bronze_{source_name}_{table_name}.sql"

        self.git.write_file(model_path, sql)

        print(f"  ✅ {model_path}")
        return model_path

    def _generate_sources_yml(self, source_name: str, tables: list[dict], generated_at: str) -> str:
        """Generate sources.yml for all tables in source."""
        import json

        # Normalize columns for template
        for table in tables:
            if isinstance(table["columns"], str):
                table["columns"] = json.loads(table["columns"])
            if isinstance(table["primary_keys"], str):
                table["primary_keys"] = json.loads(table["primary_keys"])

        template = self._load_template("sources.yml.j2")
        yml = template.render(
            source_name=source_name,
            tables=tables,
            generated_at=generated_at,
        )

        # Write to dbt_project/models/bronze/
        yml_path = f"dbt_project/models/bronze/{source_name}/sources.yml"
        self.git.write_file(yml_path, yml)

        print(f"  ✅ {yml_path}")
        return yml_path

    def _generate_schema_yml(self, source_name: str, tables: list[dict], generated_at: str) -> str:
        """Generate schema.yml with tests for bronze models."""
        import json

        # Normalize columns for template
        for table in tables:
            if isinstance(table["columns"], str):
                table["columns"] = json.loads(table["columns"])
            if isinstance(table["primary_keys"], str):
                table["primary_keys"] = json.loads(table["primary_keys"])

        template = self._load_template("bronze_schema.yml.j2")
        yml = template.render(
            source_name=source_name,
            tables=tables,
            generated_at=generated_at,
        )

        # Write to dbt_project/models/bronze/
        schema_path = f"dbt_project/models/bronze/{source_name}/schema.yml"
        self.git.write_file(schema_path, yml)

        print(f"  ✅ {schema_path}")
        return schema_path
