"""
Airbyte Catalog Syncer

Polls Airbyte API to fetch all connections and their stream catalogs.
Syncs discovered schemas to the platform registry.

Runs as an Airflow task before each source sync to detect schema changes.

Usage:
  from platform.catalog_syncer import CatalogSyncer

  syncer = CatalogSyncer()
  diff_report = syncer.sync_source("stripe")
  print(f"New columns: {diff_report['new_columns']}")
  print(f"Schema changed: {diff_report['has_changes']}")
"""

import json
import os
from typing import Optional

import requests

from platform.registry import SchemaRegistry


class CatalogSyncer:
    """Sync Airbyte catalogs to platform schema registry."""

    def __init__(self):
        """Initialize with Airbyte API credentials from environment."""
        self.api_url = os.getenv("AIRBYTE_API_URL", "http://localhost:8000/api/v1")
        self.username = os.getenv("AIRBYTE_EMAIL")
        self.password = os.getenv("AIRBYTE_PASSWORD")

        if not all([self.username, self.password]):
            raise ValueError(
                "Missing Airbyte credentials. Set: AIRBYTE_EMAIL, AIRBYTE_PASSWORD"
            )

        self.auth = (self.username, self.password)
        self.registry = SchemaRegistry()

    def _get(self, endpoint: str, **kwargs) -> dict:
        """GET request to Airbyte API."""
        url = f"{self.api_url}{endpoint}"
        resp = requests.get(url, auth=self.auth, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def _post(self, endpoint: str, data: dict, **kwargs) -> dict:
        """POST request to Airbyte API."""
        url = f"{self.api_url}{endpoint}"
        resp = requests.post(url, json=data, auth=self.auth, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def get_connections(self) -> list[dict]:
        """
        Fetch all Airbyte connections.

        Returns:
            List of connection dicts with keys: connectionId, name, sourceId, destinationId, status
        """
        try:
            result = self._get("/connections")
            return result.get("connections", [])
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch Airbyte connections: {e}") from e

    def get_connection_catalog(self, connection_id: str) -> Optional[dict]:
        """
        Fetch the discovered catalog for a connection.

        Args:
            connection_id: Airbyte connection UUID

        Returns:
            Catalog dict with 'streams' array, or None if not found
        """
        try:
            result = self._get(f"/connections/{connection_id}/discover_schema")
            return result.get("catalog")
        except requests.RequestException as e:
            print(f"⚠️  Could not fetch catalog for {connection_id}: {e}")
            return None

    def extract_columns(self, json_schema: dict) -> dict:
        """
        Extract column info from Airbyte's JSON schema.

        Args:
            json_schema: JSON schema from Airbyte stream

        Returns:
            Dict of {column_name: {type, nullable, ...}}
        """
        columns = {}

        properties = json_schema.get("properties", {})
        required = set(json_schema.get("required", []))

        for col_name, col_schema in properties.items():
            col_type = col_schema.get("type", "string")
            nullable = col_name not in required

            columns[col_name] = {
                "type": col_type,
                "nullable": nullable,
            }

        return columns

    def sync_all_connections(self) -> dict:
        """
        Sync all Airbyte connections to registry.

        Returns:
            Summary dict with keys: total_connections, synced_sources, total_tables, changes
        """
        connections = self.get_connections()
        synced_sources = []
        total_tables = 0
        all_changes = {}

        for conn in connections:
            conn_id = conn["connectionId"]
            conn_name = conn["name"]

            # Register the source in source_catalog
            self.registry.register_source(
                source_name=conn_name,
                airbyte_connection_id=conn_id,
            )

            # Fetch and sync catalog
            catalog = self.get_connection_catalog(conn_id)
            if not catalog:
                print(f"  ⚠️  No catalog for {conn_name}")
                continue

            source_changes = self.sync_catalog(conn_name, catalog)
            synced_sources.append(conn_name)
            total_tables += len(catalog.get("streams", []))
            all_changes[conn_name] = source_changes

        return {
            "total_connections": len(connections),
            "synced_sources": len(synced_sources),
            "total_tables": total_tables,
            "sources": synced_sources,
            "changes": all_changes,
        }

    def sync_source(self, source_name: str) -> dict:
        """
        Sync a specific source by name (looks up connection in source_catalog).

        Args:
            source_name: Source name (e.g. 'stripe')

        Returns:
            Diff report with keys: has_changes, new_tables, new_columns, removed_columns, type_changes
        """
        # Look up connection ID from registry
        sources = self.registry.get_active_sources()
        source = next((s for s in sources if s["source_name"] == source_name), None)

        if not source:
            raise ValueError(f"Source '{source_name}' not found in registry")

        conn_id = source["airbyte_connection_id"]

        # Fetch and sync catalog
        catalog = self.get_connection_catalog(conn_id)
        if not catalog:
            raise RuntimeError(f"Could not fetch catalog for {source_name}")

        return self.sync_catalog(source_name, catalog)

    def sync_catalog(self, source_name: str, catalog: dict) -> dict:
        """
        Sync a catalog to registry, return diff report.

        Args:
            source_name: Source name
            catalog: Airbyte catalog dict with 'streams'

        Returns:
            Aggregated diff report across all tables
        """
        streams = catalog.get("streams", [])
        diffs = []

        for stream in streams:
            config = stream.get("config", {})
            stream_name = config.get("name")
            json_schema = config.get("jsonSchema", {})

            if not stream_name or not json_schema:
                continue

            # Extract columns from schema
            columns = self.extract_columns(json_schema)

            # Upsert to registry (detects changes)
            diff = self.registry.upsert_schema_registry(
                source_name=source_name,
                table_name=stream_name,
                columns=columns,
            )
            diffs.append((stream_name, diff))

        # Aggregate diffs
        aggregated = {
            "has_changes": any(d["has_changes"] for _, d in diffs),
            "new_tables": [name for name, d in diffs if d["is_new"]],
            "all_tables": [name for name, _ in diffs],
            "new_columns_by_table": {},
            "removed_columns_by_table": {},
            "type_changes_by_table": {},
        }

        for table_name, diff in diffs:
            if diff["new_columns"]:
                aggregated["new_columns_by_table"][table_name] = diff["new_columns"]
            if diff["removed_columns"]:
                aggregated["removed_columns_by_table"][table_name] = diff["removed_columns"]
            if diff["type_changes"]:
                aggregated["type_changes_by_table"][table_name] = diff["type_changes"]

        return aggregated
