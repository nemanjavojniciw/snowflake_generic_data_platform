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
import time
from typing import Optional

import requests

from sgdp.registry import SchemaRegistry


class CatalogSyncer:
    """Sync Airbyte catalogs to platform schema registry."""

    def __init__(self):
        """Initialize with Airbyte API credentials from environment."""
        self.api_url = os.getenv("AIRBYTE_API_URL", "http://localhost:8000/api/public/v1")
        self.client_id = os.getenv("AIRBYTE_CLIENT_ID")
        self.client_secret = os.getenv("AIRBYTE_CLIENT_SECRET")

        if not all([self.client_id, self.client_secret]):
            raise ValueError(
                "Missing Airbyte credentials. Set: AIRBYTE_CLIENT_ID, AIRBYTE_CLIENT_SECRET"
            )

        self._access_token: Optional[str] = None
        self.registry = SchemaRegistry()

    def _get_token(self) -> str:
        """Fetch an OAuth2 bearer token using client credentials."""
        resp = requests.post(
            f"{self.api_url}/applications/token",
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]

    def _headers(self) -> dict:
        """Return auth headers, refreshing the token if needed."""
        if not self._access_token:
            self._access_token = self._get_token()
        return {"Authorization": f"Bearer {self._access_token}"}

    def _get(self, endpoint: str, **kwargs) -> dict:
        """GET request to Airbyte API."""
        url = f"{self.api_url}{endpoint}"
        resp = requests.get(url, headers=self._headers(), timeout=30, **kwargs)
        if resp.status_code == 401:
            # Token expired — refresh once and retry
            self._access_token = self._get_token()
            resp = requests.get(url, headers=self._headers(), timeout=30, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def _post(self, endpoint: str, data: dict, **kwargs) -> dict:
        """POST request to Airbyte API."""
        url = f"{self.api_url}{endpoint}"
        resp = requests.post(url, json=data, headers=self._headers(), timeout=30, **kwargs)
        if resp.status_code == 401:
            self._access_token = self._get_token()
            resp = requests.post(url, json=data, headers=self._headers(), timeout=30, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def _patch(self, endpoint: str, data: dict, **kwargs) -> dict:
        """PATCH request to Airbyte API."""
        url = f"{self.api_url}{endpoint}"
        resp = requests.patch(url, json=data, headers=self._headers(), timeout=30, **kwargs)
        if resp.status_code == 401:
            self._access_token = self._get_token()
            resp = requests.patch(url, json=data, headers=self._headers(), timeout=30, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def trigger_sync(self, connection_id: str, wait: bool = True, poll_interval: int = 5) -> dict:
        """
        Trigger an Airbyte sync job and optionally poll until it completes.

        Args:
            connection_id: Airbyte connection UUID
            wait: Block until the job finishes (default True)
            poll_interval: Seconds between status polls

        Returns:
            Final job dict with keys: jobId, status, startTime, duration
        """
        resp = self._post("/jobs", {"connectionId": connection_id, "jobType": "sync"})
        job_id = resp["jobId"]
        print(f"  ▶  Job {job_id} started")

        if not wait:
            return resp

        while True:
            time.sleep(poll_interval)
            job = self._get(f"/jobs/{job_id}")
            status = job.get("status", "running")

            if status == "running":
                print(f"  ⏳ {status}...")
            elif status == "succeeded":
                print(f"  ✅ Sync completed (job {job_id})")
                return job
            else:
                raise RuntimeError(f"Airbyte sync failed with status '{status}' (job {job_id})")

    def set_connection_namespace(self, connection_id: str, source_name: str) -> None:
        """
        Patch the Airbyte connection so data lands in GENERIC_AIRBYTE_LANDING.{source_name}.
        Must be called before the first sync so tables are created in the right schema.
        """
        self._patch(
            f"/connections/{connection_id}",
            {
                "namespaceDefinition": "custom_format",
                "namespaceFormat": source_name,
            },
        )

    def get_connections(self) -> list[dict]:
        """
        Fetch all Airbyte connections.

        Returns:
            List of connection dicts with keys: connectionId, name, sourceId, destinationId, status
        """
        try:
            result = self._get("/connections")
            # Platform API v1 returns {"data": [...]}; older internal API returned {"connections": [...]}
            return result.get("data", result.get("connections", []))
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch Airbyte connections: {e}") from e

    @staticmethod
    def _flatten_pk(raw_pk: list) -> list:
        """Flatten Airbyte's list-of-lists PK format: [["id"]] -> ["id"]."""
        return [
            (entry[0] if isinstance(entry, list) and entry else entry)
            for entry in raw_pk
            if entry
        ]

    def get_stream_catalog(self, connection_id: str) -> dict:
        """
        Build a stream catalog without requiring a sync.

        Tries three sources in order, merging the best available info:
          1. GET /streams?sourceId=...  — stream names, sync modes, source-defined PKs
          2. GET /connections/{id}      — configured PKs set by the user in Airbyte UI
          3. GET /connections/{id}      — jsonSchema if present in configurations.streams
                                          (some connector versions include it)

        Column schemas are NOT available via the public API v1 before a sync.
        They are populated later by trigger-sync -> sync_from_landing().

        Returns:
            {stream_name: {columns, source_pk, sync_modes}}  — columns may be empty.
        """
        try:
            conn = self._get(f"/connections/{connection_id}")
        except Exception as e:
            print(f"  ⚠️  Could not fetch connection {connection_id}: {e}")
            return {}

        source_id = conn.get("sourceId")
        if not source_id:
            return {}

        # ── Source 2 & 3: configured streams from the connection ──────────────
        # Build lookup: stream_name -> {primaryKey, jsonSchema}
        config_streams = conn.get("configurations", {}).get("streams", [])
        config_lookup: dict = {}
        for cs in config_streams:
            n = cs.get("name") or cs.get("streamName")
            if not n:
                continue
            raw_pk = cs.get("primaryKey", [])
            json_schema = cs.get("jsonSchema", {})
            config_lookup[n] = {
                "pk": self._flatten_pk(raw_pk) if raw_pk else [],
                "columns": self.extract_columns(json_schema) if json_schema else {},
            }

        # ── Source 1: /streams discovery endpoint ────────────────────────────
        discovery_streams: list = []
        try:
            result = self._get("/streams", params={"sourceId": source_id, "ignoreCache": "false"})
            # Public API v1 returns a bare list; some versions wrap it in {"data": [...]}
            if isinstance(result, list):
                discovery_streams = result
            else:
                discovery_streams = result.get("data", result.get("streams", []))
        except Exception as e:
            print(f"  ⚠️  /streams discovery failed: {e}. Using connection config only.")

        # ── Merge all sources ─────────────────────────────────────────────────
        catalog: dict = {}

        for stream in discovery_streams:
            # Public API v1 uses "streamName"; internal API uses "name"
            name = stream.get("streamName") or stream.get("name")
            if not name:
                continue

            # Prefer source-defined PK; fall back to connection-configured PK
            raw_pk = stream.get("sourceDefinedPrimaryKey", [])
            source_pk = self._flatten_pk(raw_pk)
            if not source_pk:
                source_pk = config_lookup.get(name, {}).get("pk", [])

            # jsonSchema not available from /streams in public API; try config lookup
            json_schema = stream.get("jsonSchema", {})
            columns = self.extract_columns(json_schema) if json_schema else config_lookup.get(name, {}).get("columns", {})

            # Public API uses "syncModes"; internal uses "supportedSyncModes"
            sync_modes = stream.get("syncModes") or stream.get("supportedSyncModes", [])

            catalog[name] = {
                "columns": columns,
                "source_pk": source_pk,
                "sync_modes": sync_modes,
            }

        # If /streams returned nothing, fall back to connection config streams only
        if not catalog and config_lookup:
            for name, info in config_lookup.items():
                catalog[name] = {
                    "columns": info["columns"],
                    "source_pk": info["pk"],
                    "sync_modes": [],
                }

        return catalog

    def get_connection_catalog(self, connection_id: str) -> Optional[dict]:
        """
        Fetch the configured catalog for a connection.

        Uses GET /connections/{connectionId} — the Platform API v1 embeds the
        stream catalog under configurations.streams rather than a separate discover endpoint.

        Args:
            connection_id: Airbyte connection UUID

        Returns:
            Catalog dict with 'streams' array, or None if not found
        """
        try:
            result = self._get(f"/connections/{connection_id}")
            streams = result.get("configurations", {}).get("streams", [])
            return {"streams": streams} if streams else None
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
            # Platform API v1 (flat): {"name": "...", "jsonSchema": {...}, ...}
            # Older internal API (nested): {"config": {"name": "...", "jsonSchema": {...}}}
            if "name" in stream:
                stream_name = stream["name"]
                json_schema = stream.get("jsonSchema", {})
            else:
                config = stream.get("config", {})
                stream_name = config.get("name")
                json_schema = config.get("jsonSchema", {})

            if not stream_name:
                continue

            # jsonSchema is absent from Airbyte public API v1 connection responses —
            # columns are populated later by sync_from_landing() after data arrives.
            columns = self.extract_columns(json_schema) if json_schema else {}

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

    def sync_from_landing(self, source_name: str) -> dict:
        """
        Read column schemas directly from GENERIC_AIRBYTE_LANDING.INFORMATION_SCHEMA
        after Airbyte has loaded data. More reliable than the Airbyte API because the
        public API v1 does not return jsonSchema in connection responses.

        Args:
            source_name: Source name (used as schema name in the landing database)

        Returns:
            Aggregated diff report across all discovered tables
        """
        sql = """
            SELECT table_name, column_name, data_type, is_nullable
            FROM GENERIC_AIRBYTE_LANDING.INFORMATION_SCHEMA.COLUMNS
            WHERE LOWER(table_schema) = LOWER(%s)
              AND LEFT(column_name, 1) != '_'
            ORDER BY table_name, ordinal_position
        """
        cursor = self.registry._execute(sql, (source_name,))
        rows = self.registry._fetchall_as_dicts(cursor)

        if not rows:
            print(f"  ⚠️  No tables found in GENERIC_AIRBYTE_LANDING.{source_name}. "
                  f"Run the Airbyte sync first.")
            return {"has_changes": False, "new_tables": [], "all_tables": [],
                    "new_columns_by_table": {}, "removed_columns_by_table": {}, "type_changes_by_table": {}}

        # Group columns by table
        tables: dict[str, dict] = {}
        for row in rows:
            tbl = row["table_name"].lower()
            col = row["column_name"].lower()
            tables.setdefault(tbl, {})[col] = {
                "type": row["data_type"].lower(),
                "nullable": row["is_nullable"] == "YES",
            }

        diffs = []
        for table_name, columns in tables.items():
            diff = self.registry.upsert_schema_registry(
                source_name=source_name,
                table_name=table_name,
                columns=columns,
            )
            diffs.append((table_name, diff))

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
