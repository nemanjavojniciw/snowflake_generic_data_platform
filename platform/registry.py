"""
Schema Registry Client

Reads and writes platform metadata from/to Snowflake. Single source of truth for:
  - All active sources and their schemas
  - Primary keys, load strategies, and business owners
  - Schema change history (for evolution detection)

All downstream components (catalog syncer, generators, DAG factory) query this registry.

Usage:
  from platform.registry import SchemaRegistry

  registry = SchemaRegistry()
  sources = registry.get_active_sources()
  tables = registry.get_tables_for_source("stripe")
  registry.upsert_schema(...)
"""

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Optional

try:
    import snowflake.connector
    from snowflake.connector import ProgrammingError
except ImportError:
    raise ImportError("snowflake-connector-python not installed. Run: pip install snowflake-connector-python")


class SchemaRegistry:
    """Client for reading/writing schema registry in Snowflake."""

    def __init__(self):
        """Initialize Snowflake connection from environment variables."""
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = os.getenv("SNOWFLAKE_USER")
        self.password = os.getenv("SNOWFLAKE_PASSWORD")
        self.role = os.getenv("SNOWFLAKE_ROLE", "PLATFORM_ADMIN")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "PLATFORM_WH")

        if not all([self.account, self.user, self.password]):
            raise ValueError(
                "Missing Snowflake credentials. Set: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD"
            )

        self.conn = None

    def _connect(self):
        """Establish Snowflake connection (lazy)."""
        if self.conn is None:
            self.conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                role=self.role,
                warehouse=self.warehouse,
            )
        return self.conn

    def _close(self):
        """Close connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def _execute(self, sql: str, params: Optional[dict] = None) -> Any:
        """Execute a single SQL query, return cursor."""
        conn = self._connect()
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor
        except ProgrammingError as e:
            raise RuntimeError(f"SQL error: {e}\nQuery: {sql}") from e

    def _fetchall_as_dicts(self, cursor) -> list[dict]:
        """Convert cursor result to list of dicts."""
        cols = [desc[0].lower() for desc in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def get_active_sources(self) -> list[dict]:
        """
        Get all active sources from source_catalog.

        Returns:
            List of dicts with keys: source_name, airbyte_connection_id, sync_schedule,
                                     business_owner, is_active, last_synced_at
        """
        sql = """
            SELECT source_name, airbyte_connection_id, sync_schedule, business_owner,
                   is_active, last_synced_at
            FROM PLATFORM.PUBLIC.source_catalog
            WHERE is_active = TRUE
            ORDER BY source_name
        """
        cursor = self._execute(sql)
        return self._fetchall_as_dicts(cursor)

    def get_tables_for_source(self, source_name: str) -> list[dict]:
        """
        Get all tables for a source with columns, primary keys, and load strategy.

        Args:
            source_name: Name of the source (e.g. 'stripe')

        Returns:
            List of dicts with keys: source_name, table_name, columns (JSON),
                                     primary_keys (array), load_strategy, schema_hash, business_owner
        """
        sql = """
            SELECT source_name, table_name, columns, primary_keys, load_strategy,
                   schema_hash, business_owner, is_active
            FROM PLATFORM.PUBLIC.schema_registry
            WHERE source_name = %s
              AND is_active = TRUE
            ORDER BY table_name
        """
        cursor = self._execute(sql, (source_name,))
        return self._fetchall_as_dicts(cursor)

    def get_table_schema(self, source_name: str, table_name: str) -> Optional[dict]:
        """
        Get a specific table's schema entry.

        Args:
            source_name: Source name
            table_name: Table name

        Returns:
            Dict with schema info, or None if not found
        """
        sql = """
            SELECT *
            FROM PLATFORM.PUBLIC.schema_registry
            WHERE source_name = %s AND table_name = %s
        """
        cursor = self._execute(sql, (source_name, table_name))
        results = self._fetchall_as_dicts(cursor)
        return results[0] if results else None

    def get_changed_tables_since(self, timestamp: datetime) -> list[dict]:
        """
        Get all tables whose schema changed after the given timestamp.

        Args:
            timestamp: UTC datetime to check from

        Returns:
            List of dicts with schema info for changed tables
        """
        # Format as ISO 8601 with timezone
        ts_str = timestamp.isoformat()
        sql = """
            SELECT source_name, table_name, columns, primary_keys, load_strategy,
                   change_history, last_changed_at
            FROM PLATFORM.PUBLIC.schema_registry
            WHERE last_changed_at >= %s
            ORDER BY source_name, table_name
        """
        cursor = self._execute(sql, (ts_str,))
        return self._fetchall_as_dicts(cursor)

    def upsert_schema_registry(
        self,
        source_name: str,
        table_name: str,
        columns: dict,
        primary_keys: Optional[list[str]] = None,
        load_strategy: Optional[str] = None,
        business_owner: Optional[str] = None,
        airbyte_connection_id: Optional[str] = None,
    ) -> dict:
        """
        Upsert a table schema into the registry. Detects schema changes and updates change_history.

        Args:
            source_name: Source name (e.g. 'stripe')
            table_name: Table name (e.g. 'invoices')
            columns: Dict of {column_name: {type, nullable, ...}}
            primary_keys: List of column names that form the PK
            load_strategy: 'incremental' | 'full_refresh' | 'scd2'
            business_owner: Team/email for alerts
            airbyte_connection_id: Airbyte connection UUID

        Returns:
            Dict with keys: is_new, has_changes, new_columns, removed_columns, type_changes
        """
        # Compute schema hash (MD5 of sorted column names + types)
        columns_json = json.dumps(columns, sort_keys=True)
        new_hash = hashlib.md5(columns_json.encode()).hexdigest()

        # Check if table exists
        existing = self.get_table_schema(source_name, table_name)

        if not existing:
            # New table — insert
            sql = """
                INSERT INTO PLATFORM.PUBLIC.schema_registry (
                    source_name, table_name, columns, primary_keys, load_strategy,
                    schema_hash, business_owner, airbyte_connection_id, last_seen_at, last_changed_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
            """
            self._execute(
                sql,
                (
                    source_name,
                    table_name,
                    json.dumps(columns),
                    primary_keys or [],
                    load_strategy,
                    new_hash,
                    business_owner,
                    airbyte_connection_id,
                ),
            )
            return {
                "is_new": True,
                "has_changes": False,
                "new_columns": list(columns.keys()),
                "removed_columns": [],
                "type_changes": [],
            }

        # Existing table — check if schema changed
        old_hash = existing["schema_hash"]
        has_changed = new_hash != old_hash

        diff_report = {
            "is_new": False,
            "has_changes": has_changed,
            "new_columns": [],
            "removed_columns": [],
            "type_changes": [],
        }

        if has_changed:
            # Compute column diffs
            old_columns = existing.get("columns", {})
            if isinstance(old_columns, str):
                old_columns = json.loads(old_columns)

            new_col_names = set(columns.keys())
            old_col_names = set(old_columns.keys())

            diff_report["new_columns"] = list(new_col_names - old_col_names)
            diff_report["removed_columns"] = list(old_col_names - new_col_names)

            # Type changes for columns that exist in both
            for col in new_col_names & old_col_names:
                if columns[col] != old_columns.get(col):
                    diff_report["type_changes"].append(
                        {
                            "column": col,
                            "old_type": old_columns.get(col, {}).get("type"),
                            "new_type": columns[col].get("type"),
                        }
                    )

            # Update registry, append to change_history
            change_history = existing.get("change_history", [])
            if isinstance(change_history, str):
                change_history = json.loads(change_history)
            if not isinstance(change_history, list):
                change_history = []

            # Record old state in history
            change_history.append(
                {
                    "schema_hash": old_hash,
                    "columns": old_columns,
                    "changed_at": datetime.now(timezone.utc).isoformat(),
                }
            )

            sql = """
                UPDATE PLATFORM.PUBLIC.schema_registry
                SET columns = %s,
                    primary_keys = %s,
                    load_strategy = COALESCE(%s, load_strategy),
                    business_owner = COALESCE(%s, business_owner),
                    schema_hash = %s,
                    last_seen_at = CURRENT_TIMESTAMP(),
                    last_changed_at = CURRENT_TIMESTAMP(),
                    change_history = %s
                WHERE source_name = %s AND table_name = %s
            """
            self._execute(
                sql,
                (
                    json.dumps(columns),
                    primary_keys or existing.get("primary_keys", []),
                    load_strategy,
                    business_owner,
                    new_hash,
                    json.dumps(change_history),
                    source_name,
                    table_name,
                ),
            )
        else:
            # No schema change — just update last_seen_at
            sql = """
                UPDATE PLATFORM.PUBLIC.schema_registry
                SET last_seen_at = CURRENT_TIMESTAMP(),
                    primary_keys = COALESCE(%s, primary_keys),
                    load_strategy = COALESCE(%s, load_strategy),
                    business_owner = COALESCE(%s, business_owner)
                WHERE source_name = %s AND table_name = %s
            """
            self._execute(
                sql,
                (
                    primary_keys,
                    load_strategy,
                    business_owner,
                    source_name,
                    table_name,
                ),
            )

        return diff_report

    def register_source(
        self,
        source_name: str,
        airbyte_connection_id: str,
        sync_schedule: Optional[str] = None,
        business_owner: Optional[str] = None,
    ) -> None:
        """
        Register a new source in source_catalog.

        Args:
            source_name: Name of the source
            airbyte_connection_id: UUID from Airbyte
            sync_schedule: Cron expression (e.g. '0 */6 * * *')
            business_owner: Team/email
        """
        sql = """
            INSERT INTO PLATFORM.PUBLIC.source_catalog (
                source_name, airbyte_connection_id, sync_schedule, business_owner
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        self._execute(
            sql,
            (source_name, airbyte_connection_id, sync_schedule, business_owner),
        )

    def mark_model_generated(self, source_name: str, table_name: str, layer: str) -> None:
        """
        Mark that bronze/silver models were successfully generated for a table.

        Args:
            source_name: Source name
            table_name: Table name
            layer: 'bronze' or 'silver'
        """
        # Just log to run_log — actual model status is implicit in git
        # This is here for future extensibility
        pass

    def soft_deprecate_column(self, source_name: str, table_name: str, column_name: str) -> None:
        """
        Mark a column as deprecated (removed from source but kept in models).

        Args:
            source_name: Source name
            table_name: Table name
            column_name: Column to deprecate
        """
        # In the current design, we store a marker in columns as {"deprecated": true}
        # This would be done by the evolution handler when regenerating the model
        pass
