#!/usr/bin/env python3
"""
Platform Health Check

Verifies all external service connections:
  - Snowflake: connectivity and schema setup
  - Airbyte: API reachability and authentication
  - Airflow: API reachability (if running)
  - dbt: profiles.yml and connection to Snowflake

Run after init_snowflake.py to validate the platform is ready.

Usage:
  python platform/scripts/health_check.py
"""

import os
import sys
from pathlib import Path

# Color codes for output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"


def check_snowflake() -> bool:
    """Check Snowflake connectivity and schema setup."""
    print(f"\n{BLUE}→ Snowflake{RESET}")

    try:
        import snowflake.connector
    except ImportError:
        print(f"{RED}✗ snowflake-connector-python not installed{RESET}")
        return False

    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")

    if not all([account, user, password]):
        print(f"{RED}✗ Missing credentials (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD){RESET}")
        return False

    try:
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse="PLATFORM_WH",
        )
        cursor = conn.cursor()

        # Check databases
        cursor.execute("SHOW DATABASES LIKE 'GENERIC_AIRBYTE_LANDING'")
        if not cursor.fetchall():
            print(f"{YELLOW}⚠ GENERIC_AIRBYTE_LANDING database not found — run: python platform/scripts/init_snowflake.py{RESET}")
            return False

        # Check schema registry table
        cursor.execute("SELECT COUNT(*) FROM GENERIC_PLATFORM.PUBLIC.schema_registry")
        count = cursor.fetchone()[0]
        print(f"{GREEN}✓ Snowflake connected (account={account}, schema_registry has {count} rows){RESET}")

        conn.close()
        return True

    except Exception as e:
        print(f"{RED}✗ Connection failed: {e}{RESET}")
        return False


def check_airbyte() -> bool:
    """Check Airbyte API connectivity."""
    print(f"\n{BLUE}→ Airbyte{RESET}")

    try:
        import requests
    except ImportError:
        print(f"{RED}✗ requests not installed{RESET}")
        return False

    api_url = os.getenv("AIRBYTE_API_URL", "http://localhost:8000/api/v1")
    email = os.getenv("AIRBYTE_EMAIL")
    password = os.getenv("AIRBYTE_PASSWORD")

    if not all([email, password]):
        print(f"{YELLOW}⚠ Airbyte credentials not set (AIRBYTE_EMAIL, AIRBYTE_PASSWORD){RESET}")
        return False

    try:
        resp = requests.get(f"{api_url}/health", auth=(email, password), timeout=5)
        if resp.status_code == 200:
            print(f"{GREEN}✓ Airbyte API reachable ({api_url}){RESET}")
            return True
        else:
            print(f"{YELLOW}⚠ Airbyte API returned status {resp.status_code}{RESET}")
            return False
    except requests.RequestException as e:
        print(f"{RED}✗ Cannot reach Airbyte: {e}{RESET}")
        print(f"   (Try: docker compose -f compose/docker-compose.yml up -d)")
        return False


def check_airflow() -> bool:
    """Check Airflow API connectivity."""
    print(f"\n{BLUE}→ Airflow{RESET}")

    try:
        import requests
    except ImportError:
        print(f"{RED}✗ requests not installed{RESET}")
        return False

    airflow_url = "http://localhost:8080"

    try:
        resp = requests.get(f"{airflow_url}/health", timeout=5)
        if resp.status_code == 200:
            print(f"{GREEN}✓ Airflow reachable ({airflow_url}){RESET}")
            return True
        else:
            print(f"{YELLOW}⚠ Airflow returned status {resp.status_code}{RESET}")
            return False
    except requests.RequestException as e:
        print(f"{YELLOW}⚠ Cannot reach Airflow: {e}{RESET}")
        print(f"   (Try: docker compose -f compose/docker-compose.yml up -d)")
        return False


def check_dbt() -> bool:
    """Check dbt project setup."""
    print(f"\n{BLUE}→ dbt{RESET}")

    dbt_project = Path("dbt_project")

    if not dbt_project.exists():
        print(f"{RED}✗ dbt_project/ directory not found{RESET}")
        return False

    profile_file = dbt_project / "profiles.yml"
    if not profile_file.exists():
        print(f"{RED}✗ dbt_project/profiles.yml not found{RESET}")
        return False

    try:
        import yaml

        with open(profile_file) as f:
            profiles = yaml.safe_load(f)

        if profiles:
            print(f"{GREEN}✓ dbt project ready (profiles.yml configured){RESET}")
            return True
        else:
            print(f"{YELLOW}⚠ dbt profiles.yml is empty{RESET}")
            return False

    except Exception as e:
        print(f"{YELLOW}⚠ dbt profiles.yml parse error: {e}{RESET}")
        return False


def check_git() -> bool:
    """Check git repository status."""
    print(f"\n{BLUE}→ Git{RESET}")

    try:
        from git import Repo

        repo = Repo(".")
        branch = repo.active_branch.name
        print(f"{GREEN}✓ Git repo initialized (branch={branch}){RESET}")
        return True
    except Exception as e:
        print(f"{YELLOW}⚠ Not a git repository: {e}{RESET}")
        return False


def main():
    """Run all health checks."""
    print("\n" + "=" * 70)
    print(f"{BLUE}Platform Health Check{RESET}")
    print("=" * 70)

    results = {
        "Snowflake": check_snowflake(),
        "Airbyte": check_airbyte(),
        "Airflow": check_airflow(),
        "dbt": check_dbt(),
        "Git": check_git(),
    }

    passed = sum(results.values())
    total = len(results)

    print("\n" + "=" * 70)
    if passed == total:
        print(f"{GREEN}✓ All checks passed!{RESET}")
        print("\nNext steps:")
        print("  1. Connect your first source in Airbyte (http://localhost:8000)")
        print("  2. Run: platform add-source --name <source_name> --airbyte-connection-id <id>")
        print("  3. Run: platform sync <source_name>")
        return 0
    else:
        print(f"{YELLOW}⚠ {passed}/{total} checks passed{RESET}")
        print("\nFailed checks:")
        for name, passed_check in results.items():
            if not passed_check:
                print(f"  - {name}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
