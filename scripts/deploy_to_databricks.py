#!/usr/bin/env python3
"""
Deploy ETL Framework to Databricks Workspace

This script handles deployment when Databricks Asset Bundles (DAB) fails
on Windows due to the "open nul" bug. It manually uploads notebooks and
configs, then creates/updates jobs.

Usage:
    python scripts/deploy_to_databricks.py [--target dev|stg|prod]

Prerequisites:
    - Databricks CLI configured (~/.databrickscfg)
    - pip install databricks-sdk
"""

import argparse
import subprocess
import sys
import os
import json
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
CONFIGS_DIR = PROJECT_ROOT / "configs"

# Workspace paths by target
WORKSPACE_PATHS = {
    "dev": "/Workspace/Users/pavankbs1999@gmail.com/SAS_Migration",
    "stg": "/Workspace/Shared/SAS_Migration_STG",
    "prod": "/Workspace/Shared/SAS_Migration_PROD",
}


def run_cmd(cmd: list, check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command and return result."""
    print(f"  Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"  ERROR: {result.stderr}")
        sys.exit(1)
    return result


def ensure_directory(workspace_path: str):
    """Create workspace directory if it doesn't exist."""
    run_cmd(["databricks", "workspace", "mkdirs", workspace_path], check=False)


def upload_notebook(local_path: Path, workspace_path: str, language: str = "PYTHON"):
    """Upload a notebook to Databricks workspace."""
    cmd = [
        "databricks", "workspace", "import",
        workspace_path,
        "--file", str(local_path),
        "--language", language,
        "--overwrite"
    ]
    run_cmd(cmd)


def upload_file(local_path: Path, workspace_path: str):
    """Upload a file (non-notebook) to Databricks workspace."""
    cmd = [
        "databricks", "workspace", "import",
        workspace_path,
        "--file", str(local_path),
        "--format", "AUTO",
        "--overwrite"
    ]
    run_cmd(cmd)


def deploy_notebooks(target: str):
    """Deploy all notebooks to workspace."""
    base_path = WORKSPACE_PATHS[target]

    print("\n📓 Deploying notebooks...")

    # ETL notebooks
    etl_notebooks = [
        ("etl/01_ingest_snowflake.py", f"{base_path}/ETL/01_ingest_snowflake"),
        ("etl/02_bronze_to_silver.py", f"{base_path}/ETL/02_bronze_to_silver"),
    ]

    # Utility notebooks
    util_notebooks = [
        ("utils/config_loader.py", f"{base_path}/utils/config_loader"),
        ("utils/logger.py", f"{base_path}/utils/logger"),
        ("utils/validators.py", f"{base_path}/utils/validators"),
    ]

    # Setup notebooks
    setup_notebooks = [
        ("setup/00_init_workspace.py", f"{base_path}/setup/00_init_workspace"),
    ]

    all_notebooks = etl_notebooks + util_notebooks + setup_notebooks

    # Ensure directories exist
    ensure_directory(f"{base_path}/ETL")
    ensure_directory(f"{base_path}/utils")
    ensure_directory(f"{base_path}/setup")

    for local_rel, workspace_path in all_notebooks:
        local_path = NOTEBOOKS_DIR / local_rel
        if local_path.exists():
            print(f"  Uploading {local_rel}...")
            upload_notebook(local_path, workspace_path)
        else:
            print(f"  SKIP: {local_rel} not found")


def deploy_configs(target: str):
    """Deploy configuration files to workspace."""
    base_path = WORKSPACE_PATHS[target]

    print("\n⚙️  Deploying configs...")

    ensure_directory(f"{base_path}/configs/ingestion")

    config_files = [
        ("ingestion/master_table_inventory.csv", f"{base_path}/configs/ingestion/master_table_inventory.csv"),
    ]

    for local_rel, workspace_path in config_files:
        local_path = CONFIGS_DIR / local_rel
        if local_path.exists():
            print(f"  Uploading {local_rel}...")
            upload_file(local_path, workspace_path)
        else:
            print(f"  SKIP: {local_rel} not found")


def create_job(job_name: str, table_name: str, target: str):
    """Create or update a Databricks job for a table."""
    base_path = WORKSPACE_PATHS[target]

    job_config = {
        "name": job_name,
        "tasks": [
            {
                "task_key": "ingest_to_bronze",
                "notebook_task": {
                    "notebook_path": f"{base_path}/ETL/01_ingest_snowflake",
                    "base_parameters": {
                        "table_name": table_name,
                        "force_full_load": "N"
                    },
                    "source": "WORKSPACE"
                }
            },
            {
                "task_key": "bronze_to_silver",
                "depends_on": [{"task_key": "ingest_to_bronze"}],
                "notebook_task": {
                    "notebook_path": f"{base_path}/ETL/02_bronze_to_silver",
                    "base_parameters": {
                        "table_name": table_name
                    },
                    "source": "WORKSPACE"
                }
            }
        ]
    }

    # Check if job exists
    result = run_cmd(["databricks", "jobs", "list", "--output", "JSON"], check=False)
    existing_jobs = json.loads(result.stdout) if result.stdout else {"jobs": []}

    job_id = None
    for job in existing_jobs.get("jobs", []):
        if job.get("settings", {}).get("name") == job_name:
            job_id = job["job_id"]
            break

    # Write job config to temp file
    temp_file = PROJECT_ROOT / "temp_job_config.json"
    with open(temp_file, "w") as f:
        json.dump(job_config, f)

    try:
        if job_id:
            print(f"  Updating job {job_name} (ID: {job_id})...")
            run_cmd(["databricks", "jobs", "reset", str(job_id), "--json", f"@{temp_file}"])
        else:
            print(f"  Creating job {job_name}...")
            result = run_cmd(["databricks", "jobs", "create", "--json", f"@{temp_file}"])
            job_data = json.loads(result.stdout)
            job_id = job_data.get("job_id")
            print(f"  Created job ID: {job_id}")
    finally:
        temp_file.unlink(missing_ok=True)

    return job_id


def deploy_jobs(target: str):
    """Deploy all ETL jobs."""
    print("\n🚀 Deploying jobs...")

    # Define jobs from master_table_inventory.csv
    jobs = [
        ("asb-etl-od-gdwextract-dev", "OD_GDWEXTRACT_201201_202408"),
        # Add more jobs here as needed
    ]

    for job_name, table_name in jobs:
        create_job(job_name, table_name, target)


def main():
    parser = argparse.ArgumentParser(description="Deploy ETL Framework to Databricks")
    parser.add_argument("--target", choices=["dev", "stg", "prod"], default="dev",
                        help="Deployment target (default: dev)")
    parser.add_argument("--notebooks-only", action="store_true",
                        help="Only deploy notebooks, skip jobs")
    args = parser.parse_args()

    print(f"=" * 60)
    print(f"Deploying to: {args.target.upper()}")
    print(f"Workspace: {WORKSPACE_PATHS[args.target]}")
    print(f"=" * 60)

    # Deploy components
    deploy_notebooks(args.target)
    deploy_configs(args.target)

    if not args.notebooks_only:
        deploy_jobs(args.target)

    print("\n✅ Deployment complete!")
    print(f"\nTo run a job:")
    print(f"  databricks jobs run-now --json @run_params.json")


if __name__ == "__main__":
    main()
