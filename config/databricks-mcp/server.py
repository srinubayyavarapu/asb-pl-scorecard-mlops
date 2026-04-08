"""
Databricks MCP Server - Local Version for SAS Migration Framework
Provides tools for interacting with Databricks workspace, jobs, notebooks, SQL, and Unity Catalog.

Features:
- Cluster management (list, create, start, terminate)
- Job management (list, create, run, monitor)
- Notebook operations (list, import, export, run)
- SQL execution via SQL Warehouse
- Unity Catalog operations (catalogs, schemas, tables)
- DBFS file operations
- Repository management
"""

import os
import sys
import json
import logging
import base64
import time
import requests
import subprocess
from datetime import datetime
from typing import Optional, Any, List, Dict
from pathlib import Path
from pydantic import BaseModel

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP


# ============================================================================
# PYDANTIC MODELS FOR JOB CREATION
# ============================================================================

class TaskDependencyModel(BaseModel):
    """Model for task dependency."""
    task_key: str

class NotebookTaskModel(BaseModel):
    """Model for notebook task configuration."""
    notebook_path: str
    source: Optional[str] = None

class JobTaskModel(BaseModel):
    """Model for a Databricks job task with dependencies."""
    task_key: str
    notebook_task: Optional[NotebookTaskModel] = None
    existing_cluster_id: Optional[str] = None
    new_cluster: Optional[dict] = None
    depends_on: Optional[List[TaskDependencyModel]] = None
    timeout_seconds: Optional[int] = None

class JobModel(BaseModel):
    """Simplified Databricks Job model used for job creation."""
    name: str
    tasks: List[JobTaskModel]
    existing_cluster_id: Optional[str] = None
    new_cluster: Optional[dict] = None
    max_concurrent_runs: Optional[int] = None

# Load environment variables from .env file
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)

# Setup logging
log_dir = Path(__file__).parent / "logs"
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"databricks_mcp_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stderr)
    ]
)
logger = logging.getLogger("databricks_mcp")

# Initialize MCP server
mcp = FastMCP("databricks")

# Databricks configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_SQL_WAREHOUSE_ID = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "")


def get_workspace_client():
    """Get Databricks WorkspaceClient with configured credentials."""
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient(
        host=DATABRICKS_HOST,
        token=DATABRICKS_TOKEN
    )


def json_response(data: Any) -> str:
    """Convert data to JSON string for MCP response."""
    return json.dumps(data, indent=2, default=str)


# ============================================================================
# CLUSTER OPERATIONS
# ============================================================================

@mcp.tool()
def list_clusters() -> str:
    """List all Databricks clusters.

    Returns:
        JSON list of clusters with their details (id, name, state, etc.)
    """
    logger.info("list_clusters() called")
    try:
        w = get_workspace_client()
        clusters = list(w.clusters.list())
        result = []
        for c in clusters:
            result.append({
                "cluster_id": c.cluster_id,
                "cluster_name": c.cluster_name,
                "state": str(c.state) if c.state else None,
                "spark_version": c.spark_version,
                "node_type_id": c.node_type_id,
                "num_workers": c.num_workers,
                "autoscale": {"min": c.autoscale.min_workers, "max": c.autoscale.max_workers} if c.autoscale else None,
                "creator_user_name": c.creator_user_name,
            })
        logger.info(f"Found {len(result)} clusters")
        return json_response({"clusters": result})
    except Exception as e:
        logger.error(f"list_clusters failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def get_cluster(cluster_id: str) -> str:
    """Get information about a specific cluster.

    Args:
        cluster_id: The Databricks cluster ID

    Returns:
        JSON with cluster details
    """
    logger.info(f"get_cluster() called for {cluster_id}")
    try:
        w = get_workspace_client()
        c = w.clusters.get(cluster_id)
        result = {
            "cluster_id": c.cluster_id,
            "cluster_name": c.cluster_name,
            "state": str(c.state) if c.state else None,
            "spark_version": c.spark_version,
            "node_type_id": c.node_type_id,
            "driver_node_type_id": c.driver_node_type_id,
            "num_workers": c.num_workers,
            "autoscale": {"min": c.autoscale.min_workers, "max": c.autoscale.max_workers} if c.autoscale else None,
            "spark_conf": dict(c.spark_conf) if c.spark_conf else None,
            "state_message": c.state_message,
        }
        return json_response(result)
    except Exception as e:
        logger.error(f"get_cluster failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def create_cluster(cluster_name: str, spark_version: str, node_type_id: str,
                   num_workers: int = 1, autotermination_minutes: int = 30) -> str:
    """Create a new Databricks cluster.

    Args:
        cluster_name: Name for the new cluster
        spark_version: Spark runtime version (e.g., '14.3.x-scala2.12')
        node_type_id: Node type (e.g., 'Standard_DS3_v2')
        num_workers: Number of worker nodes (default: 1)
        autotermination_minutes: Auto-terminate after idle minutes (default: 30)

    Returns:
        JSON with cluster_id of created cluster
    """
    logger.info(f"create_cluster() called: {cluster_name}")
    try:
        w = get_workspace_client()
        c = w.clusters.create(
            cluster_name=cluster_name,
            spark_version=spark_version,
            node_type_id=node_type_id,
            num_workers=num_workers,
            autotermination_minutes=autotermination_minutes
        ).result()  # Wait for cluster creation
        return json_response({"cluster_id": c.cluster_id, "status": "created"})
    except Exception as e:
        logger.error(f"create_cluster failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def start_cluster(cluster_id: str) -> str:
    """Start a terminated cluster.

    Args:
        cluster_id: The Databricks cluster ID to start

    Returns:
        JSON with status
    """
    logger.info(f"start_cluster() called for {cluster_id}")
    try:
        w = get_workspace_client()
        w.clusters.start(cluster_id).result()
        return json_response({"cluster_id": cluster_id, "status": "started"})
    except Exception as e:
        logger.error(f"start_cluster failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def terminate_cluster(cluster_id: str) -> str:
    """Terminate a running cluster.

    Args:
        cluster_id: The Databricks cluster ID to terminate

    Returns:
        JSON with status
    """
    logger.info(f"terminate_cluster() called for {cluster_id}")
    try:
        w = get_workspace_client()
        w.clusters.delete(cluster_id).result()
        return json_response({"cluster_id": cluster_id, "status": "terminated"})
    except Exception as e:
        logger.error(f"terminate_cluster failed: {e}")
        return json_response({"error": str(e)})


# ============================================================================
# JOB OPERATIONS
# ============================================================================

@mcp.tool()
def list_jobs() -> str:
    """List all Databricks jobs.

    Returns:
        JSON list of jobs
    """
    logger.info("list_jobs() called")
    try:
        w = get_workspace_client()
        jobs = list(w.jobs.list())
        result = []
        for j in jobs:
            result.append({
                "job_id": j.job_id,
                "name": j.settings.name if j.settings else None,
                "creator_user_name": j.creator_user_name,
            })
        logger.info(f"Found {len(result)} jobs")
        return json_response({"jobs": result})
    except Exception as e:
        logger.error(f"list_jobs failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def get_job(job_id: int) -> str:
    """Get detailed information about a job including tasks and dependencies.

    Args:
        job_id: The Databricks job ID

    Returns:
        JSON with job details including tasks and their dependencies
    """
    logger.info(f"get_job() called for job {job_id}")
    try:
        w = get_workspace_client()
        job = w.jobs.get(job_id)

        tasks = []
        if job.settings and job.settings.tasks:
            for t in job.settings.tasks:
                task_info = {
                    "task_key": t.task_key,
                    "depends_on": [{"task_key": d.task_key} for d in t.depends_on] if t.depends_on else [],
                    "existing_cluster_id": t.existing_cluster_id,
                    "notebook_task": {
                        "notebook_path": t.notebook_task.notebook_path
                    } if t.notebook_task else None,
                }
                tasks.append(task_info)

        result = {
            "job_id": job.job_id,
            "name": job.settings.name if job.settings else None,
            "creator_user_name": job.creator_user_name,
            "tasks": tasks,
        }
        return json_response(result)
    except Exception as e:
        logger.error(f"get_job failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def create_job(job: JobModel) -> str:
    """Create a Databricks job.

    Args:
        job: Job configuration containing:
            - name: Job name
            - tasks: List of task configurations with optional depends_on
            - existing_cluster_id: (optional) Cluster ID to use
            - new_cluster: (optional) New cluster config

    Returns:
        JSON with job_id of created job
    """
    logger.info("create_job() called")
    try:
        # Convert Pydantic model to dict for processing
        config = job.model_dump()
        logger.info(f"Parsed job config: {json.dumps(config, indent=2)}")
        w = get_workspace_client()

        from databricks.sdk.service.jobs import Task, NotebookTask, TaskDependency, Source
        from databricks.sdk.service.compute import ClusterSpec

        tasks = []
        for t in config.get("tasks", []):
            logger.info(f"\n{'='*60}\nProcessing task: {t.get('task_key')}\n{'='*60}")
            logger.info(f"Raw task config: {json.dumps(t, indent=2)}")
            
            task_params = {
                "task_key": t["task_key"]
            }
            
            # Build depends_on list if present - Must be TaskDependency objects for SDK serialization
            if t.get("depends_on"):
                depends_on = [
                    TaskDependency(task_key=dep["task_key"])
                    for dep in t["depends_on"]
                ]
                logger.info(f"Task '{t['task_key']}' has {len(depends_on)} dependencies: {[d.task_key for d in depends_on]}")
                logger.info(f"TaskDependency objects created: {[dep.as_dict() for dep in depends_on]}")
                task_params["depends_on"] = depends_on
            else:
                logger.info(f"Task '{t['task_key']}' has no dependencies")

            # Build new_cluster config if present
            if t.get("new_cluster"):
                nc = t["new_cluster"]
                new_cluster = ClusterSpec(
                    spark_version=nc.get("spark_version"),
                    node_type_id=nc.get("node_type_id"),
                    num_workers=nc.get("num_workers", 0),
                    spark_conf=nc.get("spark_conf"),
                    custom_tags=nc.get("custom_tags")
                )
                task_params["new_cluster"] = new_cluster

            # Use existing_cluster_id if provided (task level or job level)
            cluster_id = t.get("existing_cluster_id") or config.get("existing_cluster_id")
            if cluster_id:
                task_params["existing_cluster_id"] = cluster_id

            # Build notebook task if present
            if t.get("notebook_task"):
                # Convert source string to Source enum if provided
                source_value = None
                source_str = t["notebook_task"].get("source")
                if source_str:
                    source_map = {"WORKSPACE": Source.WORKSPACE, "GIT": Source.GIT}
                    source_value = source_map.get(source_str.upper())

                notebook_task = NotebookTask(
                    notebook_path=t["notebook_task"]["notebook_path"],
                    source=source_value
                )
                task_params["notebook_task"] = notebook_task

            # Add timeout if present
            if t.get("timeout_seconds"):
                task_params["timeout_seconds"] = t["timeout_seconds"]

            # Create task with only the parameters that are set
            logger.info(f"Creating Task object for '{t['task_key']}' with params: {list(task_params.keys())}")
            task = Task(**task_params)
            logger.info(f"Task '{t['task_key']}' depends_on after Task creation: {task.depends_on}")
            logger.info(f"Task '{t['task_key']}' as_dict(): {json.dumps(task.as_dict(), indent=2, default=str)}")
            tasks.append(task)

        logger.info(f"\n{'='*60}\nCreating job with {len(tasks)} tasks\n{'='*60}")
        logger.info(f"Job name: {config['name']}")
        logger.info(f"All tasks summary:")
        for idx, task in enumerate(tasks):
            logger.info(f"  Task {idx+1}: {task.task_key}, depends_on={task.depends_on}")
        
        job = w.jobs.create(
            name=config["name"],
            tasks=tasks,
            max_concurrent_runs=config.get("max_concurrent_runs")
        )
        logger.info(f"Job created successfully with job_id: {job.job_id}")
        logger.info(f"Verifying created job...")
        
        # Immediately verify the created job
        created_job = w.jobs.get(job.job_id)
        if created_job.settings and created_job.settings.tasks:
            logger.info(f"Verification - Job has {len(created_job.settings.tasks)} tasks:")
            for t in created_job.settings.tasks:
                deps = t.depends_on if t.depends_on else []
                logger.info(f"  Task: {t.task_key}, depends_on count: {len(deps)}, dependencies: {[d.task_key for d in deps]}")
        
        return json_response({"job_id": job.job_id})
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in job_config: {e}")
        return json_response({"error": f"Invalid JSON: {str(e)}"})
    except Exception as e:
        logger.error(f"create_job failed: {e}", exc_info=True)
        return json_response({"error": str(e)})


@mcp.tool()
def run_job(job_id: int, notebook_params: Optional[str] = None) -> str:
    """Trigger a job run.

    Args:
        job_id: The Databricks job ID to run
        notebook_params: Optional JSON string of notebook parameters

    Returns:
        JSON with run_id
    """
    logger.info(f"run_job() called for job {job_id}")
    try:
        w = get_workspace_client()
        params = json.loads(notebook_params) if notebook_params else None
        run = w.jobs.run_now(job_id=job_id, notebook_params=params)
        return json_response({"run_id": run.run_id})
    except Exception as e:
        logger.error(f"run_job failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def delete_job(job_id: int) -> str:
    """Delete a Databricks job.

    Args:
        job_id: The job ID to delete

    Returns:
        JSON with status
    """
    logger.info(f"delete_job() called for job {job_id}")
    try:
        w = get_workspace_client()
        w.jobs.delete(job_id)
        return json_response({"job_id": job_id, "status": "deleted"})
    except Exception as e:
        logger.error(f"delete_job failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def get_run_status(run_id: int) -> str:
    """Get status for a job run.

    Args:
        run_id: The run ID to check

    Returns:
        JSON with run status details
    """
    logger.info(f"get_run_status() called for run {run_id}")
    try:
        w = get_workspace_client()
        run = w.jobs.get_run(run_id)
        result = {
            "run_id": run.run_id,
            "job_id": run.job_id,
            "state": {
                "life_cycle_state": str(run.state.life_cycle_state) if run.state else None,
                "result_state": str(run.state.result_state) if run.state and run.state.result_state else None,
                "state_message": run.state.state_message if run.state else None,
            },
            "start_time": run.start_time,
            "end_time": run.end_time,
            "run_duration": run.run_duration,
        }
        return json_response(result)
    except Exception as e:
        logger.error(f"get_run_status failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def list_job_runs(job_id: Optional[int] = None) -> str:
    """List recent job runs.

    Args:
        job_id: Optional job ID to filter runs

    Returns:
        JSON list of runs
    """
    logger.info(f"list_job_runs() called, job_id={job_id}")
    try:
        w = get_workspace_client()
        runs = list(w.jobs.list_runs(job_id=job_id, limit=20))
        result = []
        for r in runs:
            result.append({
                "run_id": r.run_id,
                "job_id": r.job_id,
                "state": str(r.state.life_cycle_state) if r.state else None,
                "result_state": str(r.state.result_state) if r.state and r.state.result_state else None,
                "start_time": r.start_time,
            })
        return json_response({"runs": result})
    except Exception as e:
        logger.error(f"list_job_runs failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def cancel_run(run_id: int) -> str:
    """Cancel a job run.

    Args:
        run_id: The run ID to cancel

    Returns:
        JSON with status
    """
    logger.info(f"cancel_run() called for run {run_id}")
    try:
        w = get_workspace_client()
        w.jobs.cancel_run(run_id)
        return json_response({"run_id": run_id, "status": "cancelled"})
    except Exception as e:
        logger.error(f"cancel_run failed: {e}")
        return json_response({"error": str(e)})


# ============================================================================
# NOTEBOOK OPERATIONS
# ============================================================================

@mcp.tool()
def list_notebooks(path: str) -> str:
    """List notebooks in a directory.

    Args:
        path: Workspace path to list (e.g., '/Workspace/SAS_Migration')

    Returns:
        JSON list of notebooks and folders
    """
    logger.info(f"list_notebooks() called for path: {path}")
    try:
        w = get_workspace_client()
        items = list(w.workspace.list(path))
        result = []
        for item in items:
            result.append({
                "path": item.path,
                "object_type": str(item.object_type) if item.object_type else None,
                "language": str(item.language) if item.language else None,
            })
        return json_response({"items": result})
    except Exception as e:
        logger.error(f"list_notebooks failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def import_notebook(workspace_path: str, local_file_path: str, language: str = "PYTHON",
                    overwrite: bool = True) -> str:
    """Import a notebook to workspace using Databricks CLI.

    CONTEXT-EFFICIENT: Only file paths are passed, not file content.
    Uses Databricks CLI for reliable file upload.

    Args:
        workspace_path: Destination path in Databricks workspace (e.g., '/Workspace/SAS_Migration/Scripts/my_script')
                        Note: Will be automatically prefixed with '/' to create '//Workspace/...' format required by CLI
        local_file_path: Path to local file to upload (e.g., 'workflow/02_converted_notebooks/my_script.py')
                        Can be relative or absolute path.
        language: Language (PYTHON, SQL, SCALA, R)
        overwrite: Whether to overwrite existing notebook (default: True)

    Returns:
        JSON with status and details

    Example:
        import_notebook(
            workspace_path="/Workspace/SAS_Migration/Converted_Scripts/employee_payroll_summary",
            local_file_path="workflow/02_converted_notebooks/employee_payroll_summary_converted.py",
            language="PYTHON",
            overwrite=True
        )
        # CLI command executed: databricks workspace import //Workspace/SAS_Migration/... --file "local_path" --language PYTHON --overwrite
    """
    import subprocess
    import os
    import platform

    logger.info(f"import_notebook() called - workspace_path: {workspace_path}, local_file: {local_file_path}")

    try:
        # Convert to absolute path if relative
        if not os.path.isabs(local_file_path):
            local_file_path = os.path.abspath(local_file_path)
            logger.info(f"Converted to absolute path: {local_file_path}")

        # Validate local file exists
        if not os.path.exists(local_file_path):
            return json_response({
                "error": f"Local file not found: {local_file_path}",
                "workspace_path": workspace_path
            })

        # Validate language
        valid_languages = ["PYTHON", "SQL", "SCALA", "R"]
        language_upper = language.upper()
        if language_upper not in valid_languages:
            return json_response({
                "error": f"Invalid language: {language}. Must be one of: {valid_languages}",
                "workspace_path": workspace_path
            })

        # Normalize workspace path to use //Workspace/... format required by Databricks CLI
        # The CLI requires double slash prefix: //Workspace/path/to/notebook
        normalized_path = workspace_path
        if normalized_path.startswith("//Workspace"):
            # Already has correct format
            pass
        elif normalized_path.startswith("/Workspace"):
            # Add extra leading slash: /Workspace -> //Workspace
            normalized_path = "/" + normalized_path
        elif normalized_path.startswith("Workspace"):
            # Add double leading slash: Workspace -> //Workspace
            normalized_path = "//" + normalized_path
        else:
            # Assume it's a relative path, prepend //Workspace/
            normalized_path = "//Workspace/" + normalized_path.lstrip("/")

        logger.info(f"Normalized workspace path: {normalized_path}")

        # Build CLI command as a single string for shell execution (handles paths with spaces better on Windows)
        # Format: databricks workspace import //Workspace/path --file "local_path" --language PYTHON --overwrite
        is_windows = platform.system() == "Windows"

        if is_windows:
            # On Windows, use shell=True with properly quoted paths
            cmd_str = f'databricks workspace import {normalized_path} --file "{local_file_path}" --language {language_upper}'
            if overwrite:
                cmd_str += " --overwrite"

            logger.info(f"Executing CLI command (Windows shell): {cmd_str}")

            # Execute CLI command with shell=True for Windows
            result = subprocess.run(
                cmd_str,
                capture_output=True,
                text=True,
                shell=True,
                timeout=120,  # 2 minute timeout
                stdin=subprocess.DEVNULL,  # Prevent waiting for input
                env=os.environ.copy()  # Pass current environment
            )
        else:
            # On Unix-like systems, use list format
            cmd = [
                "databricks", "workspace", "import",
                normalized_path,
                "--file", local_file_path,
                "--language", language_upper
            ]
            if overwrite:
                cmd.append("--overwrite")

            logger.info(f"Executing CLI command: {' '.join(cmd)}")

            # Execute CLI command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,  # 2 minute timeout
                stdin=subprocess.DEVNULL,  # Prevent waiting for input
                env=os.environ.copy()  # Pass current environment
            )

        if result.returncode == 0:
            logger.info(f"Successfully imported notebook to {normalized_path}")
            return json_response({
                "workspace_path": normalized_path,
                "original_path": workspace_path,
                "local_file_path": local_file_path,
                "language": language_upper,
                "overwrite": overwrite,
                "status": "imported",
                "message": result.stdout.strip() if result.stdout else "Notebook imported successfully"
            })
        else:
            error_msg = result.stderr.strip() if result.stderr else "Unknown error"
            logger.error(f"CLI import failed: {error_msg}")
            return json_response({
                "error": error_msg,
                "workspace_path": normalized_path,
                "original_path": workspace_path,
                "local_file_path": local_file_path,
                "command": cmd_str if is_windows else " ".join(cmd),
                "status": "failed"
            })

    except subprocess.TimeoutExpired:
        logger.error(f"import_notebook timed out for {workspace_path}")
        return json_response({
            "error": "Command timed out after 120 seconds",
            "workspace_path": workspace_path,
            "local_file_path": local_file_path
        })
    except Exception as e:
        logger.error(f"import_notebook failed: {e}")
        return json_response({
            "error": str(e),
            "workspace_path": workspace_path,
            "local_file_path": local_file_path
        })


@mcp.tool()
def export_notebook(workspace_path: str, local_file_path: str, format: str = "SOURCE",
                    overwrite: bool = True) -> str:
    """Export a notebook from workspace using Databricks CLI.

    CONTEXT-EFFICIENT: Only file paths are passed, not file content.
    Uses Databricks CLI for reliable file download directly to local filesystem.

    Args:
        workspace_path: Workspace path of notebook (e.g., '/Workspace/SAS_Migration/Scripts/my_script')
        local_file_path: Path to save the exported file locally (e.g., 'workflow/exports/my_script.py')
                        Can be relative or absolute path.
        format: Export format (SOURCE, HTML, JUPYTER, DBC). Default: SOURCE
        overwrite: Whether to overwrite existing local file (default: True)

    Returns:
        JSON with status and details (not file content)

    Example:
        export_notebook(
            workspace_path="/Workspace/SAS_Migration/Converted_Scripts/employee_payroll_summary",
            local_file_path="workflow/exports/employee_payroll_summary.py",
            format="SOURCE",
            overwrite=True
        )
        # CLI command executed: databricks workspace export //Workspace/SAS_Migration/... --file "local_path" --format SOURCE
    """
    import subprocess
    import os
    import platform

    logger.info(f"export_notebook() called - workspace_path: {workspace_path}, local_file: {local_file_path}")

    try:
        # Convert to absolute path if relative
        if not os.path.isabs(local_file_path):
            local_file_path = os.path.abspath(local_file_path)
            logger.info(f"Converted to absolute path: {local_file_path}")

        # Check if file exists and overwrite is False
        if os.path.exists(local_file_path) and not overwrite:
            return json_response({
                "error": f"Local file already exists and overwrite=False: {local_file_path}",
                "workspace_path": workspace_path
            })

        # Create parent directory if it doesn't exist
        parent_dir = os.path.dirname(local_file_path)
        if parent_dir and not os.path.exists(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)
            logger.info(f"Created parent directory: {parent_dir}")

        # Validate format
        valid_formats = ["SOURCE", "HTML", "JUPYTER", "DBC"]
        format_upper = format.upper()
        if format_upper not in valid_formats:
            return json_response({
                "error": f"Invalid format: {format}. Must be one of: {valid_formats}",
                "workspace_path": workspace_path
            })

        # Normalize workspace path to use //Workspace/... format required by Databricks CLI
        normalized_path = workspace_path
        if normalized_path.startswith("//Workspace"):
            # Already has correct format
            pass
        elif normalized_path.startswith("/Workspace"):
            # Add extra leading slash: /Workspace -> //Workspace
            normalized_path = "/" + normalized_path
        elif normalized_path.startswith("Workspace"):
            # Add double leading slash: Workspace -> //Workspace
            normalized_path = "//" + normalized_path
        else:
            # Assume it's a relative path, prepend //Workspace/
            normalized_path = "//Workspace/" + normalized_path.lstrip("/")

        logger.info(f"Normalized workspace path: {normalized_path}")

        # Build CLI command
        # Format: databricks workspace export //Workspace/path --file "local_path" --format SOURCE
        is_windows = platform.system() == "Windows"

        if is_windows:
            # On Windows, use shell=True with properly quoted paths
            cmd_str = f'databricks workspace export {normalized_path} --file "{local_file_path}" --format {format_upper}'
            if overwrite:
                cmd_str += " --overwrite"

            logger.info(f"Executing CLI command (Windows shell): {cmd_str}")

            # Execute CLI command with shell=True for Windows
            result = subprocess.run(
                cmd_str,
                capture_output=True,
                text=True,
                shell=True,
                timeout=120,  # 2 minute timeout
                stdin=subprocess.DEVNULL,  # Prevent waiting for input
                env=os.environ.copy()  # Pass current environment
            )
        else:
            # On Unix-like systems, use list format
            cmd = [
                "databricks", "workspace", "export",
                normalized_path,
                "--file", local_file_path,
                "--format", format_upper
            ]
            if overwrite:
                cmd.append("--overwrite")

            logger.info(f"Executing CLI command: {' '.join(cmd)}")

            # Execute CLI command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,  # 2 minute timeout
                stdin=subprocess.DEVNULL,  # Prevent waiting for input
                env=os.environ.copy()  # Pass current environment
            )

        if result.returncode == 0:
            # Get file size for confirmation
            file_size = os.path.getsize(local_file_path) if os.path.exists(local_file_path) else 0
            logger.info(f"Successfully exported notebook to {local_file_path} ({file_size} bytes)")
            return json_response({
                "workspace_path": normalized_path,
                "original_path": workspace_path,
                "local_file_path": local_file_path,
                "format": format_upper,
                "overwrite": overwrite,
                "file_size_bytes": file_size,
                "status": "exported",
                "message": result.stdout.strip() if result.stdout else "Notebook exported successfully"
            })
        else:
            error_msg = result.stderr.strip() if result.stderr else "Unknown error"
            logger.error(f"CLI export failed: {error_msg}")
            return json_response({
                "error": error_msg,
                "workspace_path": normalized_path,
                "original_path": workspace_path,
                "local_file_path": local_file_path,
                "command": cmd_str if is_windows else " ".join(cmd),
                "status": "failed"
            })

    except subprocess.TimeoutExpired:
        logger.error(f"export_notebook timed out for {workspace_path}")
        return json_response({
            "error": "Command timed out after 120 seconds",
            "workspace_path": workspace_path,
            "local_file_path": local_file_path
        })
    except Exception as e:
        logger.error(f"export_notebook failed: {e}")
        return json_response({
            "error": str(e),
            "workspace_path": workspace_path,
            "local_file_path": local_file_path
        })


@mcp.tool()
def run_notebook(notebook_path: str, existing_cluster_id: Optional[str] = None,
                 base_parameters: Optional[str] = None) -> str:
    """Submit a one-time notebook run.

    Args:
        notebook_path: Path to notebook in workspace
        existing_cluster_id: Optional cluster ID (uses serverless if not provided)
        base_parameters: Optional JSON string of parameters

    Returns:
        JSON with run result
    """
    logger.info(f"run_notebook() called for: {notebook_path}")
    try:
        w = get_workspace_client()
        params = json.loads(base_parameters) if base_parameters else None

        # Build task configuration
        from databricks.sdk.service.jobs import NotebookTask, SubmitTask

        task = SubmitTask(
            task_key="notebook_run",
            notebook_task=NotebookTask(
                notebook_path=notebook_path,
                base_parameters=params
            ),
            existing_cluster_id=existing_cluster_id
        )

        run = w.jobs.submit(tasks=[task], run_name=f"Run {notebook_path}")
        run_id = run.run_id
        logger.info(f"Submitted run_id: {run_id}")

        # Poll for completion with timeout (max 30 minutes)
        max_attempts = 180  # 30 minutes at 10 second intervals
        attempt = 0
        status = None

        while attempt < max_attempts:
            status = w.jobs.get_run(run_id)
            state = status.state.life_cycle_state if status.state else None
            state_str = str(state) if state else None
            logger.info(f"Run {run_id} - attempt {attempt + 1}, state: {state_str}")

            # Check for terminal states (compare as strings)
            if state_str and ("TERMINATED" in state_str or "SKIPPED" in state_str or "INTERNAL_ERROR" in state_str):
                logger.info(f"Run {run_id} completed with state: {state_str}")
                break

            attempt += 1
            time.sleep(10)

        if attempt >= max_attempts:
            logger.warning(f"Run {run_id} timed out after 30 minutes")
            return json_response({
                "run_id": run_id,
                "state": "TIMEOUT",
                "error": "Run did not complete within 30 minutes"
            })

        # Get output
        try:
            output = w.jobs.get_run_output(run_id)
            notebook_output = output.notebook_output.result if output.notebook_output else None
        except Exception as oe:
            logger.warning(f"Could not get output: {oe}")
            notebook_output = None

        result = {
            "run_id": run_id,
            "state": str(status.state.life_cycle_state) if status.state else None,
            "result_state": str(status.state.result_state) if status.state and status.state.result_state else None,
            "notebook_output": notebook_output,
        }
        logger.info(f"run_notebook completed: {result}")
        return json_response(result)
    except Exception as e:
        logger.error(f"run_notebook failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def delete_workspace_object(path: str, recursive: bool = False) -> str:
    """Delete a workspace notebook or directory.

    Args:
        path: Workspace path to delete
        recursive: Delete recursively if directory

    Returns:
        JSON with status
    """
    logger.info(f"delete_workspace_object() called for: {path}")
    try:
        w = get_workspace_client()
        w.workspace.delete(path, recursive=recursive)
        return json_response({"path": path, "status": "deleted"})
    except Exception as e:
        logger.error(f"delete_workspace_object failed: {e}")
        return json_response({"error": str(e)})


# ============================================================================
# SQL EXECUTION
# ============================================================================

@mcp.tool()
def execute_sql(statement: str, warehouse_id: Optional[str] = None,
                catalog: Optional[str] = None, schema_name: Optional[str] = None) -> str:
    """Execute a SQL statement.

    Args:
        statement: SQL statement to execute
        warehouse_id: SQL Warehouse ID (uses default from config if not provided)
        catalog: Optional catalog name
        schema_name: Optional schema name

    Returns:
        JSON with query results
    """
    wh_id = warehouse_id or DATABRICKS_SQL_WAREHOUSE_ID
    logger.info(f"execute_sql() called with warehouse_id={wh_id}")

    if not wh_id:
        return json_response({"error": "No warehouse_id provided and DATABRICKS_SQL_WAREHOUSE_ID not set"})

    try:
        w = get_workspace_client()

        response = w.statement_execution.execute_statement(
            warehouse_id=wh_id,
            statement=statement,
            catalog=catalog,
            schema=schema_name,
            wait_timeout="50s"
        )

        result = {
            "statement_id": response.statement_id,
            "status": str(response.status.state) if response.status else None,
        }

        if response.manifest:
            result["columns"] = [{"name": c.name, "type": c.type_name}
                                for c in response.manifest.schema.columns] if response.manifest.schema else []

        if response.result and response.result.data_array:
            result["data"] = response.result.data_array
            result["row_count"] = len(response.result.data_array)
        else:
            result["data"] = []
            result["row_count"] = 0

        return json_response(result)
    except Exception as e:
        logger.error(f"execute_sql failed: {e}")
        return json_response({"error": str(e)})


# ============================================================================
# UNITY CATALOG OPERATIONS
# ============================================================================

@mcp.tool()
def list_catalogs() -> str:
    """List Unity Catalog catalogs.

    Returns:
        JSON list of catalogs
    """
    logger.info("list_catalogs() called")
    try:
        w = get_workspace_client()
        catalogs = list(w.catalogs.list())
        result = [{"name": c.name, "comment": c.comment, "owner": c.owner} for c in catalogs]
        return json_response({"catalogs": result})
    except Exception as e:
        logger.error(f"list_catalogs failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def create_catalog(name: str, comment: Optional[str] = None) -> str:
    """Create a Unity Catalog catalog.

    Args:
        name: Catalog name
        comment: Optional description

    Returns:
        JSON with status
    """
    logger.info(f"create_catalog() called: {name}")
    try:
        w = get_workspace_client()
        w.catalogs.create(name=name, comment=comment)
        return json_response({"name": name, "status": "created"})
    except Exception as e:
        logger.error(f"create_catalog failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def list_schemas(catalog_name: str) -> str:
    """List schemas in a catalog.

    Args:
        catalog_name: Catalog name

    Returns:
        JSON list of schemas
    """
    logger.info(f"list_schemas() called for catalog: {catalog_name}")
    try:
        w = get_workspace_client()
        schemas = list(w.schemas.list(catalog_name=catalog_name))
        result = [{"name": s.name, "catalog_name": s.catalog_name, "comment": s.comment} for s in schemas]
        return json_response({"schemas": result})
    except Exception as e:
        logger.error(f"list_schemas failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def create_schema(catalog_name: str, name: str, comment: Optional[str] = None) -> str:
    """Create a schema in Unity Catalog.

    Args:
        catalog_name: Catalog name
        name: Schema name
        comment: Optional description

    Returns:
        JSON with status
    """
    logger.info(f"create_schema() called: {catalog_name}.{name}")
    try:
        w = get_workspace_client()
        w.schemas.create(catalog_name=catalog_name, name=name, comment=comment)
        return json_response({"catalog_name": catalog_name, "name": name, "status": "created"})
    except Exception as e:
        logger.error(f"create_schema failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def list_tables(catalog_name: str, schema_name: str) -> str:
    """List tables in a schema.

    Args:
        catalog_name: Catalog name
        schema_name: Schema name

    Returns:
        JSON list of tables
    """
    logger.info(f"list_tables() called for: {catalog_name}.{schema_name}")
    try:
        w = get_workspace_client()
        tables = list(w.tables.list(catalog_name=catalog_name, schema_name=schema_name))
        result = [{"name": t.name, "table_type": str(t.table_type) if t.table_type else None,
                   "full_name": t.full_name} for t in tables]
        return json_response({"tables": result})
    except Exception as e:
        logger.error(f"list_tables failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def create_table(warehouse_id: str, statement: str) -> str:
    """Create a table via SQL.

    Args:
        warehouse_id: SQL Warehouse ID
        statement: CREATE TABLE SQL statement

    Returns:
        JSON with status
    """
    logger.info("create_table() called")
    return execute_sql(statement=statement, warehouse_id=warehouse_id)


@mcp.tool()
def get_table_lineage(full_name: str) -> str:
    """Get Unity Catalog table lineage.

    Args:
        full_name: Full table name (catalog.schema.table)

    Returns:
        JSON with lineage information
    """
    logger.info(f"get_table_lineage() called for: {full_name}")
    try:
        w = get_workspace_client()
        lineage = w.lineage.get_table_lineage(full_name)
        return json_response({"full_name": full_name, "lineage": str(lineage)})
    except Exception as e:
        logger.error(f"get_table_lineage failed: {e}")
        return json_response({"error": str(e)})


# ============================================================================
# DBFS OPERATIONS
# ============================================================================

@mcp.tool()
def list_files(path: str) -> str:
    """List DBFS files for a path.

    Args:
        path: DBFS path (e.g., '/Volumes/catalog/schema/volume/')

    Returns:
        JSON list of files
    """
    logger.info(f"list_files() called for: {path}")
    try:
        w = get_workspace_client()
        files = list(w.dbfs.list(path))
        result = [{"path": f.path, "is_dir": f.is_dir, "file_size": f.file_size} for f in files]
        return json_response({"files": result})
    except Exception as e:
        logger.error(f"list_files failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def dbfs_put(path: str, content: str, overwrite: bool = True) -> str:
    """Upload small content to DBFS.

    Args:
        path: DBFS destination path
        content: Content to upload (will be base64 encoded)
        overwrite: Whether to overwrite existing file

    Returns:
        JSON with status
    """
    logger.info(f"dbfs_put() called for: {path}")
    try:
        w = get_workspace_client()
        content_bytes = content.encode("utf-8")
        w.dbfs.put(path, contents=content_bytes, overwrite=overwrite)
        return json_response({"path": path, "status": "uploaded", "size": len(content_bytes)})
    except Exception as e:
        logger.error(f"dbfs_put failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def dbfs_delete(path: str, recursive: bool = False) -> str:
    """Delete a DBFS path.

    Args:
        path: DBFS path to delete
        recursive: Delete recursively

    Returns:
        JSON with status
    """
    logger.info(f"dbfs_delete() called for: {path}")
    try:
        w = get_workspace_client()
        w.dbfs.delete(path, recursive=recursive)
        return json_response({"path": path, "status": "deleted"})
    except Exception as e:
        logger.error(f"dbfs_delete failed: {e}")
        return json_response({"error": str(e)})


# ============================================================================
# LIBRARY OPERATIONS
# ============================================================================

@mcp.tool()
def install_library(cluster_id: str, libraries_spec: str) -> str:
    """Install libraries on a cluster.

    Args:
        cluster_id: Target cluster ID
        libraries_spec: JSON array of library specs (e.g., [{"pypi": {"package": "pandas"}}])

    Returns:
        JSON with status
    """
    logger.info(f"install_library() called for cluster: {cluster_id}")
    try:
        w = get_workspace_client()
        libs = json.loads(libraries_spec)
        w.libraries.install(cluster_id, libs)
        return json_response({"cluster_id": cluster_id, "status": "installing", "libraries": libs})
    except Exception as e:
        logger.error(f"install_library failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def uninstall_library(cluster_id: str, libraries_spec: str) -> str:
    """Uninstall libraries from a cluster.

    Args:
        cluster_id: Target cluster ID
        libraries_spec: JSON array of library specs

    Returns:
        JSON with status
    """
    logger.info(f"uninstall_library() called for cluster: {cluster_id}")
    try:
        w = get_workspace_client()
        libs = json.loads(libraries_spec)
        w.libraries.uninstall(cluster_id, libs)
        return json_response({"cluster_id": cluster_id, "status": "uninstalling", "libraries": libs})
    except Exception as e:
        logger.error(f"uninstall_library failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def list_cluster_libraries(cluster_id: str) -> str:
    """List libraries for a cluster.

    Args:
        cluster_id: Cluster ID

    Returns:
        JSON list of libraries
    """
    logger.info(f"list_cluster_libraries() called for cluster: {cluster_id}")
    try:
        w = get_workspace_client()
        statuses = list(w.libraries.cluster_status(cluster_id))
        result = []
        for s in statuses:
            result.append({
                "library": str(s.library) if s.library else None,
                "status": str(s.status) if s.status else None,
            })
        return json_response({"cluster_id": cluster_id, "libraries": result})
    except Exception as e:
        logger.error(f"list_cluster_libraries failed: {e}")
        return json_response({"error": str(e)})


# ============================================================================
# REPOSITORY OPERATIONS
# ============================================================================

@mcp.tool()
def list_repos(path_prefix: Optional[str] = None) -> str:
    """List repos in the workspace.

    Args:
        path_prefix: Optional path prefix to filter repos

    Returns:
        JSON list of repos
    """
    logger.info(f"list_repos() called, path_prefix={path_prefix}")
    try:
        w = get_workspace_client()
        repos = list(w.repos.list(path_prefix=path_prefix))
        result = [{"id": r.id, "path": r.path, "url": r.url, "branch": r.branch} for r in repos]
        return json_response({"repos": result})
    except Exception as e:
        logger.error(f"list_repos failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def create_repo(url: str, provider: str, path: Optional[str] = None,
                branch: Optional[str] = None) -> str:
    """Create or clone a repo.

    Args:
        url: Git repository URL
        provider: Git provider (github, gitlab, bitbucket, etc.)
        path: Optional workspace path
        branch: Optional branch name

    Returns:
        JSON with repo details
    """
    logger.info(f"create_repo() called: {url}")
    try:
        w = get_workspace_client()
        repo = w.repos.create(url=url, provider=provider, path=path)
        if branch:
            w.repos.update(repo.id, branch=branch)
        return json_response({"id": repo.id, "path": repo.path, "url": repo.url})
    except Exception as e:
        logger.error(f"create_repo failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def update_repo(repo_id: int, branch: Optional[str] = None, tag: Optional[str] = None) -> str:
    """Update repo branch or tag.

    Args:
        repo_id: Repository ID
        branch: Optional branch to checkout
        tag: Optional tag to checkout

    Returns:
        JSON with status
    """
    logger.info(f"update_repo() called for repo: {repo_id}")
    try:
        w = get_workspace_client()
        w.repos.update(repo_id, branch=branch, tag=tag)
        return json_response({"repo_id": repo_id, "status": "updated", "branch": branch, "tag": tag})
    except Exception as e:
        logger.error(f"update_repo failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def pull_repo(repo_id: int) -> str:
    """Pull latest commit for a repo.

    Args:
        repo_id: Repository ID

    Returns:
        JSON with status
    """
    logger.info(f"pull_repo() called for repo: {repo_id}")
    try:
        w = get_workspace_client()
        # Update triggers a pull
        repo = w.repos.get(repo_id)
        w.repos.update(repo_id, branch=repo.branch)
        return json_response({"repo_id": repo_id, "status": "pulled"})
    except Exception as e:
        logger.error(f"pull_repo failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def sync_repo_and_run_notebook(repo_id: int, notebook_path: str,
                               existing_cluster_id: Optional[str] = None,
                               base_parameters: Optional[str] = None) -> str:
    """Pull a repo and run a notebook.

    Args:
        repo_id: Repository ID to sync
        notebook_path: Path to notebook in workspace
        existing_cluster_id: Optional cluster ID
        base_parameters: Optional JSON string of parameters

    Returns:
        JSON with run result
    """
    logger.info(f"sync_repo_and_run_notebook() called: repo={repo_id}, notebook={notebook_path}")
    try:
        # First pull the repo
        pull_result = json.loads(pull_repo(repo_id))
        if "error" in pull_result:
            return json_response(pull_result)

        # Then run the notebook
        return run_notebook(notebook_path, existing_cluster_id, base_parameters)
    except Exception as e:
        logger.error(f"sync_repo_and_run_notebook failed: {e}")
        return json_response({"error": str(e)})


# ============================================================================
# WORKSPACE FILE OPERATIONS
# ============================================================================

@mcp.tool()
def get_workspace_file_content(path: str, format: str = "SOURCE") -> str:
    """Retrieve workspace file content (returns content in response).

    NOTE: For large files, prefer export_notebook() which saves directly to local file.
    This function is useful when you need the content in-memory for processing.

    Args:
        path: Workspace path
        format: Export format (SOURCE, HTML, JUPYTER, DBC)

    Returns:
        JSON with file content
    """
    logger.info(f"get_workspace_file_content() called for path: {path}")
    try:
        w = get_workspace_client()
        from databricks.sdk.service.workspace import ExportFormat

        format_map = {"SOURCE": ExportFormat.SOURCE, "HTML": ExportFormat.HTML,
                      "JUPYTER": ExportFormat.JUPYTER, "DBC": ExportFormat.DBC}

        result = w.workspace.export(path=path, format=format_map.get(format.upper(), ExportFormat.SOURCE))
        content = base64.b64decode(result.content).decode("utf-8") if result.content else ""
        return json_response({"path": path, "content": content})
    except Exception as e:
        logger.error(f"get_workspace_file_content failed: {e}")
        return json_response({"error": str(e)})


@mcp.tool()
def get_workspace_file_info(path: str) -> str:
    """Retrieve workspace metadata.

    Args:
        path: Workspace path

    Returns:
        JSON with file metadata
    """
    logger.info(f"get_workspace_file_info() called for: {path}")
    try:
        w = get_workspace_client()
        info = w.workspace.get_status(path)
        return json_response({
            "path": info.path,
            "object_type": str(info.object_type) if info.object_type else None,
            "language": str(info.language) if info.language else None,
            "created_at": info.created_at,
            "modified_at": info.modified_at,
            "object_id": info.object_id,
        })
    except Exception as e:
        logger.error(f"get_workspace_file_info failed: {e}")
        return json_response({"error": str(e)})

# New functions added - 21-01-2026
# ============================================================================
# ADVANCED DEBUGGING OPERATIONS
# ============================================================================

def _get_headers() -> Dict[str, str]:
    """Get authorization headers for Databricks API calls."""
    return {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

def _api_request(method: str, endpoint: str, data: Optional[Dict] = None, params: Optional[Dict] = None) -> Dict[str, Any]:
    """Make a request to Databricks API."""
    url = f"{DATABRICKS_HOST}/api/2.1{endpoint}"
    
    try:
        if method == "GET":
            response = requests.get(url, headers=_get_headers(), params=params, timeout=60)
        elif method == "POST":
            response = requests.post(url, headers=_get_headers(), json=data, timeout=60)
        elif method == "DELETE":
            response = requests.delete(url, headers=_get_headers(), json=data, timeout=60)
        else:
            return {"error": f"Unsupported HTTP method: {method}"}
            
        response.raise_for_status()
        return response.json() if response.text else {"success": True}
        
    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP Error: {e}"
        try:
            error_detail = e.response.json()
            error_msg = f"{error_msg} - {error_detail}"
        except:
            pass
        return {"error": error_msg}
    except Exception as e:
        return {"error": str(e)}

# --- 2. Main Error Extraction Function ---
@mcp.tool()
def get_run_output(run_id: int) -> str:
    """
    Get detailed output from a Databricks job run, including error traces and notebook output.
    
    For multi-task jobs, this will first get the job run details, then retrieve output
    for each failed task.
    """
    result = {
        "run_id": run_id,
        "success": False,
        "error": None,
        "error_trace": None,
        "notebook_output": None,
        "tasks": []
    }
    
    try:
        # First, try to get output directly (works for single-task runs)
        output_response = _api_request("GET", "/jobs/runs/get-output", params={"run_id": run_id})
        
        # Check if this is a multi-task job
        if "error" in output_response and "multiple tasks" in str(output_response.get("error", "")).lower():
            run_details = _api_request("GET", "/jobs/runs/get", params={"run_id": run_id})
            
            if "error" in run_details:
                result["error"] = run_details["error"]
                return json.dumps(result, indent=2)
            
            result.update({
                "job_id": run_details.get("job_id"),
                "run_name": run_details.get("run_name"),
                "state": run_details.get("state", {})
            })
            
            # Get output for each task
            tasks = run_details.get("tasks", [])
            for task in tasks:
                task_run_id = task.get("run_id")
                task_key = task.get("task_key")
                task_state = task.get("state", {})
                
                task_result = {
                    "task_key": task_key,
                    "run_id": task_run_id,
                    "state": task_state
                }
                
                # Only get output for failed or completed tasks
                if task_state.get("result_state") in ["FAILED", "SUCCESS", "TIMEDOUT", "CANCELED"]:
                    task_output = _api_request("GET", "/jobs/runs/get-output", params={"run_id": task_run_id})
                    
                    if "error" not in task_output or "message" in task_output:
                        task_result["error"] = task_output.get("error")
                        task_result["error_trace"] = task_output.get("error_trace")
                        task_result["notebook_output"] = task_output.get("notebook_output")
                        
                        # Extract just the key error info if present
                        if task_output.get("error"):
                            result["error"] = result.get("error") or task_output.get("error")
                        if task_output.get("error_trace"):
                            result["error_trace"] = result.get("error_trace") or task_output.get("error_trace")
                
                result["tasks"].append(task_result)
            
            result["success"] = True
            
        elif "error" in output_response:
            result["error"] = output_response.get("error")
            result["message"] = output_response.get("message", "")
        else:
            # Single task job - output retrieved directly
            result.update({
                "success": True,
                "error": output_response.get("error"),
                "error_trace": output_response.get("error_trace"),
                "notebook_output": output_response.get("notebook_output"),
                "metadata": output_response.get("metadata")
            })
            
    except Exception as e:
        result["error"] = f"Failed to get run output: {type(e).__name__}: {e}"
        
    return json.dumps(result, indent=2)

# --- 3. Error Summary Function ---
@mcp.tool()
def get_run_output_summary(run_id: int) -> str:
    """Get a concise summary of errors from a job run."""
    full_output = json.loads(get_run_output(run_id))
    
    summary = {
        "run_id": run_id,
        "success": full_output.get("success"),
        "error_message": None,
        "root_cause": None,
        "failed_tasks": []
    }
    
    # Extract main error
    if full_output.get("error"):
        summary["error_message"] = full_output["error"]
        
    # Try to extract root cause from error_trace
    error_trace = full_output.get("error_trace", "")
    if error_trace and "Caused by:" in error_trace:
        causes = error_trace.split("Caused by:")
        if len(causes) > 1:
            root_cause = causes[-1].split("\n")[0].strip()
            summary["root_cause"] = root_cause
            
    # Summarize failed tasks
    for task in full_output.get("tasks", []):
        if task.get("state", {}).get("result_state") == "FAILED":
            summary["failed_tasks"].append({
                "task_key": task.get("task_key"),
                "error": task.get("error")
            })
            
    return json.dumps(summary, indent=2)


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Run the MCP server."""
    logger.info("Starting Databricks MCP server")
    logger.info(f"Host: {DATABRICKS_HOST}")
    logger.info(f"Warehouse ID: {DATABRICKS_SQL_WAREHOUSE_ID}")
    mcp.run()


if __name__ == "__main__":
    main()
