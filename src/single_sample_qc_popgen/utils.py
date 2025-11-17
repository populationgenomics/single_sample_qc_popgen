import json
import re
import subprocess
from math import ceil
from typing import Any

import cpg_utils
from cloudpathlib.exceptions import CloudPathFileNotFoundError
from cpg_flow.targets import Cohort, SequencingGroup
from cpg_utils.config import get_driver_image, output_path
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from loguru import logger

from single_sample_qc_popgen.constants import DRAGEN_VERSION


def validate_cli_path_input(path: str, arg_name: str) -> None:
    """
    Validates that a path string does not contain shell metacharacters
    to prevent potential injection vulnerabilities.
    """
    # Regex for common shell metacharacters and whitespace,
    # excluding GCS 'gs://' prefix, path slashes '/', and underscores '_'
    if re.search(r'[;&|$`(){}[\]<>*?!#\s]', path):
        logger.error(f'Invalid characters found in {arg_name}: {path}')
        raise ValueError(f'Potential unsafe characters in {arg_name}')
    logger.info(f'Path validation passed for {arg_name}.')


def delete_pipeline_id_file(pipeline_id_file: str) -> None:
    logger.info(f'Deleting the pipeline run ID file {pipeline_id_file}')
    subprocess.run(  # noqa: S603
        ['gcloud', 'storage', 'rm', pipeline_id_file],  # noqa: S607
        check=True,
    )


def calculate_needed_storage(
    cram_path: cpg_utils.Path,
) -> str:
    logger.info(f'Checking blob size for {cram_path}')

    storage_size: int = cram_path.stat().st_size
    # Added a buffer (3GB) and increased multiplier slightly (1.2 -> 1.3)
    # Ceil ensures we get whole GiB, adding buffer helps avoid edge cases
    calculated_gb = ceil((storage_size / (1024**3)) + 3) * 1.3
    # Ensure a minimum storage request (e.g., 10GiB)
    final_storage_gb = max(10, ceil(calculated_gb))
    logger.info(f'Calculated storage need: {final_storage_gb}GiB for {cram_path}')
    return f'{final_storage_gb}Gi'


def run_subprocess_with_log(
    cmd: str | list[str],
    step_name: str,
    stdin_input: str | None = None,
    shell: bool = False,
) -> subprocess.CompletedProcess[Any]:
    """
    Runs a subprocess command with robust logging.
    Logs the command, its output, and errors if any occur.
    """
    cmd_str = cmd if isinstance(cmd, str) else ' '.join(cmd)
    executable = '/bin/bash' if shell else None
    logger.info(f'Running {step_name} command: {cmd_str}')
    try:
        process: subprocess.CompletedProcess[str] = subprocess.run(  # noqa: S603
            cmd,
            check=True,
            capture_output=True,
            text=True,
            input=stdin_input,
            shell=shell,
            executable=executable,
        )
        logger.info(f'{step_name} completed successfully.')
        if process.stdout:
            logger.info(f'{step_name} STDOUT:\n{process.stdout.strip()}')
        if process.stderr:
            logger.info(f'{step_name} STDERR:\n{process.stderr.strip()}')
        return process
    except subprocess.CalledProcessError as e:
        logger.error(f'{step_name} failed with return code {e.returncode}')
        logger.error(f'CMD: {cmd_str}')
        logger.error(f'STDOUT: {e.stdout}')
        logger.error(f'STDERR: {e.stderr}')
        raise


def initialise_python_job(
    job_name: str,
    target: Cohort | SequencingGroup,
    tool_name: str,
) -> PythonJob:
    """
    Initialises a standard PythonJob with common attributes.
    """
    py_job: PythonJob = get_batch().new_python_job(
        name=job_name,
        attributes=(target.get_job_attrs() or {}) | {'tool': tool_name},  # pyright: ignore[reportUnknownArgumentType]
    )
    py_job.image(get_driver_image())
    return py_job


def get_prep_path(filename: str) -> cpg_utils.Path:
    """Gets a path in the 'prepare' directory."""
    return cpg_utils.to_path(output_path(f'ica/{DRAGEN_VERSION}/prepare/{filename}'))


def get_pipeline_path(filename: str) -> cpg_utils.Path:
    """Gets a path in the 'pipelines' (state) directory."""
    return cpg_utils.to_path(output_path(f'ica/{DRAGEN_VERSION}/pipelines/{filename}'))


def get_output_path(filename: str, category: str | None = None) -> cpg_utils.Path:
    """Gets a path in the final 'output' directory."""
    return cpg_utils.to_path(output_path(f'ica/{DRAGEN_VERSION}/output/{filename}', category=category))


def get_qc_path(filename: str, category: str | None = None) -> cpg_utils.Path:
    """Gets a path in the 'qc' directory."""
    return cpg_utils.to_path(output_path(f'ica/{DRAGEN_VERSION}/qc/{filename}', category=category))

def load_json(path: cpg_utils.Path | str, extract_key: str | None = None, allow_missing: bool = False) -> Any:
    """
    Generic function to load JSON data from a cpg_utils.path.

    Args:
        path (Path | str): Path to the JSON file.
        extract_key (str, optional): If provided, only return this specific
                                     top-level key from the loaded dictionary.
                                     Defaults to None (returns everything).
        allow_missing (bool, optional): If True, missing files will return
                                        an empty dict instead of raising an error. Defaults to False.
                                        To catch case where there are no failed samples.

    Returns:
        Any: The loaded JSON data (or the specific sub-section).
    """
    # Ensure we have a Path object (supports cloud paths if using cpg_utils.Path)
    if isinstance(path, str):
        path = cpg_utils.to_path(path)

    logger.info(f"Loading JSON data from: {path}")

    try:
        with path.open() as f:
            data = json.load(f)
    except (FileNotFoundError, CloudPathFileNotFoundError):
         if allow_missing:
            # Log a warning instead of an error and return default
            logger.warning(f"File not found (as allowed): {path}. Returning empty dict.")
            return {}  # <-- Return an empty dict

         # If not allowed, re-raise the error as before
         logger.error(f"JSON file not found at: {path}")
         raise
    except json.JSONDecodeError:
         logger.error(f"Failed to decode JSON from: {path}")
         raise

    if extract_key:
        if not isinstance(data, dict):
             logger.warning(
                 f"Requested key '{extract_key}' cannot be extracted: "
                 f"loaded data from {path} is not a dictionary."
             )
             return data

        # Use .get() to avoid KeyErrors if the specific key is missing in a valid JSON
        return data.get(extract_key, {})

    return data
