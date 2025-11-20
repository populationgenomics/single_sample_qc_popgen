import json
from typing import Any

import cpg_utils
from cloudpathlib.exceptions import CloudPathFileNotFoundError
from cpg_flow.targets import Cohort, SequencingGroup
from cpg_utils.config import get_driver_image, output_path
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import PythonJob
from loguru import logger

from single_sample_qc_popgen.constants import DRAGEN_VERSION


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
            return {}

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
