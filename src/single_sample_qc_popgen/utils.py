import json
import math
from typing import Any

import cpg_utils
from cloudpathlib.exceptions import CloudPathFileNotFoundError
from cpg_flow.targets import Cohort, SequencingGroup
from cpg_utils.config import config_retrieve, get_driver_image, output_path
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


def get_cohort_sequencing_type(cohort: Cohort) -> str:
    """workflow.sequencing_type, checked against every SG's type in metamist.

    Raises:
        ValueError: If any SG in the cohort has a different sequencing type.
    """
    sequencing_type = config_retrieve(['workflow', 'sequencing_type'])
    mismatched = sorted(
        f'{sg.id} ({sg.sequencing_type})'
        for sg in cohort.get_sequencing_groups()
        if sg.sequencing_type != sequencing_type
    )
    if mismatched:
        raise ValueError(f'workflow.sequencing_type is {sequencing_type!r} but these SGs differ: {mismatched}')
    return sequencing_type


def get_dragen_output_path(filename: str, category: str | None = None) -> cpg_utils.Path:
    """Gets a path in the DRAGEN 'output' directory (`ica/<dragen_version>/output/...`)."""
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


def null_non_finite_floats(value: Any, sg_id: str, path: str) -> Any:
    """Returns value with every NaN or +/-inf float, at any depth, replaced by None.

    The metamist client serialises with requests, which raises InvalidJSONError
    ("Out of range float values are not JSON compliant") on NaN or inf, and the
    job would die part-way through the cohort. None is JSON null, which
    metamist's JSON_MERGE_PATCH treats as "remove key": correct for a value
    that could not be computed (e.g. f_stat_raw when somalier called no chrX
    sites), because it also clears a stale value from an earlier run.

    Each replacement is logged as a WARNING naming the SG and the key path.

    Args:
        value: A scalar, or dicts and lists nested to any depth.
        sg_id: The sequencing group the value belongs to, for the log.
        path: Dotted key path of value so far, for the log.

    Returns:
        A copy with the replacements made; containers are rebuilt, not mutated.
    """
    if isinstance(value, float) and not math.isfinite(value):
        logger.warning(f'SG {sg_id}: sending null instead of non-finite value {value} at {path}')
        return None
    if isinstance(value, dict):
        return {key: null_non_finite_floats(item, sg_id, f'{path}.{key}') for key, item in value.items()}
    if isinstance(value, list):
        return [null_non_finite_floats(item, sg_id, f'{path}[{i}]') for i, item in enumerate(value)]
    return value


def resolve_metric_sections(general_stats: dict[str, Any], metric_keys: list[str]) -> dict[str, str]:
    """Maps each metric name to the single general-stats section containing it.

    MultiQC keys report_general_stats_data by module anchor plus an
    auto-incrementing suffix (DRAGEN, DRAGEN_3, ...) whose numbering depends
    on the input file mix, so sections are resolved by content instead of by
    positional key.

    Args:
        general_stats: The report_general_stats_data mapping from multiqc_data.json.
        metric_keys: Metric names to resolve.

    Returns:
        Mapping of metric name to section key.

    Raises:
        ValueError: If any metric appears in zero or in more than one section.
    """
    errors: list[str] = []
    section_by_metric: dict[str, str] = {}
    for metric_key in metric_keys:
        hits = [
            section_key
            for section_key, val_by_metric_by_sample in general_stats.items()
            if any(metric_key in val_by_metric for val_by_metric in val_by_metric_by_sample.values())
        ]
        if len(hits) == 1:
            section_by_metric[metric_key] = hits[0]
        elif not hits:
            # Absent for every sample: the wrong report or sequencing-type profile, not a sample
            # lacking a value, and nothing could be checked or registered for this metric.
            errors.append(f'{metric_key!r} is in no section (wrong report or sequencing-type profile?)')
        else:
            errors.append(f'{metric_key!r} found in {len(hits)} sections {hits}')
    if errors:
        raise ValueError(
            'Could not resolve MultiQC general-stats sections '
            f'(available sections: {list(general_stats)}): ' + '; '.join(errors)
        )
    return section_by_metric
