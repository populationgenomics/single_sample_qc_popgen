"""
Batch jobs to run MultiQC.
"""

import os
from collections import Counter
from collections.abc import Sequence
from typing import NamedTuple

from cpg_flow.targets import Cohort
from cpg_utils import Path
from cpg_utils.config import image_path
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import BashJob
from loguru import logger

from single_sample_qc_popgen.constants import get_sequencing_type_profile
from single_sample_qc_popgen.utils import get_cohort_sequencing_type, get_dragen_output_path, get_qc_path


class MultiqcInputPaths(NamedTuple):
    """Per-SG DRAGEN CSV paths for MultiQC, split by what a missing file means.

    Attributes:
        required: Files that feed a QC threshold, a registered metric or a
            calculator. A missing file fails the MultiQC job.
        report_only: Files that only feed MultiQC plots and tables. A missing
            file is logged and skipped.
    """

    required: list[Path]
    report_only: list[Path]


def dragen_qc_paths(sg_names: Sequence[str], sequencing_type: str) -> MultiqcInputPaths:
    """Builds the explicit per-SG paths to the DRAGEN CSVs that MultiQC parses.

    Args:
        sg_names: Sequencing group names in the cohort.
        sequencing_type: The workflow.sequencing_type, which selects the suffix lists.

    Returns:
        One path per sequencing group per suffix, in the sequencing type's
        required_input_suffixes (required) or report_only_input_suffixes
        (report_only).

    Raises:
        ValueError: If sg_names is empty, if sequencing_type is unsupported, or
            if two paths in required and report_only combined share a basename
            (duplicates would clobber each other in the flat staging directory).
    """
    profile = get_sequencing_type_profile(sequencing_type)
    if not sg_names:
        raise ValueError('No sequencing groups to aggregate with MultiQC')

    def paths_for(suffixes: Sequence[str]) -> list[Path]:
        return [
            get_dragen_output_path(filename=f'dragen_metrics/{sg_name}/{sg_name}.{suffix}')
            for sg_name in sg_names
            for suffix in suffixes
        ]

    paths = MultiqcInputPaths(
        required=paths_for(profile.required_input_suffixes),
        report_only=paths_for(profile.report_only_input_suffixes),
    )

    basename_counts = Counter(p.name for p in paths.required + paths.report_only)
    duplicates = [name for name, count in basename_counts.items() if count > 1]
    if duplicates:
        raise ValueError(f'Duplicate basenames would clobber each other in the MultiQC staging dir: {duplicates}')

    return paths


def stage_inputs_command(required_list: str, report_only_list: str, local_dir: str) -> str:
    """Builds the bash that copies the MultiQC input files into local_dir.

    The snippet assumes `set -euo pipefail`. A missing required file fails it.
    A missing report-only file is logged and skipped.

    `gcloud storage cp -I` copies every listed object that exists and then
    exits non-zero with an error naming the objects that matched nothing, even
    with CLOUDSDK_STORAGE_PROCESS_COUNT and CLOUDSDK_STORAGE_THREAD_COUNT above 1.
    So `|| true` after the report-only copy skips missing files without
    stopping the others. `--continue-on-error` is not used: gcloud ignores it
    unless both counts are 1.

    Args:
        required_list: Path of the file listing the required object URLs, one per line.
        report_only_list: Path of the file listing the report-only object URLs.
        local_dir: Existing directory to copy the files into, flat by basename.
    """
    return f"""
        # Required inputs: any failure fails the job.
        cat {required_list} | gcloud storage cp -I {local_dir}

        # Report-only inputs: tolerate failures, then log what did not arrive.
        cat {report_only_list} | gcloud storage cp -I {local_dir} || true

        report_only_total=0
        report_only_missing=0
        while IFS= read -r report_only_path || [ -n "$report_only_path" ]; do
            report_only_total=$((report_only_total + 1))
            if [ ! -e "{local_dir}/$(basename "$report_only_path")" ]; then
                echo "WARNING: report-only input missing, skipped: $(basename "$report_only_path")" >&2
                report_only_missing=$((report_only_missing + 1))
            fi
        done < {report_only_list}
        echo "Report-only inputs: $report_only_missing of $report_only_total missing and skipped"
        """


def run_multiqc(
    cohort: Cohort,
    outputs: dict[str, Path],
) -> BashJob:
    """Creates the batch job that runs MultiQC on the per-SG DRAGEN QC files."""
    qc_file_paths = dragen_qc_paths(
        [sg.name for sg in cohort.get_sequencing_groups()],
        get_cohort_sequencing_type(cohort),
    )
    logger.info(
        f'Staging {len(qc_file_paths.required)} required and {len(qc_file_paths.report_only)} report-only '
        'QC files for MultiQC aggregation.'
    )

    b = get_batch()
    multiqc_job: BashJob = b.new_job(
        name='MultiQC',
        attributes=(cohort.get_job_attrs() or {}) | {'tool': 'MultiQC'},  # pyright: ignore[reportUnknownArgumentType]
    )
    multiqc_job.image(image=image_path('multiqc', '1.30-3'))
    multiqc_job.storage('10Gi')
    multiqc_job.cpu(8)

    # Write the lists of QC file paths to temporary input files
    required_list_path: Path = get_qc_path(f'{cohort.name}_multiqc_required_input.txt', category='tmp')
    report_only_list_path: Path = get_qc_path(f'{cohort.name}_multiqc_report_only_input.txt', category='tmp')

    required_list_path.write_text('\n'.join(str(p) for p in qc_file_paths.required))
    report_only_list_path.write_text('\n'.join(str(p) for p in qc_file_paths.report_only))

    required_list_resource = b.read_input(required_list_path)
    report_only_list_resource = b.read_input(report_only_list_path)
    local_metrics_dir = os.path.join(str(multiqc_job.outdir), 'metrics_input')

    report_name = f'{cohort.id}_multiqc_report'
    multiqc_job.declare_resource_group(
        out={
            'html': f'{report_name}.html',
            'json': f'{report_name}_data/multiqc_data.json',
        }
    )

    # CLOUDSDK_STORAGE_* match the job's 8 CPUs: the inputs are thousands of
    # small CSVs, so the copy is bound by request latency, not bandwidth.
    multiqc_job.command(
        f"""
        set -euo pipefail

        export CLOUDSDK_STORAGE_PROCESS_COUNT=8
        export CLOUDSDK_STORAGE_THREAD_COUNT=8

        mkdir -p {local_metrics_dir}

        gcloud auth list > /dev/null
        {stage_inputs_command(str(required_list_resource), str(report_only_list_resource), local_metrics_dir)}
        multiqc \\
        {local_metrics_dir} \\
        -o {multiqc_job.outdir} \\
        --title 'MultiQC Report for {cohort.name}' \\
        --filename '{report_name}.html' \\
        --cl-config 'max_table_rows: 10000'

        mv {multiqc_job.outdir}/{report_name}.html {multiqc_job.html}
        mv {multiqc_job.outdir}/{report_name}_data/multiqc_data.json {multiqc_job.json}
        """
    )

    # Write outputs to their final GCS locations
    b.write_output(multiqc_job.html, str(outputs['multiqc_report_html']))
    b.write_output(multiqc_job.json, str(outputs['multiqc_json']))

    return multiqc_job
