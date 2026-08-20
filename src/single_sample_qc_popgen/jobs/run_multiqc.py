"""
Batch jobs to run MultiQC.
"""

import os
from collections import Counter
from collections.abc import Sequence

from cpg_flow.targets import Cohort
from cpg_utils import Path
from cpg_utils.config import image_path
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import BashJob
from loguru import logger

from single_sample_qc_popgen.constants import MULTIQC_INPUT_SUFFIXES
from single_sample_qc_popgen.utils import get_dragen_output_path, get_qc_path


def dragen_qc_paths(sg_names: Sequence[str]) -> list[Path]:
    """Explicit per-SG paths to the DRAGEN CSVs that MultiQC parses.

    Paths are constructed, not discovered: dragen_metrics/<SG>/ mixes two
    layout vintages (see docs/dragen-output-schema.md), and a recursive glob
    sweeps in nested duplicates whose basenames collide when staged into the
    flat MultiQC input directory. A path listed here that does not exist in
    GCS fails the copy step loudly, naming the missing object.
    """
    if not sg_names:
        raise ValueError('No sequencing groups to aggregate with MultiQC')

    paths = [
        get_dragen_output_path(filename=f'dragen_metrics/{sg_name}/{sg_name}.{suffix}')
        for sg_name in sg_names
        for suffix in MULTIQC_INPUT_SUFFIXES
    ]

    # The staging copy is flat, so duplicate basenames would silently clobber
    # each other. Impossible while SG names are unique, but assert it so the
    # guarantee is structural rather than conventional.
    duplicates = [name for name, count in Counter(p.name for p in paths).items() if count > 1]
    if duplicates:
        raise ValueError(f'Duplicate basenames would clobber each other in the MultiQC staging dir: {duplicates}')

    return paths


def run_multiqc(
    cohort: Cohort,
    outputs: dict[str, Path],
) -> BashJob:
    """
    Creates and calls the Job to run MultiQC on the explicit per-SG DRAGEN QC files.
    """
    qc_file_paths = dragen_qc_paths([sg.name for sg in cohort.get_sequencing_groups()])
    logger.info(f'Staging {len(qc_file_paths)} QC files for MultiQC aggregation.')

    b = get_batch()
    multiqc_job: BashJob = b.new_job(
        name='MultiQC',
        attributes=(cohort.get_job_attrs() or {}) | {'tool': 'MultiQC'},  # pyright: ignore[reportUnknownArgumentType]
    )
    multiqc_job.image(image=image_path('multiqc', '1.30-3'))
    multiqc_job.storage('10Gi')
    multiqc_job.cpu(8)

    # Write the list of QC file paths to a temporary input file
    qc_files_path: Path = get_qc_path(f'{cohort.name}_multiqc_input.txt', category='tmp')

    qc_files_path.write_text('\n'.join(str(p) for p in qc_file_paths))

    b_input_dir_resource = b.read_input(qc_files_path)
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

        cat {b_input_dir_resource} | gcloud storage cp -I {local_metrics_dir}

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
