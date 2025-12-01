"""
Batch jobs to run MultiQC.
"""

import os

from cpg_flow.targets import Cohort
from cpg_utils import Path, to_path
from cpg_utils.config import image_path
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import BashJob
from loguru import logger

from single_sample_qc_popgen.utils import get_output_path, get_qc_path


def run_multiqc(
    cohort: Cohort,
    outputs: dict[str, Path],
) -> BashJob | None:
    """
    Creates and calls the Job to run MultiQC.
    Gathers all required QC input paths.
    """
    # 1. Collect all individual Dragen CSV file paths
    all_dragen_csv_paths: list[Path] = []
    for sg in cohort.get_sequencing_groups():
        dragen_prefix = get_output_path(filename=f'dragen_metrics/{sg.name}')

        try:
            # Use rglob to find all CSV files recursively within the SG's metric directory
            found_paths = [to_path(p) for p in dragen_prefix.rglob('*.csv')]
            all_dragen_csv_paths.extend(found_paths)
        except FileNotFoundError:
            logger.warning(f'Directory {dragen_prefix} not found when searching for Dragen CSVs.')
        except Exception as e:  # noqa: BLE001
            logger.error(f'Error searching for CSVs in {dragen_prefix}: {e}')

    # 2. Check if we found anything
    if not all_dragen_csv_paths:
        logger.warning('No QC files (Dragen CSVs) found to aggregate with MultiQC')
        return None  # Return None to signal the stage to skip

    logger.info(f'Found {len(all_dragen_csv_paths)} QC files for MultiQC aggregation.')
    logger.info(f'Example QC paths: {all_dragen_csv_paths[:5]}')

    # 3. Create the Job
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

    qc_files_path.write_text('\n'.join(str(p) for p in all_dragen_csv_paths))

    b_input_dir_resource = b.read_input(qc_files_path)
    local_metrics_dir = os.path.join(str(multiqc_job.outdir), 'metrics_input')

    report_name = f'{cohort.id}_multiqc_report'
    multiqc_job.declare_resource_group(
        out={
            'html': f'{report_name}.html',
            'json': f'{report_name}_data/multiqc_data.json',
        }
    )

    # Define the command
    multiqc_job.command(
        f"""
        mkdir -p {local_metrics_dir}

        cat {b_input_dir_resource} | xargs -P 16 -I {{}} gcloud storage cp -- {{}} {local_metrics_dir}

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
