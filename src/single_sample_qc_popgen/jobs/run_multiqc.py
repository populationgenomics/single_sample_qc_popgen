#!/usr/bin/env python3

"""
Batch jobs to run MultiQC.
"""

from cpg_flow.targets import Cohort
from cpg_utils import Path, to_path
from cpg_utils.config import get_driver_image
from cpg_utils.hail_batch import get_batch
from hailtop.batch.job import BashJob
from loguru import logger

from single_sample_qc_popgen.utils import get_output_path


def run_multiqc(
    cohort: Cohort,
    outputs: dict[str, str],
) -> BashJob | None:
    """
    Creates and calls the Job to run MultiQC.
    Gathers all required QC input paths.
    """

    dragen_metric_prefixes: list[Path] = []
    somalier_paths: list[Path] = []
    for sg in cohort.get_sequencing_groups():
        # 1. Get Dragen metric directory prefixes for each SG
        dragen_prefix = get_output_path(filename=f'dragen_metrics/{sg.name}')
        dragen_metric_prefixes.append(dragen_prefix)

        # 2. Get Somalier paths for each SG
        somalier_path = get_output_path(filename=f'{sg.id}.somalier')
        somalier_paths.append(somalier_path)

    # 3. Collect all individual Dragen CSV file paths
    all_dragen_csv_paths: list[Path] = []
    for prefix in dragen_metric_prefixes:
        try:
            # Use rglob to find all CSV files recursively within the SG's metric directory
            found_paths = [to_path(p) for p in prefix.rglob('*.csv')]
            all_dragen_csv_paths.extend(found_paths)
        except FileNotFoundError:
            logger.warning(f'Directory {prefix} not found when searching for Dragen CSVs.')
        except Exception as e:  # noqa: BLE001
            logger.error(f'Error searching for CSVs in {prefix}: {e}')

    # 4. Combine Dragen CSV paths and Somalier paths
    all_qc_paths: list[Path] = all_dragen_csv_paths + somalier_paths

    if not all_qc_paths:
        logger.warning('No QC files (Dragen CSVs or Somalier) found to aggregate with MultiQC')
        return None  # Return None to signal the stage to skip

    logger.info(f'Found {len(all_qc_paths)} QC files for MultiQC aggregation.')
    if all_qc_paths:
        logger.info(f'Example QC paths: {all_qc_paths[:5]}')

    # 5. Create the Job
    b = get_batch()
    multiqc_job: BashJob = b.new_job(
        name='MultiQC',
        attributes=(cohort.get_job_attrs() or {}) | {'tool': 'MultiQC'},  # pyright: ignore[reportUnknownArgumentType]
    )
    multiqc_job.image(image=get_driver_image())
    multiqc_job.storage('10Gi')

    # Read all QC files into the job's input directory
    input_file_dict: dict[str, str] = {f'file_{i}': str(p) for i, p in enumerate(all_qc_paths)}
    b_input_dir_resource = b.read_input_group(**input_file_dict)

    report_name = f'{cohort.name}_multiqc_report'
    multiqc_job.declare_resource_group(
        out={
            'html': f'{report_name}.html',
            'json': f'{report_name}_data/multiqc_data.json',
        }
    )

    # Define the command
    multiqc_job.command(
        f"""
        multiqc \\
        {b_input_dir_resource} \\
        -o {multiqc_job.outdir} \\
        --title 'MultiQC Report for {cohort.name}' \\
        --filename '{report_name}.html' \\
        --cl-config 'max_table_rows: 10000'

        mv {multiqc_job.outdir}/{report_name}.html {multiqc_job.html}
        mv {multiqc_job.outdir}/{report_name}_data/multiqc_data.json {multiqc_job.json}
        """
    )


    # Write outputs to their final GCS locations
    b.write_output(multiqc_job.html, outputs['multiqc_report'])
    b.write_output(multiqc_job.json, outputs['multiqc_data'])

    return multiqc_job
