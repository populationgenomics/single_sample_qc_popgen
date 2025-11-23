from typing import TYPE_CHECKING

import cpg_utils
from cpg_flow.stage import (
    CohortStage,
    StageInput,
    StageOutput,
    stage,
)
from cpg_flow.targets import Cohort
from loguru import logger

if TYPE_CHECKING:
    from hailtop.batch.job import BashJob, PythonJob
from single_sample_qc_popgen.jobs import check_multiqc, register_qc_metamist, run_multiqc
from single_sample_qc_popgen.utils import get_output_path, get_qc_path, initialise_python_job


@stage(analysis_type='multiqc_json', analysis_keys=['multiqc_json'])
class RunMultiQc(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> dict[str, cpg_utils.Path]:  # pyright: ignore[reportIncompatibleMethodOverride]
        return {
            'multiqc_json': get_output_path(filename=f'{cohort.id}_multiqc_data.json'),
            'multiqc_report_html': get_qc_path(filename=f'{cohort.id}_multiqc_report.html', category='web'),
        }

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None: # noqa: ARG002
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)

        multiqc_job: BashJob | None = run_multiqc.run_multiqc(
            cohort=cohort,
            outputs=outputs,
        )

        if not multiqc_job:
            logger.warning('MultiQC job was not created (no input files found). Skipping stage.')
            return self.make_outputs(cohort, skipped=True)

        return self.make_outputs(target=cohort, data=outputs, jobs=multiqc_job)  # pyright: ignore[reportArgumentType]


@stage(required_stages=[RunMultiQc])
class CheckMultiQc(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:
        return get_output_path(filename=f'{cohort.id}_failed_samples.json')

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        output: cpg_utils.Path = self.expected_outputs(cohort=cohort)

        qc_checks_job: PythonJob = initialise_python_job(
            job_name=f'Check {cohort.id} MultiQC Report',
            target=cohort,
            tool_name='Check MultiQC',
        )

        qc_checks_job.call(
            check_multiqc.run,
            cohort=cohort,
            multiqc_data_path=str(inputs.as_str(cohort, stage=RunMultiQc, key='multiqc_json')),
            multiqc_html_path=str(inputs.as_str(cohort, stage=RunMultiQc, key='multiqc_report_html')),
            output=output,
        )

        return self.make_outputs(target=cohort, data=output, jobs=qc_checks_job)  # pyright: ignore[reportArgumentType]

@stage(required_stages=[RunMultiQc, CheckMultiQc])
class RegisterQcMetricsToMetamist(CohortStage):
    """
    Registers QC metrics from MultiQC in the sequencing group 'meta' field in Metamist.
    The following metrics are registered:
        contamination_dragen: float
        mean_coverage: float
        median_coverage: float
        pct_genome_20x: float
        pct_q30_bases: float
        pct_mapped_reads: float
        pct_duplicate_reads: float
        mean_insert_size: float
        std_dev_insert_size: float
        avg_gc_content: float
        ploidy_estimation: str,
        norm_x_coverage: float,
        norm_y_coverage: float,
        ti_tv_ratio: float,
        het_hom_ratio: float,
        qc_checks_failed: list[str]

    Optionally deactivates sequencing groups that failed QC checks. Toggleable via the following config:
        workflow.multiqc.deactivate_sgs = true
    """
    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:
        return get_output_path(filename=f'{cohort.id}_registered.json')

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        output: cpg_utils.Path = self.expected_outputs(cohort=cohort)

        register_qc_job: PythonJob = initialise_python_job(
            job_name=f'Register {cohort.id} QC Metrics',
            target=cohort,
            tool_name='Register QC Metrics',
        )

        multiqc_data_path = inputs.as_str(cohort, stage=RunMultiQc, key='multiqc_json')
        failed_samples_path = inputs.as_str(cohort, stage=CheckMultiQc)

        register_qc_job.call(
            register_qc_metamist.run,
            cohort=cohort,
            multiqc_data_path=multiqc_data_path,
            failed_samples_path=failed_samples_path,
            output=output,
        )

        return self.make_outputs(target=cohort, data=output, jobs=register_qc_job)  # pyright: ignore[reportArgumentType]
