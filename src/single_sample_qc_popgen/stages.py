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
from cpg_flow.workflow import get_workflow
from cpg_utils import Path
from cpg_utils.config import config_retrieve

from single_sample_qc_popgen.jobs import check_multiqc, register_qc_metamist, run_multiqc
from single_sample_qc_popgen.utils import initialise_python_job


def get_output_prefix(cohort: Cohort, stage_name: str, category: str | None = None) -> Path:
    """
    Standardised output prefix for CohortStage outputs.
    Format: cohort.dataset.prefix() / workflow.name / stage_name / cohort.id / version
    Pass category='web' for files that should be publicly accessible via the web bucket.
    """
    stage_version = config_retrieve(['workflow', 'output_versions', stage_name], None)
    version = stage_version or config_retrieve(['workflow', 'version'], 'v1')
    prefix_kwargs = {'category': category} if category else {}
    return cohort.dataset.prefix(**prefix_kwargs) / get_workflow().name / stage_name / cohort.id / version


@stage(analysis_type='qc', analysis_keys=['multiqc_json'])
class RunMultiQc(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> dict[str, cpg_utils.Path]:  # pyright: ignore[reportIncompatibleMethodOverride]
        return {
            'multiqc_json': get_output_prefix(cohort, self.name) / 'multiqc_data.json',
            'multiqc_report_html': get_output_prefix(cohort, self.name, category='web') / 'multiqc_report.html',
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
    def expected_outputs(self, cohort: Cohort) -> dict[str, cpg_utils.Path]:  # pyright: ignore[reportIncompatibleMethodOverride]
        prefix = get_output_prefix(cohort, self.name)
        return {
            'failures': prefix / f'{cohort.id}_failed_samples.json',
            'sex_imputation': prefix / f'{cohort.id}_sex_imputation.json',
        }

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)

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
            failures_output=outputs['failures'],
            sex_imputation_output=outputs['sex_imputation'],
        )

        return self.make_outputs(target=cohort, data=outputs, jobs=qc_checks_job)  # pyright: ignore[reportArgumentType]

@stage(required_stages=[RunMultiQc, CheckMultiQc])
class RegisterQcMetricsToMetamist(CohortStage):
    """
    Registers QC metrics from MultiQC in the sequencing group 'meta' field in Metamist.
    The following metrics are registered under sg.meta['qc']:

    MultiQC-derived (from build_sg_multiqc_meta_dict):
        contamination_dragen: float
        mean_coverage: float
        median_coverage: float
        pct_genome_gt_20x: float
        q30_bases_pct: float
        mapping_rate_pct: float
        chimera_alignments: float
        total_alignments: int
        pct_duplicate_reads: float
        mean_insert_size: float
        std_dev_insert_size: float
        avg_gc_content: float
        ploidy_estimation: str
        norm_x_coverage: float
        norm_y_coverage: float
        ti_tv_ratio: float
        het_hom_ratio: float

    Somalier-derived sex imputation (merged in from CheckMultiQc):
        corrected_sex_karyotype: str | None
        f_stat_raw: float
        x_het_rate: float
        n_called_x: int
        y_calls: int
        y_n: int

    QC outcome:
        qc_checks_failed: list[str]

    Optionally deactivates sequencing groups that failed QC checks. Toggleable via the following config:
        workflow.multiqc.deactivate_sgs = true
    """
    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:
        return get_output_prefix(cohort, self.name) / f'{cohort.id}_registered.json'

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        output: cpg_utils.Path = self.expected_outputs(cohort=cohort)

        register_qc_job: PythonJob = initialise_python_job(
            job_name=f'Register {cohort.id} QC Metrics',
            target=cohort,
            tool_name='Register QC Metrics',
        )

        multiqc_data_path = inputs.as_str(cohort, stage=RunMultiQc, key='multiqc_json')
        failures_path = inputs.as_str(cohort, stage=CheckMultiQc, key='failures')
        sex_imputation_path = inputs.as_str(cohort, stage=CheckMultiQc, key='sex_imputation')

        register_qc_job.call(
            register_qc_metamist.run,
            cohort=cohort,
            multiqc_data_path=multiqc_data_path,
            failures_path=failures_path,
            sex_imputation_path=sex_imputation_path,
            output=output,
        )

        return self.make_outputs(target=cohort, data=output, jobs=register_qc_job)  # pyright: ignore[reportArgumentType]
