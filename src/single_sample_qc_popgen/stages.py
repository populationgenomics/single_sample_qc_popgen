"""
This file exists to define all the Stages for the workflow.
The logic for each stage can be contained here (if it is not too complex),
or can be delegated to a separate file in jobs.

Naming conventions for Stages are not enforced, but a series of recommendations have been made here:

https://cpg-populationanalysis.atlassian.net/wiki/spaces/ST/pages/185597962/Pipeline+Naming+Convention+Specification

A suggested naming convention for a stages is:
  - PascalCase (each word capitalized, no hyphens or underscores)
  - If the phrase contains an initialism (e.g. VCF), only the first character should be capitalised
  - Verb + Subject (noun) + Preposition + Direct Object (noun)  TODO(anyone): please correct my grammar is this is false
  e.g. AlignShortReadsWithBowtie2, or MakeSitesOnlyVcfWithBcftools
  - This becomes self-explanatory when reading the code and output folders

Each Stage should be a Class, and should inherit from one of
  - SequencingGroupStage
  - DatasetStage
  - CohortStage
  - MultiCohortStage
"""


from typing import TYPE_CHECKING

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


@stage()
class RunMultiQc(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> dict[str, str]:  # pyright: ignore[reportIncompatibleMethodOverride]
        return {
            'multiqc_json': str(get_output_path(filename=f'{cohort.id}_multiqc_data.json')),
            'multiqc_report': str(get_qc_path(filename=f'{cohort.id}_multiqc_report.html', category='web')),
        }

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None: # noqa: ARG002
        outputs: dict[str, str] = self.expected_outputs(cohort=cohort)

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
    def expected_outputs(self, cohort: Cohort) -> dict[str, str]:
        return {'failed_samples': str(get_output_path(filename=f'{cohort.id}_failed_samples.json'))}

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        outputs: dict[str, str] = self.expected_outputs(cohort=cohort)

        qc_checks_job: PythonJob = initialise_python_job(
            job_name=f'Check {cohort.id} MultiQC Report',
            target=cohort,
            tool_name='Check MultiQC',
        )
        qc_checks_job.call(
            check_multiqc.run,
            cohort=cohort,
            inputs=inputs,
            outputs=outputs,
        )

        return self.make_outputs(target=cohort, data=outputs, jobs=qc_checks_job)  # pyright: ignore[reportArgumentType]


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
    def expected_outputs(self, cohort: Cohort) -> dict[str, str]:
        return {'.registered': str(get_output_path(filename=f'{cohort.id}_registered.json'))}

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:

        register_qc_job: PythonJob = initialise_python_job(
            job_name=f'Register {cohort.id} QC Metrics',
            target=cohort,
            tool_name='Register QC Metrics',
        )

        # Cannot pass a StageInput object (or a JobResourceFile from inputs.as_path())
        # as an argument to a PythonJob's .call(). Load the JSONs within queue_jobs and pass the data instead.
        multiqc_data_path = inputs.as_path_by_target(stage=RunMultiQc, key='multiqc_json')
        failed_samples_path = inputs.as_path_by_target(stage=CheckMultiQc, key='failed_samples')
        register_qc_job.call(
            register_qc_metamist.run,
            cohort=cohort,
            multiqc_data_path=str(multiqc_data_path),
            failed_samples_path=str(failed_samples_path),
        )

        return self.make_outputs(target=cohort, data={}, jobs=register_qc_job)  # pyright: ignore[reportArgumentType]
