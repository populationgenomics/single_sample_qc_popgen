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


from cpg_flow.stage import (
    CohortStage,
    StageInput,
    StageOutput,
    stage,
)
from cpg_flow.targets import Cohort
from hailtop.batch.job import BashJob, PythonJob
from loguru import logger

from single_sample_qc_popgen.jobs import check_multiqc, run_multiqc
from single_sample_qc_popgen.utils import get_output_path, get_qc_path


@stage(analysis_type='qc', analysis_keys=['json'])
class RunMultiQc(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> dict[str, str]:  # pyright: ignore[reportIncompatibleMethodOverride]
        return {
            'multiqc_data': str(get_output_path(filename=f'{cohort.name}_multiqc_data.json')),
            'multiqc_report': str(get_qc_path(filename=f'{cohort.name}_multiqc_report.html', category='web')),
        }

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
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
    def expected_outputs(self, cohort: Cohort, inputs: StageInput) -> dict[str, str]:
        return {'failed_samples': str(get_output_path(filename=f'{cohort.name}_failed_samples.json'))}

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        outputs: dict[str, str] = self.expected_outputs(cohort=cohort, inputs=inputs)

        qc_checks_job: PythonJob = check_multiqc.check_multiqc(
            cohort=cohort,
            inputs=inputs,
            outputs=outputs,
        )

        return self.make_outputs(target=cohort, data=outputs, jobs=qc_checks_job)  # pyright: ignore[reportArgumentType]
