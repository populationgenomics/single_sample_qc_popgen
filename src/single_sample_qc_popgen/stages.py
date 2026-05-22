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

from single_sample_qc_popgen.jobs import check_multiqc, check_swap, register_qc_metamist, run_multiqc, swap_check
from single_sample_qc_popgen.metamist_utils import (
    classify_wgs_to_array_mapping,
    query_wgs_to_array_mapping,
    read_psam_array_sgs,
)
from single_sample_qc_popgen.utils import get_dragen_output_path, initialise_python_job


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


@stage(required_stages=[CheckMultiQc])
class SwapCheck(CohortStage):
    """
    WGS↔array sample-swap detection via somalier.

    Each cohort WGS SG is checked against its expected popgen-genotyping array
    SG (joined via shared sample in metamist). The expected array SG must be
    present in the latest popgen-genotyping ``array_aggregate_pgen`` export
    (path comes from ``[workflow.swap_check].pgen_path`` -- not metamist-
    registered yet). WGS SGs whose expected array SG hasn't landed in the
    pgen yet are flagged ``array_pending_export`` and skipped; re-running the
    stage after the next pgen export will pick them up.

    Pipeline:
      1. Queue-time: metamist query + pgen psam read → classified mapping.
      2. plink2 BashJob: subset pgen to the cohort's array SGs → array VCF
         (Hail Batch resource, no GCS round-trip).
      3. somalier BashJob: extract on array VCF, localise upstream WGS
         sketches via gcloud, ``somalier relate`` all-vs-all → pairs.tsv.
      4. PythonJob: parse pairs.tsv, join against mapping → swap_check.json.

    The two job split exists because plink2 and somalier ship as separate
    single-tool images; neither image has both binaries.

    A detected swap (``status == 'swap_detected'``) is a labelling problem,
    not a quality problem — it deliberately does NOT feed
    ``workflow.multiqc.deactivate_sgs``. Resolution is human review +
    metamist re-link.
    """
    def expected_outputs(self, cohort: Cohort) -> dict[str, cpg_utils.Path]:  # pyright: ignore[reportIncompatibleMethodOverride]
        prefix = get_output_prefix(cohort, self.name)
        return {
            'mapping': prefix / f'{cohort.id}_swap_mapping.json',
            'pairs_tsv': prefix / f'{cohort.id}.pairs.tsv',
            'swap_check': prefix / f'{cohort.id}_swap_check.json',
        }

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:  # noqa: ARG002
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)

        # 1. Pull required pgen paths from the user's analysis-runner config.
        #    The rolling array_aggregate_pgen is intentionally NOT in metamist
        #    (see metamist_utils docstring), so the user must supply the path
        #    explicitly. No defaults: config_retrieve raises if missing.
        pgen_path = config_retrieve(['workflow', 'swap_check', 'pgen_path'])
        pvar_path = config_retrieve(['workflow', 'swap_check', 'pvar_path'])
        psam_path = config_retrieve(['workflow', 'swap_check', 'psam_path'])

        # 2. Queue-time: metamist → records → classify against the psam.
        mapping_records = query_wgs_to_array_mapping(cohort)
        pgen_psam_sgs = read_psam_array_sgs(psam_path)
        classified = classify_wgs_to_array_mapping(mapping_records, pgen_psam_sgs)

        ready_records = [r for r in classified if r['status'] == 'ready']

        # Always persist the classified mapping so RegisterQcMetricsToMetamist
        # can surface mapping-layer statuses (array_pending_export etc.) even
        # when no somalier comparison runs.
        swap_check.write_mapping_for_python_job(classified, outputs['mapping'])

        if not ready_records:
            logger.warning(
                f'[SwapCheck] cohort={cohort.id} has no ready WGS↔array pairs '
                f'(classified statuses: {sorted({r["status"] for r in classified})}). '
                f'Skipping plink/somalier jobs; mapping JSON still written.',
            )
            # Touch the downstream output files so the stage produces all
            # expected paths -- empty pairs / empty swap_check map.
            with outputs['pairs_tsv'].open('w') as f:
                f.write('#sample_a\tsample_b\trelatedness\tibs0\tn\n')
            with outputs['swap_check'].open('w') as f:
                f.write('{}\n')
            return self.make_outputs(target=cohort, data=outputs)

        # 3. Queue-time: write the plink keep file and the WGS sketch manifest
        #    to the cohort tmp bucket; both are read inside the worker
        #    containers via Batch resource staging. Sketch paths are built
        #    via get_dragen_output_path (same helper used by sex_imputation)
        #    so the layout `ica/<dragen_version>/output/somalier/<sg>.somalier`
        #    stays in one place.
        tmp_prefix = cohort.dataset.tmp_prefix() / get_workflow().name / self.name / cohort.id
        keep_path = tmp_prefix / f'{cohort.id}_array_keep.tsv'
        manifest_path = tmp_prefix / f'{cohort.id}_wgs_sketch_manifest.txt'
        sketch_paths = [
            str(get_dragen_output_path(f'somalier/{r["wgs_sg"]}.somalier'))
            for r in ready_records
        ]
        swap_check.write_array_keep_file(ready_records, keep_path)
        swap_check.write_wgs_sketch_manifest(sketch_paths, manifest_path)

        # 4. plink2 BashJob → array VCF resource (no GCS round-trip).
        plink_job, array_vcf = swap_check.queue_plink_subset_to_vcf(
            cohort=cohort,
            pgen_path=pgen_path,
            pvar_path=pvar_path,
            psam_path=psam_path,
            keep_iids_path=keep_path,
        )

        # 5. somalier BashJob → pairs.tsv → GCS.
        somalier_job = swap_check.queue_somalier_extract_and_relate(
            cohort=cohort,
            array_vcf=array_vcf,
            wgs_manifest_path=manifest_path,
            output_pairs_tsv=outputs['pairs_tsv'],
        )
        somalier_job.depends_on(plink_job)

        # 6. PythonJob: classify pairs.tsv + mapping → swap_check.json.
        check_swap_job: PythonJob = initialise_python_job(
            job_name=f'Check {cohort.id} Sample Swaps',
            target=cohort,
            tool_name='Check Swap',
        )
        check_swap_job.depends_on(somalier_job)
        check_swap_job.call(
            check_swap.run,
            cohort=cohort,
            mapping_path=str(outputs['mapping']),
            pairs_tsv_path=str(outputs['pairs_tsv']),
            output=outputs['swap_check'],
        )

        return self.make_outputs(
            target=cohort,
            data=outputs,
            jobs=[plink_job, somalier_job, check_swap_job],  # pyright: ignore[reportArgumentType]
        )


@stage(required_stages=[RunMultiQc, CheckMultiQc, SwapCheck])
class RegisterQcMetricsToMetamist(CohortStage):
    """
    Registers QC metrics from MultiQC in the sequencing group 'meta' field in Metamist.
    The following metrics are registered:
        contamination_dragen: float
        mean_coverage: float
        median_coverage: float
        pct_genome_gt_20x: float
        pct_q30_bases: float
        q30_bases_pct: float
        mapping_rate_pct: float
        chimera_alignments: float
        total_alignments: int
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
        swap_check_path = inputs.as_str(cohort, stage=SwapCheck, key='swap_check')

        register_qc_job.call(
            register_qc_metamist.run,
            cohort=cohort,
            multiqc_data_path=multiqc_data_path,
            failures_path=failures_path,
            sex_imputation_path=sex_imputation_path,
            swap_check_path=swap_check_path,
            output=output,
        )

        return self.make_outputs(target=cohort, data=output, jobs=register_qc_job)  # pyright: ignore[reportArgumentType]
