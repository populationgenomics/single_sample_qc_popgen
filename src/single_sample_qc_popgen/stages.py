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

from single_sample_qc_popgen.jobs import (
    check_multiqc,
    classify_swaps,
    export_array_vcf,
    register_qc_metamist,
    run_multiqc,
    somalier_relate,
)
from single_sample_qc_popgen.metamist_utils import (
    classify_wgs_to_array_mapping,
    query_wgs_to_array_mapping,
    read_psam_array_sgs,
)
from single_sample_qc_popgen.utils import get_dragen_output_path, initialise_python_job, load_json


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


_EMPTY_PAIRS_HEADER = '#sample_a\tsample_b\trelatedness\tibs0\tn\n'


def _log_no_ready_records_warning(cohort_id: str, classified: list[dict]) -> None:
    """Loud warning for the 0-ready edge case.

    Suspicious enough that the operator should investigate before re-running:
    either the cohort is wrong, the rolling pgen is stale, or array data
    hasn't been ingested at all. Downstream swap-check stages no-op silently
    in this case and ``swap_check.json`` is left empty so the register stage
    skips the swap_check field on every SG.
    """
    status_counts: dict[str, int] = {}
    for r in classified:
        status_counts[r['status']] = status_counts.get(r['status'], 0) + 1
    logger.warning(
        f'⚠️  [SwapCheck] cohort={cohort_id}: 0 / {len(classified)} WGS SGs are ready for swap-check.\n'
        f'    Status breakdown: {status_counts}.\n'
        f'    Likely causes: wrong cohort, stale rolling pgen, or array data not yet ingested.\n'
        f'    Downstream swap-check stages will no-op; swap_check facts will NOT be registered to metamist.\n'
        f'    Other QC metrics (MultiQC, sex imputation) will still register normally.',
    )


@stage(required_stages=[CheckMultiQc])
class SwapCheckExportVcf(CohortStage):
    """
    First of three swap-check stages. Resolves the WGS↔array mapping at
    queue time (metamist query joined against the rolling popgen-genotyping
    pgen psam), persists the classified mapping JSON, and queues the
    plink2 BashJob that subsets the rolling pgen to the cohort's array SGs.

    The rolling ``array_aggregate_pgen`` is not registered in metamist, so
    ``[workflow.swap_check].pgen_path / pvar_path / psam_path`` must be
    supplied in the user's analysis-runner config (no defaults — missing
    keys raise at queue time).

    If 0 / N WGS SGs are ``ready`` (the suspicious edge case — wrong cohort
    or stale pgen), this stage logs a loud warning and the downstream
    swap-check stages no-op silently. swap_check facts won't be registered
    to metamist; other QC fields (MultiQC, sex imputation) still register
    normally.
    """
    def expected_outputs(self, cohort: Cohort) -> dict[str, cpg_utils.Path]:  # pyright: ignore[reportIncompatibleMethodOverride]
        prefix = get_output_prefix(cohort, self.name)
        return {
            'mapping': prefix / f'{cohort.id}_swap_mapping.json',
            'array_vcf': prefix / f'{cohort.id}_array.vcf.gz',
        }

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:  # noqa: ARG002
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)

        pgen_path = config_retrieve(['workflow', 'swap_check', 'pgen_path'])
        pvar_path = config_retrieve(['workflow', 'swap_check', 'pvar_path'])
        psam_path = config_retrieve(['workflow', 'swap_check', 'psam_path'])

        mapping_records = query_wgs_to_array_mapping(cohort)
        pgen_psam_sgs = read_psam_array_sgs(psam_path)
        classified = classify_wgs_to_array_mapping(mapping_records, pgen_psam_sgs)
        export_array_vcf.write_mapping_for_python_job(classified, outputs['mapping'])

        ready_records = [r for r in classified if r['status'] == 'ready']

        if not ready_records:
            _log_no_ready_records_warning(cohort.id, classified)
            outputs['array_vcf'].touch()
            return self.make_outputs(target=cohort, data=outputs)

        tmp_prefix = cohort.dataset.tmp_prefix() / get_workflow().name / self.name / cohort.id
        keep_path = tmp_prefix / f'{cohort.id}_array_keep.tsv'
        export_array_vcf.write_array_keep_file(ready_records, keep_path)

        plink_job = export_array_vcf.queue_plink_subset_to_vcf(
            cohort=cohort,
            pgen_path=pgen_path,
            pvar_path=pvar_path,
            psam_path=psam_path,
            keep_iids_path=keep_path,
            output_vcf=outputs['array_vcf'],
        )
        return self.make_outputs(target=cohort, data=outputs, jobs=plink_job)  # pyright: ignore[reportArgumentType]


@stage(required_stages=[SwapCheckExportVcf])
class SwapCheckSomalierRelate(CohortStage):
    """
    Second of three swap-check stages. Localises the upstream WGS
    ``.somalier`` sketches, runs ``somalier extract`` on the array VCF from
    ``SwapCheckExportVcf``, then ``somalier relate`` all-vs-all → a cohort
    pairs.tsv.

    Detects the 0-ready edge case via the upstream mapping JSON and no-ops
    silently (writes an empty pairs.tsv) — the loud warning was already
    logged by ``SwapCheckExportVcf``.
    """
    def expected_outputs(self, cohort: Cohort) -> dict[str, cpg_utils.Path]:  # pyright: ignore[reportIncompatibleMethodOverride]
        prefix = get_output_prefix(cohort, self.name)
        return {
            'pairs_tsv': prefix / f'{cohort.id}.pairs.tsv',
        }

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)

        mapping_path = inputs.as_path(cohort, stage=SwapCheckExportVcf, key='mapping')
        array_vcf_path = inputs.as_path(cohort, stage=SwapCheckExportVcf, key='array_vcf')
        classified = load_json(mapping_path)
        ready_records = [r for r in classified if r['status'] == 'ready']

        if not ready_records:
            with outputs['pairs_tsv'].open('w') as f:
                f.write(_EMPTY_PAIRS_HEADER)
            return self.make_outputs(target=cohort, data=outputs)

        tmp_prefix = cohort.dataset.tmp_prefix() / get_workflow().name / self.name / cohort.id
        manifest_path = tmp_prefix / f'{cohort.id}_wgs_sketch_manifest.txt'
        sketch_paths = [
            str(get_dragen_output_path(f'somalier/{r["wgs_sg"]}.somalier'))
            for r in ready_records
        ]
        somalier_relate.write_wgs_sketch_manifest(sketch_paths, manifest_path)

        somalier_job = somalier_relate.queue_somalier_extract_and_relate(
            cohort=cohort,
            array_vcf_path=array_vcf_path,
            wgs_manifest_path=manifest_path,
            output_pairs_tsv=outputs['pairs_tsv'],
        )
        return self.make_outputs(target=cohort, data=outputs, jobs=somalier_job)  # pyright: ignore[reportArgumentType]


@stage(required_stages=[SwapCheckExportVcf, SwapCheckSomalierRelate])
class SwapCheckClassify(CohortStage):
    """
    Third of three swap-check stages. PythonJob that parses the somalier
    pairs.tsv, joins against the classified WGS↔array mapping, and emits
    the per-SG ``swap_check.json`` consumed by ``RegisterQcMetricsToMetamist``.

    Also sends a high-alert Slack post (separate from the MultiQC failures
    post) when any ``swap_detected`` SGs are present.

    In the 0-ready edge case, writes ``{}`` to swap_check.json so the
    register stage skips the swap_check field on every SG. Partial-ready
    cohorts still propagate mapping-layer statuses (e.g.
    ``array_pending_export``) for non-ready SGs as facts on
    ``sg.meta['qc']['swap_check']``.

    A detected swap is a labelling problem, not a quality problem, and
    deliberately does NOT feed ``workflow.multiqc.deactivate_sgs``.
    Resolution is human review + possible metamist re-link.
    """
    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:
        return get_output_prefix(cohort, self.name) / f'{cohort.id}_swap_check.json'

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        output: cpg_utils.Path = self.expected_outputs(cohort=cohort)

        mapping_path = inputs.as_path(cohort, stage=SwapCheckExportVcf, key='mapping')
        classified = load_json(mapping_path)
        if not any(r['status'] == 'ready' for r in classified):
            # 0-ready edge case (already loudly logged by SwapCheckExportVcf).
            # Emit empty swap_check.json so register skips the field per-SG.
            with output.open('w') as f:
                f.write('{}\n')
            return self.make_outputs(target=cohort, data=output)

        classify_job: PythonJob = initialise_python_job(
            job_name=f'Classify {cohort.id} Sample Swaps',
            target=cohort,
            tool_name='Classify Swaps',
        )
        classify_job.call(
            classify_swaps.run,
            cohort=cohort,
            mapping_path=str(mapping_path),
            pairs_tsv_path=str(inputs.as_path(cohort, stage=SwapCheckSomalierRelate, key='pairs_tsv')),
            output=output,
        )
        return self.make_outputs(target=cohort, data=output, jobs=classify_job)  # pyright: ignore[reportArgumentType]


@stage(required_stages=[RunMultiQc, CheckMultiQc, SwapCheckClassify])
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
        swap_check_path = inputs.as_str(cohort, stage=SwapCheckClassify)

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
