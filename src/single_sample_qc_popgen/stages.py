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
from cpg_utils.config import config_retrieve, get_driver_image, image_path, output_path
from cpg_utils.hail_batch import get_batch

from single_sample_qc_popgen.constants import DRAGEN_VERSION
from single_sample_qc_popgen.jobs import (
    check_multiqc,
    register_qc_metamist,
    run_multiqc,
    somalier_extract,
    somalier_relate,
)
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


@stage(required_stages=[CheckMultiQc])
class PrepareSampleSwap(CohortStage):
    """
    First of five swap-check stages. Resolves the WGS↔array mapping by
    running ``prepare_sample_swap_job.py`` on a worker via the driver image:
    queries metamist, reads the rolling pgen psam, classifies each WGS SG,
    and writes three artifacts:

    * ``mapping.json``  -- classified mapping (one record per WGS SG)
    * ``keep.tsv``      -- plink2 ``--keep`` IIDs for downstream
                          ``SwapCheckExportVcf``
    * ``manifest.txt``  -- WGS sketch paths for downstream
                          ``SwapCheckSomalierRelate``

    All Python lives in the script; the driver only constructs the BashJob.
    In the 0-ready edge case the script writes empty keep/manifest and logs
    a loud warning to the job log (not the workflow log).
    """
    def expected_outputs(self, cohort: Cohort) -> dict[str, cpg_utils.Path]:  # pyright: ignore[reportIncompatibleMethodOverride]
        prefix = get_output_prefix(cohort, self.name)
        return {
            'mapping': prefix / f'{cohort.id}_swap_mapping.json',
            'keep': prefix / f'{cohort.id}_array_keep.tsv',
            'manifest': prefix / f'{cohort.id}_wgs_sketch_manifest.txt',
        }

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:  # noqa: ARG002
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)

        # The rolling array_aggregate_pgen is not registered in metamist, so
        # the user supplies the path explicitly. No default — config_retrieve
        # raises if the analysis-runner config omits it.
        psam_path = config_retrieve(['workflow', 'swap_check', 'psam_path'])

        # Template for the upstream WGS sketches. {{sg_id}} survives the
        # f-string as the literal placeholder the script formats per SG.
        wgs_sketch_template = output_path(f'ica/{DRAGEN_VERSION}/output/somalier/{{sg_id}}.somalier')

        b = get_batch()
        j = b.new_bash_job(
            name=f'PrepareSampleSwap {cohort.id}',
            attributes=(cohort.get_job_attrs() or {}) | {'tool': 'metamist'},
        )
        j.image(get_driver_image())
        j.command(
            f"""
            set -euo pipefail
            python3 -m single_sample_qc_popgen.jobs.prepare_sample_swap_job \\
                --cohort-id {cohort.id} \\
                --psam-path {psam_path} \\
                --wgs-sketch-template '{wgs_sketch_template}' \\
                --out-mapping {outputs['mapping']} \\
                --out-keep {outputs['keep']} \\
                --out-manifest {outputs['manifest']}
            """,
        )

        return self.make_outputs(target=cohort, data=outputs, jobs=j)  # pyright: ignore[reportArgumentType]


@stage(required_stages=[PrepareSampleSwap])
class SwapCheckExportVcf(CohortStage):
    """
    Second of five swap-check stages. Subsets the rolling popgen-genotyping
    pgen to the cohort's array SGs using the keep file produced by
    ``PrepareSampleSwap``, exports a bgzipped VCF.

    ``--output-chr chrM`` keeps the 'chr' prefix so the exported VCF matches
    the chr-prefixed somalier sites panel — without it every downstream
    relate row reports n=0 (the #1 silent-failure mode).

    If the upstream keep file is empty (0-ready case), the BashJob skips
    plink and writes an empty placeholder VCF; the loud warning was already
    logged inside the PrepareSampleSwap script.
    """
    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:
        return get_output_prefix(cohort, self.name) / f'{cohort.id}_array.vcf.gz'

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        output: cpg_utils.Path = self.expected_outputs(cohort=cohort)

        pgen_path = config_retrieve(['workflow', 'swap_check', 'pgen_path'])
        pvar_path = config_retrieve(['workflow', 'swap_check', 'pvar_path'])
        psam_path = config_retrieve(['workflow', 'swap_check', 'psam_path'])

        b = get_batch()
        j = b.new_bash_job(
            name=f'SwapCheckExportVcf {cohort.id}',
            attributes=(cohort.get_job_attrs() or {}) | {'tool': 'plink2'},
        )
        j.image(image_path('plink'))
        j.cpu(config_retrieve(['workflow', 'swap_check', 'plink2_cpu'], 2))
        j.memory(config_retrieve(['workflow', 'swap_check', 'plink2_memory'], 'standard'))
        j.storage(config_retrieve(['workflow', 'swap_check', 'plink2_storage'], '50G'))

        pgen = b.read_input_group(pgen=pgen_path, pvar=pvar_path, psam=psam_path)
        keep = b.read_input(str(inputs.as_path(cohort, stage=PrepareSampleSwap, key='keep')))
        j.declare_resource_group(array_vcf={'vcf.gz': '{root}.vcf.gz'})

        j.command(
            f"""
            set -euo pipefail

            # If PrepareSampleSwap emitted an empty keep file (0-ready case),
            # skip plink and produce an empty placeholder VCF. Downstream
            # somalier stage detects the empty manifest and skips too.
            if [[ ! -s {keep} ]]; then
                echo "[SwapCheckExportVcf] keep file is empty; skipping plink2"
                : > {j.array_vcf['vcf.gz']}
                exit 0
            fi

            # --output-chr chrM keeps the 'chr' prefix so the exported VCF
            # matches the chr-prefixed somalier sites panel.
            plink2 --pfile {pgen} \\
                   --keep {keep} \\
                   --autosome --max-alleles 2 --snps-only \\
                   --output-chr chrM \\
                   --export vcf bgz id-paste=iid \\
                   --out {j.array_vcf}
            """,
        )

        b.write_output(j.array_vcf['vcf.gz'], str(output))
        return self.make_outputs(target=cohort, data=output, jobs=j)  # pyright: ignore[reportArgumentType]


@stage(required_stages=[SwapCheckExportVcf])
class SwapCheckSomalierExtract(CohortStage):
    """
    Third of five swap-check stages. Runs ``somalier extract`` on the array
    VCF from ``SwapCheckExportVcf`` to produce one ``.somalier`` sketch per
    array sample, then persists each sketch to GCS for provenance under this
    stage's standard cohort-scoped output prefix.

    Previously the array sketches were generated and consumed inside the
    same ``somalier relate`` BashJob and never written anywhere, so there
    was no record of the array fingerprints.

    The sites VCF MUST match what dragen_align_pa used to produce the
    upstream WGS sketches; mismatched panels silently yield n=0 in relate
    output.

    Per-array-sample ``.somalier`` filenames are the array IIDs, which are
    only known at runtime (from the metamist mapping + pgen psam), so they
    can't be enumerated in ``expected_outputs``. The tracked output is
    therefore a deterministic ``manifest`` listing the GCS path of every
    saved sketch (one per line) — the same pattern ``PrepareSampleSwap``
    uses for the WGS sketches — which ``SwapCheckSomalierRelate`` localises
    from. In the 0-ready case ``SwapCheckExportVcf`` writes an empty
    placeholder VCF; this stage detects it, writes an empty manifest, and
    no-ops.
    """
    def expected_outputs(self, cohort: Cohort) -> dict[str, cpg_utils.Path]:  # pyright: ignore[reportIncompatibleMethodOverride]
        return {
            'manifest': get_output_prefix(cohort, self.name) / f'{cohort.id}_array_sketch_manifest.txt',
        }

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        outputs: dict[str, cpg_utils.Path] = self.expected_outputs(cohort=cohort)

        j: BashJob = somalier_extract.somalier_extract(
            cohort=cohort,
            array_vcf_path=inputs.as_path(cohort, stage=SwapCheckExportVcf),
            # somalier names each output by the VCF sample (the array IID), so
            # sketches land as <prefix>/<array_iid>.somalier. The prefix is the
            # standard cohort-scoped stage output (already ends in <cohort.id>).
            array_sketch_prefix=get_output_prefix(cohort, self.name),
            output_manifest=outputs['manifest'],
        )

        return self.make_outputs(target=cohort, data=outputs, jobs=j)  # pyright: ignore[reportArgumentType]


@stage(required_stages=[PrepareSampleSwap, SwapCheckSomalierExtract])
class SwapCheckSomalierRelate(CohortStage):
    """
    Fourth of five swap-check stages. Localises the WGS ``.somalier``
    sketches listed in the manifest from ``PrepareSampleSwap`` and the array
    ``.somalier`` sketches listed in the manifest from
    ``SwapCheckSomalierExtract`` (both via ``gcloud storage cp | xargs`` —
    the somalier image has gcloud), then runs ``somalier relate`` all-vs-all
    → ``<cohort>.pairs.tsv``.

    If the upstream WGS manifest is empty (0-ready case), the BashJob skips
    somalier and writes an empty pairs.tsv (header only).
    """
    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:
        return get_output_prefix(cohort, self.name) / f'{cohort.id}.pairs.tsv'

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        output: cpg_utils.Path = self.expected_outputs(cohort=cohort)

        j: BashJob = somalier_relate.somalier_relate(
            cohort=cohort,
            wgs_manifest_path=inputs.as_path(cohort, stage=PrepareSampleSwap, key='manifest'),
            array_manifest_path=inputs.as_path(cohort, stage=SwapCheckSomalierExtract, key='manifest'),
            output_pairs_tsv=output,
        )

        return self.make_outputs(target=cohort, data=output, jobs=j)  # pyright: ignore[reportArgumentType]


@stage(required_stages=[PrepareSampleSwap, SwapCheckSomalierRelate])
class SwapCheckClassify(CohortStage):
    """
    Fifth of five swap-check stages. Parses the somalier pairs.tsv, joins
    against the classified WGS↔array mapping from ``PrepareSampleSwap``,
    and emits the per-SG ``swap_check.json`` consumed by
    ``RegisterQcMetricsToMetamist``.

    Sends a high-alert Slack post (separate from the MultiQC failures post)
    for any ``swap_detected`` SGs. A detected swap is a labelling problem,
    not a quality problem, and deliberately does NOT feed
    ``workflow.multiqc.deactivate_sgs``. Resolution is human review +
    possible metamist re-link.
    """
    def expected_outputs(self, cohort: Cohort) -> cpg_utils.Path:
        return get_output_prefix(cohort, self.name) / f'{cohort.id}_swap_check.json'

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        output: cpg_utils.Path = self.expected_outputs(cohort=cohort)

        mapping_path = inputs.as_path(cohort, stage=PrepareSampleSwap, key='mapping')
        pairs_tsv_path = inputs.as_path(cohort, stage=SwapCheckSomalierRelate)

        concordant_min = config_retrieve(['workflow', 'swap_check', 'relatedness_concordant_min'])
        swap_max = config_retrieve(['workflow', 'swap_check', 'relatedness_swap_max'])
        n_min = config_retrieve(['workflow', 'swap_check', 'n_sites_min'])
        send_to_slack = config_retrieve(['workflow', 'send_to_slack'], default=True)

        b = get_batch()
        j = b.new_bash_job(
            name=f'SwapCheckClassify {cohort.id}',
            attributes=(cohort.get_job_attrs() or {}) | {'tool': 'classify_swaps'},
        )
        j.image(get_driver_image())
        j.command(
            f"""
            set -euo pipefail
            python3 -m single_sample_qc_popgen.jobs.classify_swaps_job \\
                --cohort-id {cohort.id} \\
                --mapping-path {mapping_path} \\
                --pairs-tsv-path {pairs_tsv_path} \\
                --out {output} \\
                --concordant-min {concordant_min} \\
                --swap-max {swap_max} \\
                --n-min {n_min} \\
                {'--send-to-slack' if send_to_slack else ''}
            """,
        )

        return self.make_outputs(target=cohort, data=output, jobs=j)  # pyright: ignore[reportArgumentType]


@stage(required_stages=[RunMultiQc, CheckMultiQc, SwapCheckClassify])
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

    Somalier-derived raw signals (merged in from CheckMultiQc):
        f_stat_raw: float
        x_het_rate: float
        n_called_x: int
        y_calls: int
        y_n: int
    Karyotype derivation (LoY rescue, ambiguous gate) lives downstream in
    ourdna_genomic_atlas.

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
