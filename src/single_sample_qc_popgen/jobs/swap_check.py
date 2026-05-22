"""
BashJobs and queue-time helpers for the SwapCheck stage.

Two single-purpose containers, two BashJobs. Both images are resolved via
``image_path('plink')`` / ``image_path('somalier')``; both reference files
(somalier sites panel, hg38 fasta) come via ``reference_path('somalier_sites')``
/ ``reference_path('broad/ref_fasta')``. The rolling popgen-genotyping pgen
paths are NOT cpg-common -- they are required entries in the user's
analysis-runner config under ``[workflow.swap_check]``.

  1. ``queue_plink_subset_to_vcf`` -- plink2 image (no gcloud SDK).
     Subsets the rolling popgen-genotyping pgen to the cohort's array SGs and
     exports a bgzipped VCF as a Hail Batch resource. ``--output-chr chrM`` is
     mandatory: plink2 drops the 'chr' prefix by default, which silently
     yields zero overlap against the chr-prefixed somalier sites panel and
     produces empty sketches. The pgen is staged via Batch resource groups
     (no gcloud needed in the container).
  2. ``queue_somalier_extract_and_relate`` -- somalier image (has gcloud).
     Runs ``somalier extract`` on the array VCF produced by the plink job,
     localises every WGS ``.somalier`` sketch listed in a manifest using
     ``gcloud storage cp`` in parallel (matches the manifest pattern used in
     ``run_multiqc.py`` and ``ourdna_genomic_atlas``), and runs
     ``somalier relate`` all-vs-all. Emits ``<cohort>.pairs.tsv`` to cloud
     storage.

The sites VCF passed to ``somalier extract`` MUST match the panel used by
``dragen_align_pa`` to produce the upstream WGS sketches -- mismatched sites
yield ``n=0`` in relate output (the #1 silent-failure mode).

Downstream ``check_swap.py`` (PythonJob) parses the pairs.tsv into per-SG
status records and joins them against the WGS-to-array mapping resolved at
queue time.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from cpg_utils.config import config_retrieve, image_path, reference_path
from cpg_utils.hail_batch import get_batch

if TYPE_CHECKING:
    from cpg_flow.targets import Cohort
    from cpg_utils import Path
    from hailtop.batch.job import BashJob
    from hailtop.batch.resource import ResourceGroup


def write_mapping_for_python_job(
    classified_records: list[dict[str, Any]],
    output: Path,
) -> None:
    """Persist the WGS-to-array mapping as JSON for the downstream PythonJob.

    The classified mapping is the source of truth for ``expected`` array SG
    per WGS SG and for skip-reason statuses (everything not 'ready'). Written
    once at queue time so the PythonJob is a pure-IO consumer.
    """
    with output.open('w') as f:
        json.dump(classified_records, f, indent=2)


def write_array_keep_file(
    ready_records: list[dict[str, Any]],
    output: Path,
) -> None:
    """Write the plink2 ``--keep`` file: one IID (array SG ID) per line.

    The popgen-genotyping psam uses IID-only rows, so this single-column
    format is what plink2 expects after ``--keep``.
    """
    with output.open('w') as f:
        f.write('\n'.join(r['array_sg'] for r in ready_records) + '\n')


def write_wgs_sketch_manifest(
    sketch_paths: list[str],
    output: Path,
) -> None:
    """Write the manifest of upstream WGS ``.somalier`` sketch paths.

    The manifest is consumed inside the somalier container via
    ``cat manifest | xargs gcloud storage cp`` (parallel localise). Paths
    are built upstream via ``get_dragen_output_path`` so this helper has no
    knowledge of dataset/dragen-version layout.
    """
    with output.open('w') as f:
        f.write('\n'.join(sketch_paths) + '\n')


def queue_plink_subset_to_vcf(
    cohort: Cohort,
    pgen_path: str,
    pvar_path: str,
    psam_path: str,
    keep_iids_path: Path,
) -> tuple[BashJob, ResourceGroup]:
    """Queue the plink2 BashJob that subsets the pgen to a cohort array VCF.

    The plink2 image has no gcloud SDK, so all inputs are passed via Hail
    Batch resource staging (``read_input_group`` / ``read_input``). The
    output VCF is declared as a resource group so Batch can hand it directly
    to the downstream somalier job without a GCS round-trip.

    Args:
        cohort: cpg-flow Cohort target (used for job attrs only).
        pgen_path / pvar_path / psam_path: cloud paths to the rolling pgen,
            from ``[workflow.swap_check]`` in config.
        keep_iids_path: GCS path to the queue-time-written one-IID-per-line
            file listing the array SG IDs to keep.

    Returns:
        ``(BashJob, array_vcf_resource_group)``. The resource group has key
        ``vcf.gz`` -- pass it through to the somalier job's command.
    """
    b = get_batch()

    j = b.new_bash_job(
        name=f'SwapCheck plink2 subset {cohort.id}',
        attributes=(cohort.get_job_attrs() or {}) | {'tool': 'plink2'},
    )
    j.image(image_path('plink'))
    j.cpu(config_retrieve(['workflow', 'swap_check', 'plink2_cpu'], 2))
    j.memory(config_retrieve(['workflow', 'swap_check', 'plink2_memory'], 'standard'))
    j.storage(config_retrieve(['workflow', 'swap_check', 'plink2_storage'], '50G'))

    pgen = b.read_input_group(pgen=pgen_path, pvar=pvar_path, psam=psam_path)
    keep = b.read_input(str(keep_iids_path))

    j.declare_resource_group(array_vcf={'vcf.gz': '{root}.vcf.gz'})

    j.command(
        f"""
        set -euo pipefail

        # --output-chr chrM keeps the 'chr' prefix so the exported VCF
        # matches the chr-prefixed somalier sites panel. Without it, every
        # downstream relate row reports n=0.
        plink2 --pfile {pgen} \\
               --keep {keep} \\
               --autosome --max-alleles 2 --snps-only \\
               --output-chr chrM \\
               --export vcf bgz id-paste=iid \\
               --out {j.array_vcf}
        """,
    )

    return j, j.array_vcf


def queue_somalier_extract_and_relate(
    cohort: Cohort,
    array_vcf: ResourceGroup,
    wgs_manifest_path: Path,
    output_pairs_tsv: Path,
) -> BashJob:
    """Queue the somalier BashJob: extract on array VCF + relate vs WGS.

    The somalier image has gcloud, so per-sample WGS sketches are localised
    inside the container via the manifest + ``gcloud storage cp | xargs``
    pattern (parallelisable across hundreds of sketches). The array VCF
    arrives as a Hail Batch resource from the upstream plink job. Sites VCF
    and fasta are pulled via ``read_input`` since they are single files.

    Args:
        cohort: cpg-flow Cohort target (used for job attrs only).
        array_vcf: resource group from ``queue_plink_subset_to_vcf`` with
            key ``vcf.gz``.
        wgs_manifest_path: GCS path to the queue-time-written manifest of
            WGS sketch URLs (one per line).
        output_pairs_tsv: cloud Path where the somalier ``pairs.tsv`` lands.
    """
    b = get_batch()

    parallel_localise = config_retrieve(['workflow', 'swap_check', 'somalier_localise_parallelism'], 16)
    sites_vcf_path = str(reference_path('somalier_sites'))
    fasta_ref_path = str(reference_path('broad/ref_fasta'))

    j = b.new_bash_job(
        name=f'SwapCheck somalier relate {cohort.id}',
        attributes=(cohort.get_job_attrs() or {}) | {'tool': 'somalier'},
    )
    j.image(image_path('somalier'))
    j.cpu(config_retrieve(['workflow', 'swap_check', 'somalier_cpu'], 4))
    j.memory(config_retrieve(['workflow', 'swap_check', 'somalier_memory'], 'standard'))
    j.storage(config_retrieve(['workflow', 'swap_check', 'somalier_storage'], '50G'))

    sites = b.read_input(sites_vcf_path)
    fasta = b.read_input_group(base=fasta_ref_path, fai=f'{fasta_ref_path}.fai')
    manifest = b.read_input(str(wgs_manifest_path))

    j.declare_resource_group(relate_output={'pairs.tsv': '{root}.pairs.tsv'})

    j.command(
        f"""
        set -euo pipefail

        mkdir -p wgs_somalier array_somalier

        # 1. Localise WGS .somalier sketches in parallel (image has gcloud).
        gcloud auth list > /dev/null
        cat {manifest} | xargs -P {parallel_localise} -I {{}} gcloud storage cp -- {{}} wgs_somalier/

        # 2. somalier extract on the cohort array VCF (Batch-staged resource).
        somalier extract \\
            --sites {sites} \\
            --fasta {fasta.base} \\
            --out-dir array_somalier/ \\
            {array_vcf['vcf.gz']}

        # 3. somalier relate all-vs-all (only the pairs.tsv is persisted;
        #    samples.tsv / html are debug-only and not retained).
        somalier relate \\
            -o {j.relate_output} \\
            wgs_somalier/*.somalier array_somalier/*.somalier
        """,
    )

    b.write_output(j.relate_output['pairs.tsv'], str(output_pairs_tsv))
    return j
