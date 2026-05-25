"""
somalier BashJob and queue-time helpers for the ``SwapCheckSomalierRelate``
stage.

Runs ``somalier extract`` on the array VCF produced by ``SwapCheckExportVcf``,
localises every WGS ``.somalier`` sketch listed in a manifest using
``gcloud storage cp`` in parallel (matches the manifest pattern used in
``run_multiqc.py`` and ``ourdna_genomic_atlas``), and runs ``somalier
relate`` all-vs-all. Emits ``<cohort>.pairs.tsv`` to cloud storage.

The sites VCF passed to ``somalier extract`` MUST match the panel used by
``dragen_align_pa`` to produce the upstream WGS sketches -- mismatched
sites yield ``n=0`` in relate output (the #1 silent-failure mode).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cpg_utils.config import config_retrieve, image_path, reference_path
from cpg_utils.hail_batch import get_batch

if TYPE_CHECKING:
    from cpg_flow.targets import Cohort
    from cpg_utils import Path
    from hailtop.batch.job import BashJob


def write_wgs_sketch_manifest(
    sketch_paths: list[str],
    output: Path,
) -> None:
    """Write the manifest of upstream WGS ``.somalier`` sketch paths.

    Consumed inside the somalier container via
    ``cat manifest | xargs gcloud storage cp`` (parallel localise). Paths
    are built upstream via ``get_dragen_output_path`` so this helper has no
    knowledge of dataset/dragen-version layout.
    """
    with output.open('w') as f:
        f.write('\n'.join(sketch_paths) + '\n')


def queue_somalier_extract_and_relate(
    cohort: Cohort,
    array_vcf_path: Path,
    wgs_manifest_path: Path,
    output_pairs_tsv: Path,
) -> BashJob:
    """Queue the somalier BashJob: extract on the array VCF + relate vs WGS.

    The somalier image has gcloud, so per-sample WGS sketches are localised
    inside the container via the manifest + ``gcloud storage cp | xargs``
    pattern (parallelisable across hundreds of sketches). The array VCF
    arrives from the upstream ``SwapCheckExportVcf`` stage's GCS output and
    is staged via ``b.read_input``. Sites VCF and fasta are pulled via
    ``read_input`` since they are single files.

    Args:
        cohort: cpg-flow Cohort target (used for job attrs only).
        array_vcf_path: GCS path to the bgzipped array VCF produced by the
            ``SwapCheckExportVcf`` stage.
        wgs_manifest_path: GCS path to the queue-time-written manifest of
            WGS sketch URLs (one per line).
        output_pairs_tsv: cloud Path where the somalier ``pairs.tsv`` lands.
    """
    b = get_batch()

    parallel_localise = config_retrieve(['workflow', 'swap_check', 'somalier_localise_parallelism'], 16)
    sites_vcf_path = str(reference_path('somalier_sites'))
    fasta_ref_path = str(reference_path('broad/ref_fasta'))

    j = b.new_bash_job(
        name=f'SwapCheckSomalierRelate {cohort.id}',
        attributes=(cohort.get_job_attrs() or {}) | {'tool': 'somalier'},
    )
    j.image(image_path('somalier'))
    j.cpu(config_retrieve(['workflow', 'swap_check', 'somalier_cpu'], 4))
    j.memory(config_retrieve(['workflow', 'swap_check', 'somalier_memory'], 'standard'))
    j.storage(config_retrieve(['workflow', 'swap_check', 'somalier_storage'], '50G'))

    sites = b.read_input(sites_vcf_path)
    fasta = b.read_input_group(base=fasta_ref_path, fai=f'{fasta_ref_path}.fai')
    manifest = b.read_input(str(wgs_manifest_path))
    array_vcf = b.read_input(str(array_vcf_path))

    j.declare_resource_group(relate_output={'pairs.tsv': '{root}.pairs.tsv'})

    j.command(
        f"""
        set -euo pipefail

        mkdir -p wgs_somalier array_somalier

        # 1. Localise WGS .somalier sketches in parallel (image has gcloud).
        gcloud auth list > /dev/null
        cat {manifest} | xargs -P {parallel_localise} -I {{}} gcloud storage cp -- {{}} wgs_somalier/

        # 2. somalier extract on the cohort array VCF.
        somalier extract \\
            --sites {sites} \\
            --fasta {fasta.base} \\
            --out-dir array_somalier/ \\
            {array_vcf}

        # 3. somalier relate all-vs-all (only the pairs.tsv is persisted;
        #    samples.tsv / html are debug-only and not retained).
        somalier relate \\
            -o {j.relate_output} \\
            wgs_somalier/*.somalier array_somalier/*.somalier
        """,
    )

    b.write_output(j.relate_output['pairs.tsv'], str(output_pairs_tsv))
    return j
