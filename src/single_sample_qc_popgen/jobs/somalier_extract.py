"""
somalier ``extract`` BashJob builder for the ``SwapCheckSomalierExtract`` stage.

Runs ``somalier extract`` on the array VCF produced by ``SwapCheckExportVcf``
to emit one ``.somalier`` sketch per array sample, persists each sketch to GCS
for provenance, and records the GCS destinations in a manifest (one path per
line) consumed by ``SwapCheckSomalierRelate``.

Keeping the BashJob construction here — rather than inline in ``stages.py`` —
mirrors ``dragen_align_pa/jobs/somalier_extract.py``. The somalier image ships
python3 + google-cloud-sdk + the somalier binary, so the in-container ``gcloud
storage cp`` provenance copy is safe (the plink2 image, by contrast, has
neither, which is why ``SwapCheckExportVcf`` must stay inline bash).

The sites VCF MUST match the panel ``dragen_align_pa`` used to produce the
upstream WGS sketches; mismatched panels silently yield ``n=0`` in relate
output (the #1 silent-failure mode).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cpg_utils.config import config_retrieve, image_path, reference_path
from cpg_utils.hail_batch import get_batch

if TYPE_CHECKING:
    from cpg_flow.targets import Cohort
    from cpg_utils import Path
    from hailtop.batch.job import BashJob


def somalier_extract(
    cohort: Cohort,
    array_vcf_path: Path,
    array_sketch_prefix: Path,
    output_manifest: Path,
) -> BashJob:
    """Queue the somalier ``extract`` BashJob for the cohort array VCF.

    Args:
        cohort: cpg-flow Cohort target (used for job attrs only).
        array_vcf_path: GCS path to the bgzipped array VCF from
            ``SwapCheckExportVcf``. In the 0-ready case this is an empty
            placeholder; the job detects it and writes an empty manifest.
        array_sketch_prefix: cohort-scoped stage output prefix under which the
            per-array-sample ``.somalier`` sketches are persisted. somalier
            names each output by the VCF sample (the array IID), so sketches
            land as ``<prefix>/<array_iid>.somalier``.
        output_manifest: cloud Path where the manifest of saved sketch GCS
            paths lands (one per line).
    """
    b = get_batch()

    sites_vcf_path = str(reference_path('somalier_sites'))
    fasta_ref_path = str(reference_path('broad/ref_fasta'))

    j = b.new_bash_job(
        name=f'SwapCheckSomalierExtract {cohort.id}',
        attributes=(cohort.get_job_attrs() or {}) | {'tool': 'somalier'},
    )
    j.image(image_path('somalier'))
    j.cpu(config_retrieve(['workflow', 'swap_check', 'somalier_cpu'], 4))
    j.memory(config_retrieve(['workflow', 'swap_check', 'somalier_memory'], 'standard'))
    j.storage(config_retrieve(['workflow', 'swap_check', 'somalier_storage'], '50G'))

    sites = b.read_input(sites_vcf_path)
    fasta = b.read_input_group(base=fasta_ref_path, fai=f'{fasta_ref_path}.fai')
    array_vcf = b.read_input(str(array_vcf_path))

    j.command(
        f"""
        set -euo pipefail

        # If SwapCheckExportVcf emitted an empty placeholder VCF (0-ready
        # case), skip somalier and write an empty manifest so the relate
        # stage no-ops cleanly.
        if [[ ! -s {array_vcf} ]]; then
            echo "[SwapCheckSomalierExtract] array VCF is empty; skipping somalier extract"
            : > {j.manifest}
            exit 0
        fi

        mkdir -p array_somalier
        somalier extract \\
            --sites {sites} \\
            --fasta {fasta.base} \\
            --out-dir array_somalier/ \\
            {array_vcf}

        # Persist each array sketch to GCS for provenance and record its
        # GCS destination in the manifest (image has gcloud). The manifest
        # line is written only after a successful copy, so it never
        # references a sketch that failed to upload.
        gcloud auth list > /dev/null
        : > {j.manifest}
        for sketch in array_somalier/*.somalier; do
            dest="{array_sketch_prefix}/$(basename "$sketch")"
            gcloud storage cp -- "$sketch" "$dest"
            echo "$dest" >> {j.manifest}
        done
        """,
    )

    b.write_output(j.manifest, str(output_manifest))
    return j
