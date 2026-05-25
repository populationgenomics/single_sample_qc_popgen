"""
plink2 BashJob and queue-time helpers for the ``SwapCheckExportVcf`` stage.

Subsets the rolling popgen-genotyping pgen to the cohort's array SGs and
exports a bgzipped VCF. ``--output-chr chrM`` is mandatory: plink2 drops
the 'chr' prefix by default, which silently yields zero overlap against
the chr-prefixed somalier sites panel and produces empty sketches.

Queue-time writers (``write_mapping_for_python_job``,
``write_array_keep_file``) persist the classified WGS-to-array mapping and
the plink2 ``--keep`` list to the cohort tmp bucket so the BashJob has
everything it needs as Hail Batch resources.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from cpg_utils.config import config_retrieve, image_path
from cpg_utils.hail_batch import get_batch

if TYPE_CHECKING:
    from cpg_flow.targets import Cohort
    from cpg_utils import Path
    from hailtop.batch.job import BashJob


def write_mapping_for_python_job(
    classified_records: list[dict[str, Any]],
    output: Path,
) -> None:
    """Persist the WGS-to-array mapping as JSON.

    The classified mapping is the source of truth for ``expected`` array SG
    per WGS SG and for skip-reason statuses (everything not 'ready'). Read
    by ``classify_swaps.run`` downstream to join against the somalier
    pairs.tsv.
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


def queue_plink_subset_to_vcf(
    cohort: Cohort,
    pgen_path: str,
    pvar_path: str,
    psam_path: str,
    keep_iids_path: Path,
    output_vcf: Path,
) -> BashJob:
    """Queue the plink2 BashJob that subsets the pgen to a cohort array VCF.

    The plink2 image has no gcloud SDK, so all inputs are passed via Hail
    Batch resource staging (``read_input_group`` / ``read_input``). The
    bgzipped VCF is written to ``output_vcf`` via ``b.write_output``, where
    the downstream somalier stage picks it up.

    Args:
        cohort: cpg-flow Cohort target (used for job attrs only).
        pgen_path / pvar_path / psam_path: cloud paths to the rolling pgen,
            from ``[workflow.swap_check]`` in the user's analysis-runner
            config.
        keep_iids_path: GCS path to the queue-time-written one-IID-per-line
            file listing the array SG IDs to keep.
        output_vcf: cloud Path where the bgzipped VCF lands. The somalier
            stage reads it via ``b.read_input`` (separate Batch DAG).
    """
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

    b.write_output(j.array_vcf['vcf.gz'], str(output_vcf))
    return j
