"""
somalier ``relate`` BashJob builder for the ``SwapCheckSomalierRelate`` stage.

Localises the WGS ``.somalier`` sketches listed in the manifest from
``PrepareSampleSwap`` and the array ``.somalier`` sketches listed in the
manifest from ``SwapCheckSomalierExtract`` (both via ``gcloud storage cp |
xargs`` — the somalier image ships gcloud), then runs ``somalier relate``
all-vs-all to emit ``<cohort>.pairs.tsv``.

Keeping the BashJob construction here — rather than inline in ``stages.py`` —
mirrors ``dragen_align_pa/jobs/somalier_extract.py``. Sketch *extraction* now
lives in the separate ``SwapCheckSomalierExtract`` stage (see
``somalier_extract.py``); this builder only consumes the two manifests.

If the upstream WGS manifest is empty (0-ready case), the BashJob skips
somalier entirely and writes an empty ``pairs.tsv`` (header only) so the
downstream classify step reads cleanly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cpg_utils.config import config_retrieve, image_path
from cpg_utils.hail_batch import get_batch

if TYPE_CHECKING:
    from cpg_flow.targets import Cohort
    from cpg_utils import Path
    from hailtop.batch.job import BashJob


def somalier_relate(
    cohort: Cohort,
    wgs_manifest_path: Path,
    array_manifest_path: Path,
    output_pairs_tsv: Path,
) -> BashJob:
    """Queue the somalier ``relate`` BashJob (WGS vs array, all-vs-all).

    Args:
        cohort: cpg-flow Cohort target (used for job attrs only).
        wgs_manifest_path: GCS path to the WGS sketch manifest written by
            ``PrepareSampleSwap`` (one sketch URL per line). Empty in the
            0-ready case, which short-circuits the job.
        array_manifest_path: GCS path to the array sketch manifest written by
            ``SwapCheckSomalierExtract`` (one sketch URL per line).
        output_pairs_tsv: cloud Path where the somalier ``pairs.tsv`` lands.
    """
    b = get_batch()

    parallel_localise = config_retrieve(['workflow', 'swap_check', 'somalier_localise_parallelism'], 16)

    j = b.new_bash_job(
        name=f'SwapCheckSomalierRelate {cohort.id}',
        attributes=(cohort.get_job_attrs() or {}) | {'tool': 'somalier'},
    )
    j.image(image_path('somalier'))
    j.cpu(config_retrieve(['workflow', 'swap_check', 'somalier_cpu'], 4))
    j.memory(config_retrieve(['workflow', 'swap_check', 'somalier_memory'], 'standard'))
    j.storage(config_retrieve(['workflow', 'swap_check', 'somalier_storage'], '50G'))

    wgs_manifest = b.read_input(str(wgs_manifest_path))
    array_manifest = b.read_input(str(array_manifest_path))

    j.declare_resource_group(relate_output={'pairs.tsv': '{root}.pairs.tsv'})

    j.command(
        f"""
        set -euo pipefail

        # If PrepareSampleSwap emitted an empty WGS manifest (0-ready
        # case), skip somalier and produce an empty pairs.tsv with just
        # the header so downstream classify reads cleanly.
        if [[ ! -s {wgs_manifest} ]]; then
            echo "[SwapCheckSomalierRelate] WGS manifest is empty; skipping somalier"
            printf '#sample_a\\tsample_b\\trelatedness\\tibs0\\tn\\n' > {j.relate_output['pairs.tsv']}
            exit 0
        fi

        mkdir -p wgs_somalier array_somalier

        # Localise WGS and array .somalier sketches in parallel (image has gcloud).
        cat {wgs_manifest} | xargs -P {parallel_localise} -I {{}} gcloud storage cp -- {{}} wgs_somalier/
        cat {array_manifest} | xargs -P {parallel_localise} -I {{}} gcloud storage cp -- {{}} array_somalier/

        somalier relate \\
            -o {j.relate_output} \\
            wgs_somalier/*.somalier array_somalier/*.somalier
        """,
    )

    b.write_output(j.relate_output['pairs.tsv'], str(output_pairs_tsv))
    return j
