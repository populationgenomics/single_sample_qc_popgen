"""
Standalone script for the ``PrepareSampleSwap`` stage.

Invoked via ``python prepare_sample_swap_job.py --flag value ...`` from the
BashJob queued by ``prepare_sample_swap.queue_prepare_sample_swap``. Reads
the cohort's WGS→array mapping from metamist, joins against the rolling
popgen-genotyping pgen psam, classifies each WGS SG, and writes three
artifacts consumed by the downstream swap-check stages:

* ``--out-mapping``  : classified mapping JSON (one record per WGS SG)
* ``--out-keep``     : plink2 ``--keep`` file (one array IID per line) for
                       the SGs whose array data is in the rolling pgen
* ``--out-manifest`` : list of upstream WGS ``.somalier`` sketch paths to
                       localise inside the somalier BashJob

In the 0-ready edge case (no WGS SG in the cohort has array data in the
rolling pgen), all three files are written empty and a loud warning is
logged at job-time. Downstream stages detect emptiness and no-op.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from cpg_utils import to_path

from single_sample_qc_popgen.metamist_utils import (
    classify_wgs_to_array_mapping,
    query_wgs_to_array_mapping,
    read_psam_array_sgs,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description='Prepare sample-swap mapping + plink keep + WGS sketch manifest')
    parser.add_argument('--cohort-id', required=True, help='Cohort ID for the metamist query')
    parser.add_argument('--psam-path', required=True, help='GCS path to the rolling pgen psam')
    parser.add_argument(
        '--wgs-sketch-template',
        required=True,
        help='Path template for WGS .somalier sketches with a literal {sg_id} placeholder',
    )
    parser.add_argument('--out-mapping', required=True, help='GCS path for the classified mapping JSON')
    parser.add_argument('--out-keep', required=True, help='GCS path for the plink2 --keep file')
    parser.add_argument('--out-manifest', required=True, help='GCS path for the WGS sketch manifest')
    args = parser.parse_args()

    # 1. Resolve WGS→array mapping from metamist + the pgen psam.
    mapping_records = query_wgs_to_array_mapping(args.cohort_id)
    pgen_psam_sgs = read_psam_array_sgs(args.psam_path)
    classified = classify_wgs_to_array_mapping(mapping_records, pgen_psam_sgs)
    ready_records = [r for r in classified if r['status'] == 'ready']

    # 2. Write the mapping JSON (always).
    with to_path(args.out_mapping).open('w') as f:
        json.dump(classified, f, indent=2)

    # 3. The keep file is one array SG IID per line (or empty in the 0-ready case).
    keep_lines = [r['array_sg'] for r in ready_records]
    with to_path(args.out_keep).open('w') as f:
        if keep_lines:
            f.write('\n'.join(keep_lines) + '\n')

    # 4. The manifest is one WGS sketch path per line.
    sketch_paths = [args.wgs_sketch_template.format(sg_id=r['wgs_sg']) for r in ready_records]
    with to_path(args.out_manifest).open('w') as f:
        if sketch_paths:
            f.write('\n'.join(sketch_paths) + '\n')

    # 5. Loud 0-ready warning. Lives here rather than the driver so the
    #    cpg-flow workflow log stays clean.
    if not ready_records:
        status_counts: dict[str, int] = {}
        for r in classified:
            status_counts[r['status']] = status_counts.get(r['status'], 0) + 1
        logger.warning(
            '⚠️  [PrepareSampleSwap] cohort=%s: 0 / %d WGS SGs are ready for swap-check.\n'
            '    Status breakdown: %s.\n'
            '    Likely causes: wrong cohort, stale rolling pgen, or array data not yet ingested.\n'
            '    Downstream swap-check stages will no-op; swap_check facts will NOT be registered to metamist.\n'
            '    Other QC metrics (MultiQC, sex imputation) will still register normally.',
            args.cohort_id,
            len(classified),
            status_counts,
        )
    else:
        logger.info(
            '[PrepareSampleSwap] cohort=%s: %d / %d WGS SGs are ready for swap-check.',
            args.cohort_id,
            len(ready_records),
            len(classified),
        )


if __name__ == '__main__':
    main()
