"""
Standalone script for the ``SwapCheckClassify`` stage.

Invoked via ``python3 -m single_sample_qc_popgen.jobs.classify_swaps_job ...``
from the BashJob queued in stages.py. Parses the somalier pairs.tsv, joins
against the classified WGS↔array mapping from ``PrepareSampleSwap``, and
emits the per-SG ``swap_check.json`` consumed by
``RegisterQcMetricsToMetamist``.

Also sends a high-alert Slack post when any ``swap_detected`` SGs are
present. The Slack post is a separate message from the MultiQC failures
report; both read the pipeline-wide ``[workflow].send_to_slack`` flag.

Thresholds (relatedness cutoffs, minimum site count) are CLI flags passed
by the queueing code in stages.py — keeps this script independent of
cpg-flow config bootstrap.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from cpg_utils import to_path
from cpg_utils.slack import send_message

from single_sample_qc_popgen.jobs.classify_swaps import (
    build_swap_detected_slack_message,
    classify_swap_check,
    parse_pairs_tsv,
)
from single_sample_qc_popgen.utils import load_json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description='Classify cohort WGS↔array somalier-relate output')
    parser.add_argument('--cohort-id', required=True, help='Cohort ID (logging + Slack context)')
    parser.add_argument('--project', required=True, help='Dataset + access level, e.g. ourdna-test (Slack context)')
    parser.add_argument('--mapping-path', required=True, help='GCS path to mapping JSON from PrepareSampleSwap')
    parser.add_argument('--pairs-tsv-path', required=True, help='GCS path to somalier pairs.tsv')
    parser.add_argument('--out', required=True, help='GCS path for swap_check.json')
    parser.add_argument('--concordant-min', type=float, required=True)
    parser.add_argument('--swap-max', type=float, required=True)
    parser.add_argument('--n-min', type=int, required=True)
    parser.add_argument(
        '--send-to-slack',
        action='store_true',
        help='Send a Slack post for any swap_detected SGs',
    )
    args = parser.parse_args()

    mapping = load_json(args.mapping_path)
    pairs = parse_pairs_tsv(args.pairs_tsv_path)

    swap_check_by_sg = classify_swap_check(
        mapping,
        pairs,
        concordant_min=args.concordant_min,
        swap_max=args.swap_max,
        n_min=args.n_min,
    )

    summary: dict[str, int] = {}
    for v in swap_check_by_sg.values():
        summary[v['status']] = summary.get(v['status'], 0) + 1
    logger.info('[SwapCheckClassify] cohort=%s summary=%s', args.cohort_id, summary)

    for sg_id, v in swap_check_by_sg.items():
        if v['status'] == 'swap_detected':
            logger.warning(
                '❗ %s (expected %s) best match is %s (rel=%.3f, n=%s)',
                sg_id,
                v.get('expected_array_sg'),
                v.get('best_array_sg'),
                v.get('best_relatedness'),
                v.get('n_sites_compared'),
            )

    for sg_id, v in swap_check_by_sg.items():
        if v['status'] == 'no_pair_row':
            logger.warning(
                '⚠️  %s (expected %s) is marked ready but produced NO somalier pair row. '
                'The WGS .somalier sketch is likely missing, empty, corrupt, or named '
                'differently from the SG id -- check the upstream sketch for this SG.',
                sg_id,
                v.get('expected_array_sg'),
            )

    with to_path(args.out).open('w') as f:
        json.dump(swap_check_by_sg, f, indent=2)

    if args.send_to_slack:
        message = build_swap_detected_slack_message(args.cohort_id, args.project, swap_check_by_sg)
        if message is not None:
            logger.warning(message)
            send_message(message)


if __name__ == '__main__':
    main()
