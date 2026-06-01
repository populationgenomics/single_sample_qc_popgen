"""
Pure-logic helpers for the ``SwapCheckClassify`` stage.

The job entrypoint that drives these (parses CLI args, reads inputs,
writes outputs, posts to Slack) lives in ``classify_swaps_job.py`` and is
invoked from a BashJob via ``python3 -m
single_sample_qc_popgen.jobs.classify_swaps_job``.

Status taxonomy:
    concordant            -- best somalier-relate match IS the expected
                             array SG (relatedness >= concordant_min,
                             n >= n_min).
    swap_detected         -- best match is a DIFFERENT array SG with
                             relatedness >= concordant_min (the actionable
                             swap signal).
    discordant_no_match   -- best relatedness < swap_max: looks unrelated
                             to every array sample in the cohort.
    ambiguous             -- best relatedness in (swap_max, concordant_min).
    insufficient_sites    -- best pair n < n_min (sites panel mismatch).
    no_array_sg /
    array_pending_export /
    multiple_array_sgs /
    missing_sample        -- pass-through from the mapping classifier.
"""

from __future__ import annotations

import csv
from typing import TYPE_CHECKING, Any

from cpg_utils import to_path

if TYPE_CHECKING:
    from cpg_utils import Path


def parse_pairs_tsv(pairs_tsv_path: str | 'Path') -> list[dict[str, Any]]:
    """Parse a somalier pairs.tsv into a list of row dicts.

    Header line starts with ``#sample_a``; we strip the leading '#' so
    DictReader keys are clean. Numeric coercion is kept minimal -- only
    the columns we actually compare on (relatedness, ibs0, n) are typed.
    """
    rows: list[dict[str, Any]] = []
    with to_path(pairs_tsv_path).open() as f:
        # Strip the '#' off the header so DictReader names are usable.
        header = f.readline().lstrip('#').rstrip('\n').split('\t')
        reader = csv.DictReader(f, fieldnames=header, delimiter='\t')
        for row in reader:
            row['relatedness'] = float(row.get('relatedness') or 'nan')
            row['ibs0'] = int(row.get('ibs0') or 0)
            row['n'] = int(row.get('n') or 0)
            rows.append(row)
    return rows


def classify_swap_check(
    mapping: list[dict[str, Any]],
    pairs: list[dict[str, Any]],
    *,
    concordant_min: float,
    swap_max: float,
    n_min: int,
) -> dict[str, dict[str, Any]]:
    """For each WGS SG, find the best array match and assign a status.

    All decisions are driven by somalier's ``relatedness`` score from
    ``somalier relate``. That score is a KING-robust kinship coefficient
    computed from IBS counts across the somalier sites panel, interpreted
    as: ~1.0 = identical samples (a true WGS↔array self-match), ~0.5 =
    first-degree relatives (parent/child or sibs), ~0.0 = unrelated.
    ``n`` is the number of comparable sites; very low ``n`` means the
    relatedness estimate is noisy and shouldn't be trusted.

    Status taxonomy:
        concordant            -- best somalier-relate match IS the
                                 expected array SG, ``relatedness >=
                                 concordant_min`` AND ``n >= n_min``.
        swap_detected         -- best match is a DIFFERENT array SG with
                                 ``relatedness >= concordant_min`` (the
                                 actionable swap signal).
        discordant_no_match   -- best ``relatedness < swap_max``: the WGS
                                 sample looks unrelated to every array
                                 sample in the cohort. Not a swap, but a
                                 sample-quality red flag (e.g. cross-
                                 contamination, wrong-cohort run).
        ambiguous             -- best ``relatedness`` in
                                 ``(swap_max, concordant_min)`` -- middle
                                 band where the signal is too weak to call
                                 a match but too strong to call a non-match.
        insufficient_sites    -- best pair ``n < n_min``: relatedness
                                 estimate is too noisy. Usually means the
                                 somalier sites panel didn't match what
                                 ``dragen_align_pa`` used to produce the
                                 upstream WGS sketches.
        no_array_sg / array_pending_export / multiple_array_sgs /
        missing_sample        -- pass-through from the mapping classifier;
                                 no somalier comparison ran for these.
    """
    # Index ready array SGs so we know which pairs are WGS<->array (and not
    # WGS<->WGS within-cohort relatedness, which we ignore here).
    array_sgs_present = {r['array_sg'] for r in mapping if r['array_sg']}
    wgs_sgs_present = {r['wgs_sg'] for r in mapping}

    # Best array match per WGS SG, across the whole pairs table.
    best: dict[str, dict[str, Any]] = {}
    for row in pairs:
        a, b = row['sample_a'], row['sample_b']
        wgs_sg, array_sg = None, None
        if a in wgs_sgs_present and b in array_sgs_present:
            wgs_sg, array_sg = a, b
        elif b in wgs_sgs_present and a in array_sgs_present:
            wgs_sg, array_sg = b, a
        else:
            continue  # WGS<->WGS or array<->array, ignore for swap detection

        prev = best.get(wgs_sg)
        if prev is None or row['relatedness'] > prev['relatedness']:
            best[wgs_sg] = {
                'best_array_sg': array_sg,
                'relatedness': row['relatedness'],
                'ibs0': row['ibs0'],
                'n': row['n'],
            }

    # Combine mapping + best-match into per-SG status.
    out: dict[str, dict[str, Any]] = {}
    for r in mapping:
        wgs_sg = r['wgs_sg']
        expected = r['array_sg']
        base: dict[str, Any] = {
            'expected_array_sg': expected,
            'sample_external_id': r['sample_external_id'],
        }

        # Non-ready statuses pass straight through; somalier wasn't run for
        # these SGs so there's no relatedness to report.
        if r['status'] != 'ready':
            base['status'] = r['status']
            base['notes'] = r.get('notes', '')
            out[wgs_sg] = base
            continue

        b = best.get(wgs_sg)
        if b is None:
            # Ready but somehow no pair row -- shouldn't normally happen.
            base['status'] = 'no_pair_row'
            out[wgs_sg] = base
            continue

        base.update(
            best_array_sg=b['best_array_sg'],
            best_relatedness=b['relatedness'],
            best_ibs0=b['ibs0'],
            n_sites_compared=b['n'],
        )

        if b['n'] < n_min:
            base['status'] = 'insufficient_sites'
        elif b['relatedness'] < swap_max:
            base['status'] = 'discordant_no_match'
        elif b['relatedness'] < concordant_min:
            base['status'] = 'ambiguous'
        elif b['best_array_sg'] == expected:
            base['status'] = 'concordant'
        else:
            base['status'] = 'swap_detected'

        out[wgs_sg] = base
    return out


def build_swap_detected_slack_message(
    cohort_id: str,
    swap_check_by_sg: dict[str, dict[str, Any]],
) -> str | None:
    """Build the high-alert Slack message for any swap_detected SGs.

    Returns None if there are no swap_detected SGs (caller should skip the
    Slack send entirely in that case rather than posting an all-clear --
    swap-check is fact-gathering, not a routine pass/fail check).
    """
    swaps = [
        (sg_id, v) for sg_id, v in swap_check_by_sg.items()
        if v['status'] == 'swap_detected'
    ]
    if not swaps:
        return None

    lines = [
        '=================================',
        '🚨 ALERT: Sample Swap Detected 🚨',
        '=================================',
        f'*[{cohort_id}]* {len(swaps)} WGS sample(s) match a different array SG than expected:',
    ]
    for sg_id, v in swaps:
        lines.append(
            f'❗ `{sg_id}`: expected `{v.get("expected_array_sg")}`, '
            f'best match `{v.get("best_array_sg")}` '
            f'(relatedness={v.get("best_relatedness"):.3f}, '
            f'n_sites_compared={v.get("n_sites_compared")})',
        )
    return '\n'.join(lines)
