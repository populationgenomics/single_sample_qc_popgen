"""
PythonJob entrypoint: parse the somalier ``pairs.tsv``, join against the
WGS-to-array mapping (resolved at queue time), and emit the per-SG
``swap_check.json`` consumed by ``register_qc_metamist.py``.

The thresholds are loaded from config so they can be tuned per release
without code changes; defaults are calibrated against the COH8495 workshop
(expected matches sat at relatedness ~0.999 with n ~5800).
"""

from __future__ import annotations

import csv
import json
from typing import TYPE_CHECKING, Any

from cpg_utils import to_path
from cpg_utils.config import config_retrieve
from loguru import logger

from single_sample_qc_popgen.utils import load_json

if TYPE_CHECKING:
    from cpg_flow.targets import Cohort
    from cpg_utils import Path


# Default thresholds -- calibrated on COH8495.
#   Expected self-matches: relatedness ~0.999, n ~5800
#   Within-cohort first-degree relatives: relatedness ~0.5
RELATEDNESS_CONCORDANT_MIN = 0.8
RELATEDNESS_SWAP_MAX = 0.3
N_SITES_MIN = 500


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
    concordant_min: float = RELATEDNESS_CONCORDANT_MIN,
    swap_max: float = RELATEDNESS_SWAP_MAX,
    n_min: int = N_SITES_MIN,
) -> dict[str, dict[str, Any]]:
    """For each WGS SG, find the best array match and assign a status.

    Pure logic: no IO. Caller persists the result.

    Status taxonomy:
        concordant            -- best match IS the expected array SG, with
                                 relatedness >= concordant_min and n >= n_min
        swap_detected         -- best match is a DIFFERENT array SG with
                                 relatedness >= concordant_min
        discordant_no_match   -- best relatedness < swap_max (sample looks
                                 unlike every array sample in the cohort;
                                 not a swap, but a sample-quality red flag)
        ambiguous             -- best relatedness in (swap_max, concordant_min)
        insufficient_sites    -- best-pair n < n_min
        no_array_sg / array_pending_export / multiple_array_sgs / missing_sample
                              -- pass-through from the mapping classifier;
                                 no somalier comparison was run for these
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


def run(
    cohort: Cohort,
    mapping_path: str,
    pairs_tsv_path: str,
    output: Path,
) -> None:
    """PythonJob entry: load inputs, classify, write swap_check.json.

    ``mapping_path`` and ``pairs_tsv_path`` are stage outputs; ``cohort`` is
    only used here for logging context.
    """
    mapping: list[dict[str, Any]] = load_json(mapping_path)
    pairs = parse_pairs_tsv(pairs_tsv_path)

    thresholds = {
        'concordant_min': config_retrieve(
            ['workflow', 'swap_check', 'relatedness_concordant_min'],
            RELATEDNESS_CONCORDANT_MIN,
        ),
        'swap_max': config_retrieve(
            ['workflow', 'swap_check', 'relatedness_swap_max'],
            RELATEDNESS_SWAP_MAX,
        ),
        'n_min': config_retrieve(
            ['workflow', 'swap_check', 'n_sites_min'],
            N_SITES_MIN,
        ),
    }

    swap_check_by_sg = classify_swap_check(mapping, pairs, **thresholds)

    summary: dict[str, int] = {}
    for v in swap_check_by_sg.values():
        summary[v['status']] = summary.get(v['status'], 0) + 1
    logger.info(f'[SwapCheck] cohort={cohort.id} summary={summary}')

    for sg_id, v in swap_check_by_sg.items():
        if v['status'] == 'swap_detected':
            logger.warning(
                f'❗ {sg_id} (expected {v.get("expected_array_sg")}) '
                f'best match is {v.get("best_array_sg")} '
                f'(rel={v.get("best_relatedness"):.3f}, n={v.get("n_sites_compared")})'
            )

    with to_path(output).open('w') as f:
        json.dump(swap_check_by_sg, f, indent=2)
