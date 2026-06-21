"""Unit tests for the swap-check pure logic.

Covers the queue-time mapping classifier and path helpers in
``metamist_utils`` and the pairs.tsv parsing / classification / Slack
formatting in ``jobs.classify_swaps``. All inputs are synthetic; no metamist
round-trip is involved.
"""
from __future__ import annotations

from typing import Any

import pytest

from single_sample_qc_popgen.jobs.classify_swaps import (
    build_swap_detected_slack_message,
    classify_swap_check,
    parse_pairs_tsv,
)
from single_sample_qc_popgen.metamist_utils import (
    classify_wgs_to_array_mapping,
    derive_pgen_sibling_paths,
    read_psam_array_sgs,
)

THRESHOLDS = {'concordant_min': 0.8, 'swap_max': 0.3, 'n_min': 500}


# --- derive_pgen_sibling_paths --------------------------------------------


def test_derive_pgen_sibling_paths_swaps_suffixes():
    pgen, pvar, psam = derive_pgen_sibling_paths('gs://bucket/dir/20260330_cohort.pgen')
    assert pgen == 'gs://bucket/dir/20260330_cohort.pgen'
    assert pvar == 'gs://bucket/dir/20260330_cohort.pvar'
    assert psam == 'gs://bucket/dir/20260330_cohort.psam'


def test_derive_pgen_sibling_paths_rejects_non_pgen():
    with pytest.raises(ValueError, match=r'Expected a \.pgen path'):
        derive_pgen_sibling_paths('gs://bucket/dir/cohort.bed')


# --- read_psam_array_sgs --------------------------------------------------


def test_read_psam_array_sgs_extracts_iids(tmp_path):
    psam = tmp_path / 'cohort.psam'
    psam.write_text('#FID\tIID\tSEX\n0\tCPG1\t1\n0\tCPG2\t2\n')
    assert read_psam_array_sgs(psam) == {'CPG1', 'CPG2'}


def test_read_psam_array_sgs_skips_blank_and_comment_lines(tmp_path):
    psam = tmp_path / 'cohort.psam'
    psam.write_text('#IID\tSEX\nCPG1\t1\n\nCPG2\t2\n')
    assert read_psam_array_sgs(psam) == {'CPG1', 'CPG2'}


def test_read_psam_array_sgs_empty_raises(tmp_path):
    psam = tmp_path / 'cohort.psam'
    psam.write_text('')
    with pytest.raises(RuntimeError, match='psam is empty'):
        read_psam_array_sgs(psam)


def test_read_psam_array_sgs_no_iid_column_raises(tmp_path):
    psam = tmp_path / 'cohort.psam'
    psam.write_text('#FID\tSEX\n0\t1\n')
    with pytest.raises(RuntimeError, match='No IID column'):
        read_psam_array_sgs(psam)


# --- parse_pairs_tsv ------------------------------------------------------


def test_parse_pairs_tsv_strips_header_and_coerces_types(tmp_path):
    rel, ibs0, n = 0.99, 3, 5000
    pairs = tmp_path / 'cohort.pairs.tsv'
    pairs.write_text(
        '#sample_a\tsample_b\trelatedness\tibs0\tn\n'
        f'WGS1\tARR1\t{rel}\t{ibs0}\t{n}\n'
    )
    rows = parse_pairs_tsv(pairs)
    assert len(rows) == 1
    row = rows[0]
    assert row['sample_a'] == 'WGS1'
    assert row['sample_b'] == 'ARR1'
    assert row['relatedness'] == pytest.approx(rel)
    assert row['ibs0'] == ibs0
    assert row['n'] == n


def test_parse_pairs_tsv_missing_values_default(tmp_path):
    pairs = tmp_path / 'cohort.pairs.tsv'
    pairs.write_text('#sample_a\tsample_b\trelatedness\tibs0\tn\nWGS1\tARR1\t\t\t\n')
    row = parse_pairs_tsv(pairs)[0]
    assert row['relatedness'] != row['relatedness']  # nan
    assert row['ibs0'] == 0
    assert row['n'] == 0


def test_parse_pairs_tsv_header_only_is_empty(tmp_path):
    pairs = tmp_path / 'cohort.pairs.tsv'
    pairs.write_text('#sample_a\tsample_b\trelatedness\tibs0\tn\n')
    assert parse_pairs_tsv(pairs) == []


# --- classify_wgs_to_array_mapping ----------------------------------------


def _record(wgs_sg, candidates, *, sample_present=True, ext='EXT') -> dict[str, Any]:
    return {
        'wgs_sg': wgs_sg,
        'sample_external_id': ext,
        'sample_present': sample_present,
        'array_candidates': candidates,
    }


def test_classify_mapping_statuses():
    records = [
        _record('WGS_READY', ['ARR_A']),
        _record('WGS_PENDING', ['ARR_MISSING']),
        _record('WGS_DISAMBIG', ['ARR_A', 'ARR_OTHER']),
        _record('WGS_MULTI', ['ARR_X', 'ARR_Y']),
        _record('WGS_NONE', []),
        _record('WGS_NOSAMPLE', [], sample_present=False),
    ]
    pgen_sgs = {'ARR_A'}
    by_sg = {r['wgs_sg']: r for r in classify_wgs_to_array_mapping(records, pgen_sgs)}

    assert by_sg['WGS_READY']['status'] == 'ready'
    assert by_sg['WGS_READY']['array_sg'] == 'ARR_A'
    assert by_sg['WGS_PENDING']['status'] == 'array_pending_export'
    assert by_sg['WGS_DISAMBIG']['status'] == 'ready'
    assert by_sg['WGS_DISAMBIG']['array_sg'] == 'ARR_A'
    assert by_sg['WGS_DISAMBIG']['notes']
    assert by_sg['WGS_MULTI']['status'] == 'multiple_array_sgs'
    assert by_sg['WGS_MULTI']['array_sg'] is None
    assert by_sg['WGS_NONE']['status'] == 'no_array_sg'
    assert by_sg['WGS_NOSAMPLE']['status'] == 'missing_sample'


# --- classify_swap_check --------------------------------------------------


def _mapping(wgs_sg, array_sg, status='ready', ext='EXT', notes='') -> dict[str, Any]:
    return {
        'wgs_sg': wgs_sg,
        'array_sg': array_sg,
        'status': status,
        'sample_external_id': ext,
        'notes': notes,
    }


def _pair(a, b, rel, n=5000, ibs0=0) -> dict[str, Any]:
    return {'sample_a': a, 'sample_b': b, 'relatedness': rel, 'ibs0': ibs0, 'n': n}


def test_classify_swap_check_all_branches():
    mapping = [
        _mapping('WGS_CONC', 'ARR_CONC'),
        _mapping('WGS_SWAP', 'ARR_SWAP_EXP'),
        _mapping('WGS_AMB', 'ARR_AMB'),
        _mapping('WGS_DISC', 'ARR_DISC'),
        _mapping('WGS_INSUF', 'ARR_INSUF'),
        _mapping('WGS_NOPAIR', 'ARR_NOPAIR'),
        _mapping('WGS_PENDING', None, status='array_pending_export', notes='waiting'),
    ]
    pairs = [
        _pair('WGS_CONC', 'ARR_CONC', 0.99),
        # best match is a different array SG than expected -> swap
        _pair('WGS_SWAP', 'ARR_CONC', 0.95),
        _pair('WGS_SWAP', 'ARR_SWAP_EXP', 0.20),
        _pair('WGS_AMB', 'ARR_AMB', 0.50),
        _pair('WGS_DISC', 'ARR_DISC', 0.10),
        _pair('WGS_INSUF', 'ARR_INSUF', 0.99, n=100),
        # WGS<->WGS pair must be ignored
        _pair('WGS_CONC', 'WGS_AMB', 0.40),
    ]
    out = classify_swap_check(mapping, pairs, **THRESHOLDS)

    assert out['WGS_CONC']['status'] == 'concordant'
    assert out['WGS_SWAP']['status'] == 'swap_detected'
    assert out['WGS_SWAP']['best_array_sg'] == 'ARR_CONC'
    assert out['WGS_AMB']['status'] == 'ambiguous'
    assert out['WGS_DISC']['status'] == 'discordant_no_match'
    assert out['WGS_INSUF']['status'] == 'insufficient_sites'
    assert out['WGS_NOPAIR']['status'] == 'no_pair_row'
    assert out['WGS_PENDING']['status'] == 'array_pending_export'
    assert out['WGS_PENDING']['notes'] == 'waiting'


def test_classify_swap_check_insufficient_sites_beats_relatedness():
    mapping = [_mapping('WGS1', 'ARR1')]
    pairs = [_pair('WGS1', 'ARR1', 0.99, n=10)]
    out = classify_swap_check(mapping, pairs, **THRESHOLDS)
    assert out['WGS1']['status'] == 'insufficient_sites'


# --- build_swap_detected_slack_message ------------------------------------


def test_slack_message_none_when_no_swaps():
    swap_check = {'WGS1': {'status': 'concordant'}}
    assert build_swap_detected_slack_message('COH1', 'ourdna-test', swap_check) is None


def test_slack_message_includes_project_cohort_and_swap_details():
    swap_check = {
        'WGS_SWAP': {
            'status': 'swap_detected',
            'expected_array_sg': 'ARR_EXP',
            'best_array_sg': 'ARR_BEST',
            'best_relatedness': 1.0,
            'n_sites_compared': 5726,
        },
        'WGS_OK': {'status': 'concordant'},
    }
    msg = build_swap_detected_slack_message('COH13529', 'ourdna-test', swap_check)
    assert msg is not None
    assert 'ourdna-test' in msg
    assert 'COH13529' in msg
    assert 'WGS_SWAP' in msg
    assert 'ARR_EXP' in msg
    assert 'ARR_BEST' in msg
    assert 'WGS_OK' not in msg  # concordant SGs are not listed
