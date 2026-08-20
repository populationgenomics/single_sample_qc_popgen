"""Tests for the explicit MultiQC input-path construction."""

from __future__ import annotations

import pytest
from cpg_utils import to_path

from single_sample_qc_popgen.constants import MULTIQC_INPUT_SUFFIXES
from single_sample_qc_popgen.jobs import run_multiqc


@pytest.fixture
def fake_dragen_prefix(monkeypatch):
    monkeypatch.setattr(
        run_multiqc,
        'get_dragen_output_path',
        lambda filename: to_path(f'gs://bucket/ica/dragen_3_7_8/output/{filename}'),
    )


@pytest.mark.usefixtures('fake_dragen_prefix')
def test_dragen_qc_paths_one_path_per_sg_per_suffix():
    paths = run_multiqc.dragen_qc_paths(['CPG000001', 'CPG000002'])

    assert len(paths) == 2 * len(MULTIQC_INPUT_SUFFIXES)
    assert (
        to_path('gs://bucket/ica/dragen_3_7_8/output/dragen_metrics/CPG000001/CPG000001.mapping_metrics.csv') in paths
    )
    assert (
        to_path('gs://bucket/ica/dragen_3_7_8/output/dragen_metrics/CPG000002/CPG000002.vc_metrics.csv') in paths
    )


@pytest.mark.usefixtures('fake_dragen_prefix')
def test_dragen_qc_paths_all_basenames_unique():
    paths = run_multiqc.dragen_qc_paths(['CPG000001', 'CPG000002', 'CPG000003'])
    basenames = [p.name for p in paths]
    assert len(set(basenames)) == len(basenames)


def test_dragen_qc_paths_excludes_time_metrics_and_unparsed_files():
    assert not any('time_metrics' in suffix for suffix in MULTIQC_INPUT_SUFFIXES)
    for unparsed in ('cnv_metrics.csv', 'sv_metrics.csv', 'roh_metrics.csv', 'wgs_hist.csv'):
        assert unparsed not in MULTIQC_INPUT_SUFFIXES


@pytest.mark.usefixtures('fake_dragen_prefix')
def test_dragen_qc_paths_duplicate_sg_names_raise():
    with pytest.raises(ValueError, match='Duplicate basenames'):
        run_multiqc.dragen_qc_paths(['CPG000001', 'CPG000001'])


def test_dragen_qc_paths_empty_cohort_raises():
    with pytest.raises(ValueError, match='No sequencing groups'):
        run_multiqc.dragen_qc_paths([])
