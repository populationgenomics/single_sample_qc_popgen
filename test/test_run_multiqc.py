"""Tests for the explicit MultiQC input-path construction."""

from __future__ import annotations

import pytest
from cpg_utils import to_path

from single_sample_qc_popgen.constants import MULTIQC_BASE_INPUT_SUFFIXES, get_sequencing_type_profile
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
    paths = run_multiqc.dragen_qc_paths(['CPG000001', 'CPG000002'], 'genome')

    assert len(paths) == 2 * len(MULTIQC_BASE_INPUT_SUFFIXES)
    assert (
        to_path('gs://bucket/ica/dragen_3_7_8/output/dragen_metrics/CPG000001/CPG000001.mapping_metrics.csv') in paths
    )
    assert (
        to_path('gs://bucket/ica/dragen_3_7_8/output/dragen_metrics/CPG000002/CPG000002.vc_metrics.csv') in paths
    )


@pytest.mark.usefixtures('fake_dragen_prefix')
def test_dragen_qc_paths_all_basenames_unique():
    paths = run_multiqc.dragen_qc_paths(['CPG000001', 'CPG000002', 'CPG000003'], 'genome')
    basenames = [p.name for p in paths]
    assert len(set(basenames)) == len(basenames)


def test_dragen_qc_paths_excludes_time_metrics_and_unparsed_files():
    for sequencing_type in ('genome', 'exome'):
        suffixes = get_sequencing_type_profile(sequencing_type).multiqc_input_suffixes
        assert not any('time_metrics' in suffix for suffix in suffixes)
        for unparsed in ('cnv_metrics.csv', 'sv_metrics.csv', 'roh_metrics.csv', 'wgs_hist.csv'):
            assert unparsed not in suffixes


@pytest.mark.usefixtures('fake_dragen_prefix')
def test_dragen_qc_paths_duplicate_sg_names_raise():
    with pytest.raises(ValueError, match='Duplicate basenames'):
        run_multiqc.dragen_qc_paths(['CPG000001', 'CPG000001'], 'genome')


def test_dragen_qc_paths_empty_cohort_raises():
    with pytest.raises(ValueError, match='No sequencing groups'):
        run_multiqc.dragen_qc_paths([], 'genome')


TARGET_BED_SUFFIXES = {
    'target_bed_coverage_metrics.csv',
    'target_bed_overall_mean_cov.csv',
    'target_bed_contig_mean_cov.csv',
    'target_bed_fine_hist.csv',
}


def test_genome_suffixes_are_the_base_list_without_target_bed_files():
    suffixes = get_sequencing_type_profile('genome').multiqc_input_suffixes
    assert suffixes == MULTIQC_BASE_INPUT_SUFFIXES
    assert not TARGET_BED_SUFFIXES & set(suffixes)


def test_exome_suffixes_are_the_base_list_plus_target_bed_files():
    suffixes = get_sequencing_type_profile('exome').multiqc_input_suffixes
    assert set(suffixes) == set(MULTIQC_BASE_INPUT_SUFFIXES) | TARGET_BED_SUFFIXES
    assert len(suffixes) == len(set(suffixes))


@pytest.mark.usefixtures('fake_dragen_prefix')
def test_dragen_qc_paths_exome_stages_target_bed_files_genome_does_not():
    exome_paths = run_multiqc.dragen_qc_paths(['CPG000001', 'CPG000002'], 'exome')
    genome_paths = run_multiqc.dragen_qc_paths(['CPG000001', 'CPG000002'], 'genome')

    assert len(exome_paths) == 2 * (len(MULTIQC_BASE_INPUT_SUFFIXES) + len(TARGET_BED_SUFFIXES))
    assert (
        to_path(
            'gs://bucket/ica/dragen_3_7_8/output/dragen_metrics/CPG000002/CPG000002.target_bed_fine_hist.csv'
        )
        in exome_paths
    )
    assert len({p.name for p in exome_paths}) == len(exome_paths)
    assert not any('target_bed' in p.name for p in genome_paths)


@pytest.mark.usefixtures('fake_dragen_prefix')
def test_dragen_qc_paths_unknown_sequencing_type_raises():
    with pytest.raises(ValueError, match="Unsupported workflow.sequencing_type 'transcriptome'"):
        run_multiqc.dragen_qc_paths(['CPG000001'], 'transcriptome')
