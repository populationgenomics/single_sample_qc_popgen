"""Tests for the sequencing-type-specific QC mapping and the qc_thresholds lookup."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from single_sample_qc_popgen.jobs import check_multiqc
from single_sample_qc_popgen.jobs.check_multiqc import QCChecker, build_qc_thresholds

EXOME_PCT_TARGET_GT_20X_THRESHOLD = 0.95

@pytest.fixture
def make_qc_checker(monkeypatch):
    """Builds a QCChecker with the metamist and somalier lookups faked out."""
    monkeypatch.setattr(check_multiqc, 'get_sgid_reported_sex_mapping_and_control_ids', lambda _cohort: ({}, set()))
    monkeypatch.setattr(check_multiqc, 'impute_sex_for_cohort', lambda _sgs: {})
    cohort = SimpleNamespace(get_sequencing_groups=list)

    def make(sequencing_type: str) -> QCChecker:
        return QCChecker(cohort, {}, sequencing_type)  # pyright: ignore[reportArgumentType]

    return make


def set_qc_thresholds(monkeypatch, qc_thresholds: dict) -> None:
    monkeypatch.setattr(check_multiqc, 'get_config', lambda: {'qc_thresholds': qc_thresholds})


# --- QC_MAPPING -----------------------------------------------------------


def test_qc_mapping_genome_coverage_metrics(make_qc_checker):
    mapping = make_qc_checker('genome').QC_MAPPING

    assert mapping['mean_coverage']['multiqc_report_name'] == 'Average sequenced coverage over genome'
    assert mapping['pct_genome_gt_20x']['multiqc_report_name'] == 'wgs pct of genome with coverage [20x:inf)'
    assert 'pct_target_gt_20x' not in mapping


def test_qc_mapping_exome_coverage_metrics(make_qc_checker):
    mapping = make_qc_checker('exome').QC_MAPPING

    assert mapping['mean_coverage']['multiqc_report_name'] == 'Average sequenced coverage over target region'
    assert (
        mapping['pct_target_gt_20x']['multiqc_report_name']
        == 'target bed pct of target region with coverage [20x:inf)'
    )
    assert 'pct_genome_gt_20x' not in mapping


def test_qc_mapping_non_coverage_metrics_are_shared(make_qc_checker):
    genome = make_qc_checker('genome').QC_MAPPING
    exome = make_qc_checker('exome').QC_MAPPING

    coverage_keys = {'mean_coverage', 'pct_genome_gt_20x', 'pct_target_gt_20x'}
    shared_genome = {k: v for k, v in genome.items() if k not in coverage_keys and 'calculator' not in v}
    shared_exome = {k: v for k, v in exome.items() if k not in coverage_keys and 'calculator' not in v}
    assert shared_genome == shared_exome
    assert shared_genome['q30_bases_pct']['multiqc_report_name'] == 'Q30 bases pct'


def test_qc_checker_unknown_sequencing_type_raises(make_qc_checker):
    with pytest.raises(ValueError, match="Unsupported workflow.sequencing_type 'transcriptome'"):
        make_qc_checker('transcriptome')


# --- build_qc_thresholds --------------------------------------------------


def test_build_qc_thresholds_exome_uses_exome_block_and_metric_names(monkeypatch, make_qc_checker):
    set_qc_thresholds(
        monkeypatch,
        {
            'genome': {'min': {'pct_genome_gt_20x': 0.9}},
            'exome': {'min': {'pct_target_gt_20x': EXOME_PCT_TARGET_GT_20X_THRESHOLD, 'mean_coverage': 60}},
        },
    )

    thresholds = build_qc_thresholds('exome', 'min', make_qc_checker('exome'))

    assert set(thresholds) == {'pct_target_gt_20x', 'mean_coverage'}
    assert thresholds['pct_target_gt_20x']['threshold'] == EXOME_PCT_TARGET_GT_20X_THRESHOLD
    assert (
        thresholds['pct_target_gt_20x']['multiqc_report_name']
        == 'target bed pct of target region with coverage [20x:inf)'
    )
    assert thresholds['mean_coverage']['multiqc_report_name'] == 'Average sequenced coverage over target region'


def test_build_qc_thresholds_missing_seq_type_block_raises(monkeypatch, make_qc_checker):
    set_qc_thresholds(monkeypatch, {'genome': {'min': {'mean_coverage': 30}}})

    with pytest.raises(RuntimeError, match=r'No \[qc_thresholds\.exome\] block'):
        build_qc_thresholds('exome', 'min', make_qc_checker('exome'))


def test_build_qc_thresholds_no_qc_thresholds_at_all_raises(monkeypatch, make_qc_checker):
    monkeypatch.setattr(check_multiqc, 'get_config', dict)

    with pytest.raises(RuntimeError, match=r'No \[qc_thresholds\.genome\] block'):
        build_qc_thresholds('genome', 'min', make_qc_checker('genome'))


def test_build_qc_thresholds_missing_sub_block_in_existing_block_is_empty(monkeypatch, make_qc_checker):
    set_qc_thresholds(monkeypatch, {'exome': {'min': {'mean_coverage': 60}}})
    qc_checker = make_qc_checker('exome')

    assert build_qc_thresholds('exome', 'max', qc_checker) == {}
    assert build_qc_thresholds('exome', 'equality', qc_checker) == {}
