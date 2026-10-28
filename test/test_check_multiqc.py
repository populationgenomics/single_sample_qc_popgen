"""Tests for the sequencing-type-specific QC mapping and the qc_thresholds lookup."""

from __future__ import annotations

import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest

from single_sample_qc_popgen.jobs import check_multiqc
from single_sample_qc_popgen.jobs.check_multiqc import QCChecker, build_qc_thresholds

EXOME_PCT_TARGET_GT_20X_THRESHOLD = 0.95
GENOME_MEDIAN_COVERAGE_THRESHOLD = 25
EXOME_MEDIAN_COVERAGE_THRESHOLD = 50
DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / 'config' / 'single_sample_qc_popgen.toml'

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
    assert mapping['median_coverage']['multiqc_report_name'] == 'wgs median autosomal coverage over genome'
    assert mapping['pct_genome_gt_20x']['multiqc_report_name'] == 'wgs pct of genome with coverage [20x:inf)'
    assert 'pct_target_gt_20x' not in mapping


def test_qc_mapping_exome_coverage_metrics(make_qc_checker):
    mapping = make_qc_checker('exome').QC_MAPPING

    assert mapping['mean_coverage']['multiqc_report_name'] == 'Average sequenced coverage over target region'
    assert (
        mapping['median_coverage']['multiqc_report_name']
        == 'target bed median autosomal coverage over target region'
    )
    assert (
        mapping['pct_target_gt_20x']['multiqc_report_name']
        == 'target bed pct of target region with coverage [20x:inf)'
    )
    assert 'pct_genome_gt_20x' not in mapping


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

    with pytest.raises(RuntimeError, match=r'No thresholds under \[qc_thresholds\.exome\]'):
        build_qc_thresholds('exome', 'min', make_qc_checker('exome'))


def test_build_qc_thresholds_no_qc_thresholds_at_all_raises(monkeypatch, make_qc_checker):
    monkeypatch.setattr(check_multiqc, 'get_config', dict)

    with pytest.raises(RuntimeError, match=r'No thresholds under \[qc_thresholds\.genome\]'):
        build_qc_thresholds('genome', 'min', make_qc_checker('genome'))


@pytest.mark.parametrize(
    'exome_block',
    [{}, {'min': {}}, {'min': {}, 'max': {}, 'equality': {}}],
    ids=['bare_header', 'empty_min', 'all_sub_blocks_empty'],
)
def test_build_qc_thresholds_empty_seq_type_block_raises(monkeypatch, make_qc_checker, exome_block):
    set_qc_thresholds(monkeypatch, {'exome': exome_block})

    with pytest.raises(RuntimeError, match=r'No thresholds under \[qc_thresholds\.exome\]'):
        build_qc_thresholds('exome', 'min', make_qc_checker('exome'))


def test_build_qc_thresholds_missing_sub_block_in_existing_block_is_empty(monkeypatch, make_qc_checker):
    set_qc_thresholds(monkeypatch, {'exome': {'min': {'mean_coverage': 60}}})
    qc_checker = make_qc_checker('exome')

    assert build_qc_thresholds('exome', 'max', qc_checker) == {}
    assert build_qc_thresholds('exome', 'equality', qc_checker) == {}


def test_build_qc_thresholds_median_coverage_is_thresholdable(monkeypatch, make_qc_checker):
    set_qc_thresholds(
        monkeypatch,
        {
            'genome': {'min': {'median_coverage': GENOME_MEDIAN_COVERAGE_THRESHOLD}},
            'exome': {'min': {'median_coverage': EXOME_MEDIAN_COVERAGE_THRESHOLD}},
        },
    )

    genome = build_qc_thresholds('genome', 'min', make_qc_checker('genome'))
    exome = build_qc_thresholds('exome', 'min', make_qc_checker('exome'))

    assert genome['median_coverage']['threshold'] == GENOME_MEDIAN_COVERAGE_THRESHOLD
    assert genome['median_coverage']['multiqc_report_name'] == 'wgs median autosomal coverage over genome'
    assert exome['median_coverage']['threshold'] == EXOME_MEDIAN_COVERAGE_THRESHOLD
    assert exome['median_coverage']['multiqc_report_name'] == 'target bed median autosomal coverage over target region'


def test_build_qc_thresholds_unmapped_metric_raises(monkeypatch, make_qc_checker):
    # The realistic mistake: a genome-named metric in an exome block.
    set_qc_thresholds(monkeypatch, {'exome': {'min': {'pct_genome_gt_20x': 0.9}}})

    with pytest.raises(RuntimeError, match='pct_genome_gt_20x') as exc_info:
        build_qc_thresholds('exome', 'min', make_qc_checker('exome'))

    message = str(exc_info.value)
    assert '[qc_thresholds.exome.min]' in message
    # The message lists the valid metrics so the fix is obvious.
    assert 'pct_target_gt_20x' in message
    assert 'median_coverage' in message


def test_build_qc_thresholds_verifybamid_contamination_raises(monkeypatch, make_qc_checker):
    # DRAGEN runs report no FREEMIX, so a verifybamid threshold could never be enforced.
    set_qc_thresholds(monkeypatch, {'genome': {'max': {'contamination_verifybamid': 0.05}}})

    with pytest.raises(RuntimeError, match='contamination_verifybamid'):
        build_qc_thresholds('genome', 'max', make_qc_checker('genome'))


@pytest.mark.parametrize('config_key', ['min', 'max', 'equality'])
def test_build_qc_thresholds_unmapped_metric_raises_for_every_check_type(monkeypatch, make_qc_checker, config_key):
    set_qc_thresholds(monkeypatch, {'genome': {config_key: {'not_a_metric': 1}}})

    with pytest.raises(RuntimeError, match=rf'\[qc_thresholds\.genome\.{config_key}\].*not_a_metric'):
        build_qc_thresholds('genome', config_key, make_qc_checker('genome'))


@pytest.mark.parametrize('seq_type', ['genome', 'exome'])
def test_build_qc_thresholds_shipped_default_config_builds(monkeypatch, make_qc_checker, seq_type):
    with DEFAULT_CONFIG_PATH.open('rb') as f:
        default_config = tomllib.load(f)
    set_qc_thresholds(monkeypatch, default_config['qc_thresholds'])
    qc_checker = make_qc_checker(seq_type)

    for config_key in ('min', 'max', 'equality'):
        thresholds = build_qc_thresholds(seq_type, config_key, qc_checker)
        assert set(thresholds) == set(default_config['qc_thresholds'][seq_type].get(config_key, {}))
