"""Tests for the sequencing-type-specific QC mapping, the qc_thresholds lookup, and the per-SG threshold checks."""

from __future__ import annotations

import copy
import math
import tomllib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from single_sample_qc_popgen.constants import get_sequencing_type_profile
from single_sample_qc_popgen.jobs import check_multiqc
from single_sample_qc_popgen.jobs.check_multiqc import (
    QCChecker,
    QCResults,
    ReportedSexInfo,
    build_qc_thresholds,
    check_cohort_metrics,
    check_threshold_metrics_in_report,
    generate_slack_message,
    get_reported_sex_info,
    resolve_reported_sex,
)

EXOME_PCT_TARGET_GT_20X_THRESHOLD = 0.95
GENOME_MEDIAN_COVERAGE_THRESHOLD = 25
EXOME_MEDIAN_COVERAGE_THRESHOLD = 50
DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / 'config' / 'single_sample_qc_popgen.toml'

@pytest.fixture
def make_qc_checker(monkeypatch):
    """Builds a QCChecker with the metamist and somalier lookups faked out.

    The cohort has an SG for each of sg_ids; reported_sex is what the metamist lookup returns.
    """

    def make(
        sequencing_type: str,
        multiqc_data: dict | None = None,
        sg_ids: tuple[str, ...] = (),
        reported_sex: ReportedSexInfo | None = None,
    ) -> QCChecker:
        monkeypatch.setattr(
            check_multiqc,
            'get_reported_sex_info',
            lambda _cohort: reported_sex or ReportedSexInfo(sex_by_sg={}, discordant_by_sg={}, control_sg_ids=set()),
        )
        monkeypatch.setattr(check_multiqc, 'impute_sex_for_cohort', lambda _sgs: {})
        cohort = SimpleNamespace(get_sequencing_groups=lambda: [SimpleNamespace(id=sg_id) for sg_id in sg_ids])
        return QCChecker(cohort, multiqc_data or {}, sequencing_type)  # pyright: ignore[reportArgumentType]

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


# --- check_threshold_metrics_in_report ----------------------------------


GENOME_ONLY_STATS = {'DRAGEN': {'CPG000001': {get_sequencing_type_profile('genome').median_coverage.multiqc_name: 2.6}}}


def test_check_threshold_metrics_in_report_raises_when_metric_in_no_section(monkeypatch, make_qc_checker):
    set_qc_thresholds(monkeypatch, {'exome': {'min': {'median_coverage': EXOME_MEDIAN_COVERAGE_THRESHOLD}}})
    threshold_map = build_qc_thresholds('exome', 'min', make_qc_checker('exome'))

    with pytest.raises(RuntimeError, match='target bed median autosomal coverage over target region'):
        check_threshold_metrics_in_report(threshold_map, GENOME_ONLY_STATS)


def test_check_threshold_metrics_in_report_passes_when_only_some_samples_lack_metric(monkeypatch, make_qc_checker):
    set_qc_thresholds(monkeypatch, {'genome': {'min': {'median_coverage': GENOME_MEDIAN_COVERAGE_THRESHOLD}}})
    threshold_map = build_qc_thresholds('genome', 'min', make_qc_checker('genome'))
    stats = {'DRAGEN': {**GENOME_ONLY_STATS['DRAGEN'], 'CPG000002': {}}}

    check_threshold_metrics_in_report(threshold_map, stats)


# --- check_cohort_metrics -------------------------------------------------

MAPPING_RATE_THRESHOLD = 80
PCT_GT_20X_THRESHOLD = 0.9
CHIMERA_THRESHOLD = 0.03
PASSING_MAPPING_RATE = 99.5
PASSING_PCT_GT_20X = 0.95
FAILING_MAPPING_RATE = 70.0
# Thresholds in config order: min (mapping rate, pct >= 20x), max (chimera), equality (ploidy).
GENOME_THRESHOLDS = {
    'genome': {
        'min': {'mapping_rate_pct': MAPPING_RATE_THRESHOLD, 'pct_genome_gt_20x': PCT_GT_20X_THRESHOLD},
        'max': {'chimera_rate': CHIMERA_THRESHOLD},
        'equality': {'ploidy_estimation': True},
    }
}
MALE = 1
FEMALE = 2
SG_A = 'CPG000001'
SG_B = 'CPG000002'
SG_C = 'CPG000003'


def make_stats(sg_ids: tuple[str, ...]) -> dict[str, dict]:
    """General-stats sections holding every metric in GENOME_THRESHOLDS, split as in a real report."""
    return {
        'DRAGEN': {
            sg: {
                'Mapped reads pct': PASSING_MAPPING_RATE,
                'Supplementary (chimeric) alignments': 1000,
                'Total alignments': 1_000_000,
            }
            for sg in sg_ids
        },
        'DRAGEN_2': {sg: {'Ploidy estimation': 'XY'} for sg in sg_ids},
        'DRAGEN_3': {sg: {'wgs pct of genome with coverage [20x:inf)': PASSING_PCT_GT_20X} for sg in sg_ids},
    }


def run_checks(
    monkeypatch,
    make_qc_checker,
    stats: dict,
    sg_ids: tuple[str, ...] = (SG_A, SG_B),
    sex_by_sg: dict[str, int] | None = None,
    discordant_by_sg: dict[str, str] | None = None,
    control_sg_ids: set[str] | None = None,
    thresholds: dict | None = None,
) -> QCResults:
    """Runs check_cohort_metrics on a genome cohort of sg_ids; every SG is male unless sex_by_sg says otherwise."""
    set_qc_thresholds(monkeypatch, thresholds or GENOME_THRESHOLDS)
    reported_sex = ReportedSexInfo(
        sex_by_sg={sg: MALE for sg in sg_ids} if sex_by_sg is None else sex_by_sg,
        discordant_by_sg=discordant_by_sg or {},
        control_sg_ids=control_sg_ids or set(),
    )
    qc_checker = make_qc_checker('genome', stats, sg_ids, reported_sex)
    return check_cohort_metrics(qc_checker, 'genome')


def test_check_cohort_metrics_passing_sgs_have_no_failures(monkeypatch, make_qc_checker):
    results = run_checks(monkeypatch, make_qc_checker, make_stats((SG_A, SG_B)))

    assert results.bad_lines_by_sample == {}
    assert results.ploidy_not_checked == []


def test_check_cohort_metrics_flags_values_failing_thresholds(monkeypatch, make_qc_checker):
    stats = make_stats((SG_A, SG_B))
    stats['DRAGEN'][SG_A]['Mapped reads pct'] = FAILING_MAPPING_RATE
    stats['DRAGEN'][SG_A]['Supplementary (chimeric) alignments'] = 50_000
    stats['DRAGEN_2'][SG_A]['Ploidy estimation'] = 'XO'

    results = run_checks(monkeypatch, make_qc_checker, stats)

    assert results.bad_lines_by_sample == {
        SG_A: [
            'Mapping Rate (%)=70.0000 < 80.0000',
            'Chimera Rate=0.0500 > 0.0300',
            'Ploidy Estimation is XO (expected XY)',
        ]
    }


def test_check_cohort_metrics_ploidy_expects_xx_for_female(monkeypatch, make_qc_checker):
    stats = make_stats((SG_A, SG_B))
    stats['DRAGEN_2'][SG_B]['Ploidy estimation'] = 'XX'

    results = run_checks(monkeypatch, make_qc_checker, stats, sex_by_sg={SG_A: MALE, SG_B: FEMALE})

    assert results.bad_lines_by_sample == {}


def test_check_cohort_metrics_metric_absent_for_one_sg_fails_as_missing(monkeypatch, make_qc_checker):
    stats = make_stats((SG_A, SG_B))
    del stats['DRAGEN'][SG_B]['Mapped reads pct']

    results = run_checks(monkeypatch, make_qc_checker, stats)

    assert results.bad_lines_by_sample == {SG_B: ['Mapping Rate (%) missing']}


def test_check_cohort_metrics_sg_absent_from_metric_section_fails_as_missing(monkeypatch, make_qc_checker):
    stats = make_stats((SG_A, SG_B))
    del stats['DRAGEN_3'][SG_B]

    results = run_checks(monkeypatch, make_qc_checker, stats)

    assert results.bad_lines_by_sample == {SG_B: ['Pct Genome @ >20x missing']}


def test_check_cohort_metrics_sg_absent_from_report_fails_every_threshold_as_missing(monkeypatch, make_qc_checker):
    results = run_checks(monkeypatch, make_qc_checker, make_stats((SG_A,)), sg_ids=(SG_A, SG_B))

    assert results.bad_lines_by_sample == {
        SG_B: [
            'Mapping Rate (%) missing',
            'Pct Genome @ >20x missing',
            'Chimera Rate missing',
            'Ploidy Estimation missing',
        ]
    }


@pytest.mark.parametrize('missing_value', [None, math.nan, math.inf, -math.inf], ids=['none', 'nan', 'inf', 'neg_inf'])
def test_check_cohort_metrics_non_finite_or_null_value_fails_as_missing(monkeypatch, make_qc_checker, missing_value):
    # A NaN compares False against every threshold and would otherwise pass.
    stats = make_stats((SG_A, SG_B))
    stats['DRAGEN'][SG_B]['Mapped reads pct'] = missing_value

    results = run_checks(monkeypatch, make_qc_checker, stats)

    assert results.bad_lines_by_sample == {SG_B: ['Mapping Rate (%) missing']}


def test_check_cohort_metrics_chimera_with_zero_total_alignments_fails_as_missing(monkeypatch, make_qc_checker):
    stats = make_stats((SG_A, SG_B))
    stats['DRAGEN'][SG_B]['Total alignments'] = 0

    results = run_checks(monkeypatch, make_qc_checker, stats)

    assert results.bad_lines_by_sample == {SG_B: ['Chimera Rate missing']}


@pytest.mark.parametrize('absent_input', ['Supplementary (chimeric) alignments', 'Total alignments'])
def test_check_cohort_metrics_chimera_with_absent_input_fails_as_missing(monkeypatch, make_qc_checker, absent_input):
    stats = make_stats((SG_A, SG_B))
    del stats['DRAGEN'][SG_B][absent_input]

    results = run_checks(monkeypatch, make_qc_checker, stats)

    assert results.bad_lines_by_sample == {SG_B: ['Chimera Rate missing']}


def test_check_cohort_metrics_chimera_with_nan_input_fails_as_missing(monkeypatch, make_qc_checker):
    stats = make_stats((SG_A, SG_B))
    stats['DRAGEN'][SG_B]['Supplementary (chimeric) alignments'] = math.nan

    results = run_checks(monkeypatch, make_qc_checker, stats)

    assert results.bad_lines_by_sample == {SG_B: ['Chimera Rate missing']}


def test_check_cohort_metrics_unknown_ploidy_with_known_sex_fails_as_missing(monkeypatch, make_qc_checker):
    stats = make_stats((SG_A, SG_B))
    stats['DRAGEN_2'][SG_B]['Ploidy estimation'] = 'Unknown'

    results = run_checks(monkeypatch, make_qc_checker, stats)

    assert results.bad_lines_by_sample == {SG_B: ['Ploidy Estimation missing']}
    assert results.ploidy_not_checked == []


@pytest.mark.parametrize('how', ['key_absent', 'sg_absent', 'none'])
def test_check_cohort_metrics_no_dragen_ploidy_with_known_sex_fails_as_missing(monkeypatch, make_qc_checker, how):
    stats = make_stats((SG_A, SG_B))
    if how == 'key_absent':
        del stats['DRAGEN_2'][SG_B]['Ploidy estimation']
    elif how == 'sg_absent':
        del stats['DRAGEN_2'][SG_B]
    else:
        stats['DRAGEN_2'][SG_B]['Ploidy estimation'] = None

    results = run_checks(monkeypatch, make_qc_checker, stats)

    assert results.bad_lines_by_sample == {SG_B: ['Ploidy Estimation missing']}


def test_check_cohort_metrics_metric_with_min_and_max_threshold_reports_missing_once(monkeypatch, make_qc_checker):
    thresholds = {'genome': {'min': {'mapping_rate_pct': 80}, 'max': {'mapping_rate_pct': 100}}}
    stats = make_stats((SG_A, SG_B))
    del stats['DRAGEN'][SG_B]['Mapped reads pct']

    results = run_checks(monkeypatch, make_qc_checker, stats, thresholds=thresholds)

    assert results.bad_lines_by_sample == {SG_B: ['Mapping Rate (%) missing']}


def test_check_cohort_metrics_checks_cohort_sgs_only(monkeypatch, make_qc_checker):
    stats = make_stats((SG_A, SG_B, SG_C))
    stats['DRAGEN'][SG_C]['Mapped reads pct'] = FAILING_MAPPING_RATE

    results = run_checks(monkeypatch, make_qc_checker, stats, sg_ids=(SG_A, SG_B))

    assert results.bad_lines_by_sample == {}


def test_check_cohort_metrics_discordant_reported_sex_fails_without_ploidy_line(monkeypatch, make_qc_checker):
    discordant_line = 'Reported sex discordant (participant_portal_reported_sex=0, reportedSex=1)'
    stats = make_stats((SG_A, SG_B))
    # Neither a mismatching nor a missing ploidy adds a ploidy line for the discordant SG.
    stats['DRAGEN_2'][SG_B]['Ploidy estimation'] = 'XO'

    results = run_checks(
        monkeypatch,
        make_qc_checker,
        stats,
        sex_by_sg={SG_A: MALE},
        discordant_by_sg={SG_B: discordant_line},
    )

    assert results.bad_lines_by_sample == {SG_B: [discordant_line]}
    assert results.ploidy_not_checked == []

    stats['DRAGEN_2'][SG_B]['Ploidy estimation'] = 'Unknown'
    results = run_checks(
        monkeypatch,
        make_qc_checker,
        stats,
        sex_by_sg={SG_A: MALE},
        discordant_by_sg={SG_B: discordant_line},
    )

    assert results.bad_lines_by_sample == {SG_B: [discordant_line]}


def test_check_cohort_metrics_discordant_sg_still_gets_other_checks(monkeypatch, make_qc_checker):
    discordant_line = 'Reported sex discordant (participant_portal_reported_sex=1, reportedSex=2)'
    stats = make_stats((SG_A, SG_B))
    stats['DRAGEN'][SG_B]['Mapped reads pct'] = FAILING_MAPPING_RATE

    results = run_checks(
        monkeypatch, make_qc_checker, stats, sex_by_sg={SG_A: MALE}, discordant_by_sg={SG_B: discordant_line}
    )

    assert results.bad_lines_by_sample == {SG_B: [discordant_line, 'Mapping Rate (%)=70.0000 < 80.0000']}


def test_check_cohort_metrics_no_reported_sex_is_not_a_failure_and_is_listed(
    monkeypatch, make_qc_checker, warnings_logged
):
    stats = make_stats((SG_A, SG_B, SG_C))
    # The ploidy is not compared for these SGs, whatever DRAGEN estimated.
    stats['DRAGEN_2'][SG_B]['Ploidy estimation'] = 'XO'
    stats['DRAGEN_2'][SG_C]['Ploidy estimation'] = 'Unknown'

    results = run_checks(monkeypatch, make_qc_checker, stats, sg_ids=(SG_A, SG_B, SG_C), sex_by_sg={SG_A: MALE})

    assert results.bad_lines_by_sample == {}
    assert results.ploidy_not_checked == [SG_B, SG_C]
    assert any('2 SGs' in message and SG_B in message and SG_C in message for message in warnings_logged)


def test_check_cohort_metrics_no_reported_sex_sg_still_gets_other_checks(monkeypatch, make_qc_checker):
    stats = make_stats((SG_A, SG_B))
    stats['DRAGEN'][SG_B]['Mapped reads pct'] = FAILING_MAPPING_RATE

    results = run_checks(monkeypatch, make_qc_checker, stats, sex_by_sg={SG_A: MALE})

    assert results.bad_lines_by_sample == {SG_B: ['Mapping Rate (%)=70.0000 < 80.0000']}
    assert results.ploidy_not_checked == [SG_B]


def test_check_cohort_metrics_controls_without_sex_are_not_listed(monkeypatch, make_qc_checker):
    results = run_checks(
        monkeypatch, make_qc_checker, make_stats((SG_A, SG_B)), sex_by_sg={SG_A: MALE}, control_sg_ids={SG_B}
    )

    assert results.bad_lines_by_sample == {}
    assert results.ploidy_not_checked == []


def test_check_cohort_metrics_reported_sex_is_ignored_when_ploidy_is_not_thresholded(monkeypatch, make_qc_checker):
    # Exome defaults set no ploidy threshold; reported sex is then unused, so it cannot fail or be listed.
    thresholds = {'genome': {'min': {'mapping_rate_pct': MAPPING_RATE_THRESHOLD}}}
    discordant_line = 'Reported sex discordant (participant_portal_reported_sex=0, reportedSex=1)'

    results = run_checks(
        monkeypatch,
        make_qc_checker,
        make_stats((SG_A, SG_B, SG_C)),
        sg_ids=(SG_A, SG_B, SG_C),
        sex_by_sg={SG_A: MALE},
        discordant_by_sg={SG_B: discordant_line},
        thresholds=thresholds,
    )

    assert results.bad_lines_by_sample == {}
    assert results.ploidy_not_checked == []


def test_check_cohort_metrics_metric_in_two_sections_raises(monkeypatch, make_qc_checker):
    stats = make_stats((SG_A, SG_B))
    stats['DRAGEN_3'][SG_A]['Mapped reads pct'] = PASSING_MAPPING_RATE

    with pytest.raises(ValueError, match=r"'Mapped reads pct' found in 2 sections"):
        run_checks(monkeypatch, make_qc_checker, stats)


@pytest.mark.parametrize(
    ('section', 'metric'),
    [
        ('DRAGEN_2', 'Ploidy estimation'),
        ('DRAGEN', 'Supplementary (chimeric) alignments'),
        ('DRAGEN', 'Total alignments'),
        ('DRAGEN', 'Mapped reads pct'),
    ],
)
def test_check_cohort_metrics_metric_absent_from_whole_report_raises(monkeypatch, make_qc_checker, section, metric):
    # Wrong report or profile, not an SG lacking a value: calculators' inputs count too.
    stats = make_stats((SG_A, SG_B))
    for val_by_metric in stats[section].values():
        del val_by_metric[metric]

    with pytest.raises(RuntimeError, match=metric.replace('(', r'\(').replace(')', r'\)')):
        run_checks(monkeypatch, make_qc_checker, stats)


def test_check_cohort_metrics_does_not_mutate_report(monkeypatch, make_qc_checker):
    stats = make_stats((SG_A, SG_B))
    del stats['DRAGEN'][SG_B]['Mapped reads pct']
    before = copy.deepcopy(stats)

    run_checks(monkeypatch, make_qc_checker, stats)

    assert stats == before


# --- reported sex ---------------------------------------------------------

PORTAL = 'participant_portal_reported_sex'


@pytest.mark.parametrize(
    ('portal_sex', 'reported_sex', 'expected_sex', 'expected_discordant'),
    [
        (1, None, 1, False),
        (2, None, 2, False),
        (None, 1, 1, False),
        (None, 2, 2, False),
        (1, 1, 1, False),
        (2, 2, 2, False),
        (0, 1, None, True),
        (0, 2, None, True),
        (1, 2, None, True),
        (2, 1, None, True),
        (1, 0, None, True),
        (0, None, None, False),
        (None, 0, None, False),
        (0, 0, None, False),
        (None, None, None, False),
    ],
)
def test_resolve_reported_sex(portal_sex, reported_sex, expected_sex, expected_discordant):
    sex, discordant_line = resolve_reported_sex(portal_sex, reported_sex)

    assert sex == expected_sex
    if expected_discordant:
        assert discordant_line == (
            f'Reported sex discordant ({PORTAL}={portal_sex}, reportedSex={reported_sex})'
        )
    else:
        assert discordant_line is None


def test_get_reported_sex_info_reads_both_fields_per_sg(monkeypatch, warnings_logged):
    def sg_record(sg_id: str, portal_sex, reported_sex, external_id: str = 'SAMPLE', meta: Any = 'portal') -> dict:
        participant_meta = {PORTAL: portal_sex} if meta == 'portal' else meta
        return {
            'id': sg_id,
            'sample': {
                'externalId': external_id,
                'participant': {'reportedSex': reported_sex, 'meta': participant_meta},
            },
        }

    response = {
        'cohorts': [
            {
                'sequencingGroups': [
                    sg_record('CPG1', 1, None),
                    sg_record('CPG2', None, 2),
                    sg_record('CPG3', 0, 1),  # discordant
                    sg_record('CPG4', 0, None),
                    sg_record('CPG5', None, None),
                    sg_record('CPG6', None, 1, meta=None),  # participant with no meta at all
                    sg_record('CPG7', None, None, external_id='NA12878-control'),
                ]
            }
        ]
    }
    monkeypatch.setattr(check_multiqc, 'query', lambda _query, variables: response)  # noqa: ARG005

    info = get_reported_sex_info(SimpleNamespace(id='COH0001'))  # pyright: ignore[reportArgumentType]

    assert info.sex_by_sg == {'CPG1': 1, 'CPG2': 2, 'CPG6': 1}
    assert info.discordant_by_sg == {
        'CPG3': f'Reported sex discordant ({PORTAL}=0, reportedSex=1)',
    }
    assert info.control_sg_ids == {'CPG7'}
    # The fallback to reportedSex is logged for SGs that use it.
    assert any('CPG2' in message and 'fallback' in message for message in warnings_logged)
    assert any('CPG6' in message and 'fallback' in message for message in warnings_logged)
    assert not any('CPG1:' in message for message in warnings_logged)


# --- Slack message --------------------------------------------------------


def make_slack_checker(control_sg_ids: set[str]) -> Any:
    """Stands in for the QCChecker attributes generate_slack_message reads."""
    cohort_sgs = [SimpleNamespace(id=sg_id) for sg_id in ('CPG1', 'CPG2', 'CPG3', 'CPG4', 'CPG5', 'CPG6', 'CPG7')]
    return SimpleNamespace(cohort=SimpleNamespace(id='COH0001'), cohort_sgs=cohort_sgs, control_sg_ids=control_sg_ids)


def test_generate_slack_message_lists_ploidy_not_checked_without_flagging_them():
    text = generate_slack_message(
        {}, make_slack_checker(set()), 'https://example/report.html', ['CPG3', 'CPG4']
    )

    assert text.startswith('✅ ')
    assert 'flagged' not in text
    assert 'ℹ️ Ploidy not checked (no reported sex): 2 SGs: CPG3, CPG4' in text  # noqa: RUF001


def test_generate_slack_message_without_ploidy_not_checked_has_no_such_line():
    text = generate_slack_message(
        {'CPG1': ['Ploidy Estimation missing']}, make_slack_checker(set()), 'https://example/report.html', []
    )

    assert '❗ CPG1: Ploidy Estimation missing' in text
    assert 'Ploidy not checked' not in text


def test_generate_slack_message_keeps_control_failures_in_their_own_block():
    text = generate_slack_message(
        {'CPG1': ['Mapping Rate (%) missing'], 'CPG2': ['Mapping Rate (%) missing']},
        make_slack_checker({'CPG2'}),
        'https://example/report.html',
        ['CPG3'],
    )

    assert '1 samples are flagged' in text
    assert '❗ CPG1: Mapping Rate (%) missing' in text
    assert '— Controls —' in text
    assert 'ℹ️ CPG2: Mapping Rate (%) missing' in text  # noqa: RUF001
    assert 'CPG2' not in text.split('— Controls —')[0]
    # Controls are out of both sides of the failure rate: 1 real failure among 6 real SGs.
    assert '*Failure Rate:* 1 out of 6 samples' in text
