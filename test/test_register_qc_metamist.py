"""Tests for MultiQC general-stats section resolution and metric extraction."""

from __future__ import annotations

import json
import math
from types import SimpleNamespace
from typing import Any

import pytest

from single_sample_qc_popgen import utils
from single_sample_qc_popgen.jobs import register_qc_metamist
from single_sample_qc_popgen.jobs.register_qc_metamist import build_sg_multiqc_meta_dict, metric_map
from single_sample_qc_popgen.utils import null_non_finite_floats, resolve_metric_sections


CONTAMINATION = 0.001
AVG_GC_CONTENT = 41.0
TI_TV_RATIO = 2.0
EXOME_GENOME_WIDE_COVERAGE = 2.6
EXOME_TARGET_MEAN_COVERAGE = 78.4
EXOME_TARGET_MEDIAN_COVERAGE = 77.0
EXOME_PCT_TARGET_GT_20X = 0.97
F_STAT_RAW = 0.9


def make_general_stats(sg_ids: list[str], sequencing_type: str = 'genome') -> dict[str, dict]:
    """Section layout mirroring a production multiqc_data.json
    report_general_stats_data (note the non-contiguous DRAGEN_N suffixes).

    For exomes the genome-named coverage metrics are also present with
    genome-wide values, as the wgs_* files are staged for exomes too; the
    target-region metrics carry the values that must be registered.
    """
    general_stats: dict[str, dict] = {
        'DRAGEN': {
            sg: {
                'Estimated sample contamination': CONTAMINATION,
                'Average sequenced coverage over genome': 35.2,
                'Q30 bases pct': 91.0,
                'Mapped reads pct': 99.5,
                'Number of duplicate marked reads pct': 12.3,
                'Insert length: mean': 400.1,
                'Insert length: standard deviation': 80.2,
                'Supplementary (chimeric) alignments': 1000,
                'Total alignments': 1_000_000,
            }
            for sg in sg_ids
        },
        'DRAGEN_3': {sg: {'Ti/Tv ratio': TI_TV_RATIO, 'Het/Hom ratio': 1.5} for sg in sg_ids},
        'DRAGEN_4': {
            sg: {
                'Ploidy estimation': 'XY',
                'X median / Autosomal median': 0.5,
                'Y median / Autosomal median': 0.5,
            }
            for sg in sg_ids
        },
        'DRAGEN_5': {
            sg: {
                'wgs median autosomal coverage over genome': 34.0,
                'wgs pct of genome with coverage [20x:inf)': 0.95,
            }
            for sg in sg_ids
        },
        'dragen-fastqc': {sg: {'avg_gc_content_percent': AVG_GC_CONTENT} for sg in sg_ids},
    }
    if sequencing_type == 'exome':
        for sg in sg_ids:
            general_stats['DRAGEN'][sg]['Average sequenced coverage over genome'] = EXOME_GENOME_WIDE_COVERAGE
            general_stats['DRAGEN'][sg]['Average sequenced coverage over target region'] = EXOME_TARGET_MEAN_COVERAGE
            general_stats['DRAGEN_5'][sg]['wgs median autosomal coverage over genome'] = EXOME_GENOME_WIDE_COVERAGE
            general_stats['DRAGEN_5'][sg]['wgs pct of genome with coverage [20x:inf)'] = 0.01
        general_stats['DRAGEN_6'] = {
            sg: {
                'target bed median autosomal coverage over target region': EXOME_TARGET_MEDIAN_COVERAGE,
                'target bed pct of target region with coverage [20x:inf)': EXOME_PCT_TARGET_GT_20X,
            }
            for sg in sg_ids
        }
    return general_stats


def test_resolve_metric_sections_maps_every_metric():
    general_stats = make_general_stats(['CPG000001'])
    section_by_metric = resolve_metric_sections(general_stats, [m for _, m in metric_map('genome')])

    assert section_by_metric['Estimated sample contamination'] == 'DRAGEN'
    assert section_by_metric['Ti/Tv ratio'] == 'DRAGEN_3'
    assert section_by_metric['Ploidy estimation'] == 'DRAGEN_4'
    assert section_by_metric['wgs pct of genome with coverage [20x:inf)'] == 'DRAGEN_5'
    assert section_by_metric['avg_gc_content_percent'] == 'dragen-fastqc'
    assert set(section_by_metric) == {m for _, m in metric_map('genome')}


def test_resolve_metric_sections_is_immune_to_section_renumbering():
    general_stats = make_general_stats(['CPG000001'])
    renumbered = {
        {'DRAGEN_3': 'DRAGEN_2', 'DRAGEN_4': 'DRAGEN_3', 'DRAGEN_5': 'DRAGEN_4'}.get(key, key): section
        for key, section in general_stats.items()
    }
    section_by_metric = resolve_metric_sections(renumbered, ['Ti/Tv ratio', 'Ploidy estimation'])
    assert section_by_metric == {'Ti/Tv ratio': 'DRAGEN_2', 'Ploidy estimation': 'DRAGEN_3'}


def test_resolve_metric_sections_missing_metric_raises():
    general_stats = make_general_stats(['CPG000001'])
    with pytest.raises(ValueError, match=r"'FREEMIX' is in no section"):
        resolve_metric_sections(general_stats, ['FREEMIX'])


def test_resolve_metric_sections_ambiguous_metric_raises():
    general_stats = make_general_stats(['CPG000001'])
    general_stats['DRAGEN_3']['CPG000001']['Ploidy estimation'] = 'XX'
    with pytest.raises(ValueError, match=r"'Ploidy estimation' found in 2 sections"):
        resolve_metric_sections(general_stats, ['Ploidy estimation'])


def test_build_sg_multiqc_meta_dict_extracts_values():
    general_stats = make_general_stats(['CPG000001'])
    sgs = [SimpleNamespace(id='CPG000001')]

    extracted = build_sg_multiqc_meta_dict(sgs, general_stats, 'genome')  # pyright: ignore[reportArgumentType]

    assert set(extracted['CPG000001']) == {meta_key for meta_key, _ in metric_map('genome')}
    assert extracted['CPG000001']['contamination_dragen'] == CONTAMINATION
    assert extracted['CPG000001']['ploidy_estimation'] == 'XY'
    assert extracted['CPG000001']['avg_gc_content'] == AVG_GC_CONTENT


def test_build_sg_multiqc_meta_dict_raises_for_sg_missing_from_report():
    # Sending null for every metric of an absent SG would delete its earlier QC in metamist.
    general_stats = make_general_stats(['CPG000001'])
    sgs = [SimpleNamespace(id='CPG000001'), SimpleNamespace(id='CPG000002')]

    with pytest.raises(ValueError, match=r"missing from the MultiQC report.*\['CPG000002'\]"):
        build_sg_multiqc_meta_dict(sgs, general_stats, 'genome')  # pyright: ignore[reportArgumentType]


def test_build_sg_multiqc_meta_dict_raises_for_sg_missing_from_one_section():
    general_stats = make_general_stats(['CPG000001', 'CPG000002'])
    del next(iter(general_stats.values()))['CPG000002']
    sgs = [SimpleNamespace(id='CPG000001'), SimpleNamespace(id='CPG000002')]

    with pytest.raises(ValueError, match=r"\['CPG000002'\]"):
        build_sg_multiqc_meta_dict(sgs, general_stats, 'genome')  # pyright: ignore[reportArgumentType]


def test_build_sg_multiqc_meta_dict_nones_metric_missing_for_one_sg():
    # The metric still resolves to a section via CPG000002; it is only
    # CPG000001's row that lacks it, which yields None rather than an error.
    general_stats = make_general_stats(['CPG000001', 'CPG000002'])
    del general_stats['DRAGEN_3']['CPG000001']['Het/Hom ratio']
    sgs = [SimpleNamespace(id='CPG000001')]

    extracted = build_sg_multiqc_meta_dict(sgs, general_stats, 'genome')  # pyright: ignore[reportArgumentType]

    assert extracted['CPG000001']['het_hom_ratio'] is None
    assert extracted['CPG000001']['ti_tv_ratio'] == TI_TV_RATIO


# --- sequencing-type-specific coverage metrics ----------------------------

GENOME_META_KEYS = [
    'contamination_dragen',
    'mean_coverage',
    'median_coverage',
    'pct_genome_gt_20x',
    'q30_bases_pct',
    'mapping_rate_pct',
    'pct_duplicate_reads',
    'mean_insert_size',
    'std_dev_insert_size',
    'avg_gc_content',
    'chimera_alignments',
    'total_alignments',
    'ploidy_estimation',
    'norm_x_coverage',
    'norm_y_coverage',
    'ti_tv_ratio',
    'het_hom_ratio',
]


def test_metric_map_genome_keys_and_coverage_names():
    assert [meta_key for meta_key, _ in metric_map('genome')] == GENOME_META_KEYS
    genome = dict(metric_map('genome'))
    assert genome['mean_coverage'] == 'Average sequenced coverage over genome'
    assert genome['median_coverage'] == 'wgs median autosomal coverage over genome'
    assert genome['pct_genome_gt_20x'] == 'wgs pct of genome with coverage [20x:inf)'


def test_metric_map_exome_coverage_names_and_key():
    exome = dict(metric_map('exome'))
    assert exome['mean_coverage'] == 'Average sequenced coverage over target region'
    assert exome['median_coverage'] == 'target bed median autosomal coverage over target region'
    assert exome['pct_target_gt_20x'] == 'target bed pct of target region with coverage [20x:inf)'
    assert 'pct_genome_gt_20x' not in exome


def test_metric_map_unknown_sequencing_type_raises():
    with pytest.raises(ValueError, match="Unsupported workflow.sequencing_type 'transcriptome'"):
        metric_map('transcriptome')


def test_resolve_metric_sections_exome_maps_target_region_metrics():
    general_stats = make_general_stats(['CPG000001'], 'exome')
    section_by_metric = resolve_metric_sections(general_stats, [m for _, m in metric_map('exome')])

    assert section_by_metric['Average sequenced coverage over target region'] == 'DRAGEN'
    assert section_by_metric['target bed median autosomal coverage over target region'] == 'DRAGEN_6'
    assert section_by_metric['target bed pct of target region with coverage [20x:inf)'] == 'DRAGEN_6'


def test_build_sg_multiqc_meta_dict_exome_registers_target_region_values():
    general_stats = make_general_stats(['CPG000001'], 'exome')
    sgs = [SimpleNamespace(id='CPG000001')]

    extracted = build_sg_multiqc_meta_dict(sgs, general_stats, 'exome')  # pyright: ignore[reportArgumentType]

    # Not the genome-wide values that were registered when exomes were QC'd in genome mode.
    assert extracted['CPG000001']['mean_coverage'] == EXOME_TARGET_MEAN_COVERAGE
    assert extracted['CPG000001']['median_coverage'] == EXOME_TARGET_MEDIAN_COVERAGE
    assert extracted['CPG000001']['pct_target_gt_20x'] == EXOME_PCT_TARGET_GT_20X
    assert 'pct_genome_gt_20x' not in extracted['CPG000001']


def test_build_sg_multiqc_meta_dict_exome_on_genome_report_raises():
    general_stats = make_general_stats(['CPG000001'], 'genome')
    sgs = [SimpleNamespace(id='CPG000001')]
    with pytest.raises(ValueError, match=r"'Average sequenced coverage over target region' is in no section"):
        build_sg_multiqc_meta_dict(sgs, general_stats, 'exome')  # pyright: ignore[reportArgumentType]


# --- swap_check registration ----------------------------------------------

SWAP_CHECK_PATH = 'gs://bucket/swap_check.json'
SWAP_CHECK = {'CPG000001': {'status': 'concordant', 'best_relatedness': 0.999}}


def run_register(
    monkeypatch,
    tmp_path,
    swap_check_path: str | None,
    sequencing_type: str = 'genome',
    drop_paths: frozenset[str] | set[str] = frozenset(),
    sex_imputation: dict | None = None,
    swap_check: dict | None = None,
) -> dict:
    """Runs register_qc_metamist.run against fakes; returns each SG's meta as sent to metamist.

    Paths in drop_paths behave as missing files. sex_imputation and swap_check replace
    the default JSON served for CPG000001.
    """
    json_by_path = {
        'gs://bucket/multiqc.json': make_general_stats(['CPG000001'], sequencing_type),
        'gs://bucket/failures.json': {},
        'gs://bucket/sex_imputation.json': sex_imputation or {'CPG000001': {'f_stat_raw': F_STAT_RAW}},
        SWAP_CHECK_PATH: swap_check or SWAP_CHECK,
    }
    for path in drop_paths:
        del json_by_path[path]
    loaded: list[str] = []
    sent_meta: dict[str, dict] = {}

    def fake_load_json(path, extract_key=None, allow_missing=False) -> Any:  # noqa: ARG001
        loaded.append(path)
        if path not in json_by_path:
            if allow_missing:
                return {}
            raise FileNotFoundError(path)
        return json_by_path[path]

    def fake_query(_mutation, variables) -> dict:
        sent_meta[variables['sequencingGroup']['id']] = variables['sequencingGroup']['meta']
        return {}

    def fake_config(keys, default=None) -> Any:  # noqa: ARG001
        return {('workflow', 'sequencing_type'): sequencing_type, ('workflow', 'multiqc'): {}}[tuple(keys)]

    monkeypatch.setattr(register_qc_metamist, 'load_json', fake_load_json)
    monkeypatch.setattr(register_qc_metamist, 'query', fake_query)
    monkeypatch.setattr(register_qc_metamist, 'config_retrieve', fake_config)
    monkeypatch.setattr(utils, 'config_retrieve', fake_config)
    monkeypatch.setattr(register_qc_metamist, 'get_access_level', lambda: 'test')

    cohort = SimpleNamespace(
        get_sequencing_groups=lambda: [SimpleNamespace(id='CPG000001', sequencing_type=sequencing_type)],
        dataset=SimpleNamespace(name='project'),
    )
    output = tmp_path / 'registered.json'
    register_qc_metamist.run(
        cohort=cohort,  # pyright: ignore[reportArgumentType]
        multiqc_data_path='gs://bucket/multiqc.json',
        failures_path='gs://bucket/failures.json',
        sex_imputation_path='gs://bucket/sex_imputation.json',
        swap_check_path=swap_check_path,
        output=output,  # pyright: ignore[reportArgumentType]
    )
    assert json.loads(output.read_text())['CPG000001']['contamination_dragen'] == CONTAMINATION
    assert (SWAP_CHECK_PATH in loaded) == (swap_check_path is not None)
    return sent_meta


def test_run_sends_null_swap_check_when_no_path_given(monkeypatch, tmp_path):
    sent_meta = run_register(monkeypatch, tmp_path, swap_check_path=None)

    qc = sent_meta['CPG000001']['qc']
    assert 'swap_check' in qc
    assert qc['swap_check'] is None
    assert 'swap_check' not in json.loads((tmp_path / 'registered.json').read_text())['CPG000001']
    assert qc['f_stat_raw'] == F_STAT_RAW
    assert qc['qc_checks_failed'] == []


def test_run_registers_swap_check_under_nested_key_when_path_given(monkeypatch, tmp_path):
    sent_meta = run_register(monkeypatch, tmp_path, swap_check_path=SWAP_CHECK_PATH)

    qc = sent_meta['CPG000001']['qc']
    assert qc['swap_check'] == SWAP_CHECK['CPG000001']
    assert qc['f_stat_raw'] == F_STAT_RAW


def test_run_raises_when_enabled_swap_check_file_is_missing(monkeypatch, tmp_path):
    with pytest.raises(FileNotFoundError, match='missing_swap_check.json'):
        run_register(monkeypatch, tmp_path, swap_check_path='gs://bucket/missing_swap_check.json')


@pytest.mark.parametrize('present_key', ['failures', 'sex_imputation'])
def test_run_raises_when_a_check_multiqc_output_is_missing(monkeypatch, tmp_path, present_key):
    missing_key = {'failures', 'sex_imputation'}.difference({present_key}).pop()
    with pytest.raises(FileNotFoundError, match=f'{missing_key}.json'):
        run_register(monkeypatch, tmp_path, swap_check_path=None, drop_paths={f'gs://bucket/{missing_key}.json'})


def test_run_registers_exome_coverage_under_exome_keys(monkeypatch, tmp_path):
    sent_meta = run_register(monkeypatch, tmp_path, swap_check_path=None, sequencing_type='exome')

    qc = sent_meta['CPG000001']['qc']
    assert qc['mean_coverage'] == EXOME_TARGET_MEAN_COVERAGE
    assert qc['pct_target_gt_20x'] == EXOME_PCT_TARGET_GT_20X


@pytest.mark.parametrize(
    ('sequencing_type', 'other_type_key'),
    [('exome', 'pct_genome_gt_20x'), ('genome', 'pct_target_gt_20x')],
)
def test_run_sends_null_for_other_type_coverage_keys(monkeypatch, tmp_path, sequencing_type, other_type_key):
    # Metamist merges meta with JSON_MERGE_PATCH; null is the only way to delete a stale key.
    sent_meta = run_register(monkeypatch, tmp_path, swap_check_path=SWAP_CHECK_PATH, sequencing_type=sequencing_type)

    qc = sent_meta['CPG000001']['qc']
    assert other_type_key in qc
    assert qc[other_type_key] is None
    assert all(value is not None for key, value in qc.items() if key != other_type_key)
    assert other_type_key not in json.loads((tmp_path / 'registered.json').read_text())['CPG000001']


# --- non-finite floats ----------------------------------------------------


def parse_json_without_non_finite(text: str) -> dict:
    """json.loads that fails on NaN and Infinity, which strict JSON parsers reject."""

    def fail(constant: str) -> None:
        raise AssertionError(f'non-finite constant {constant} in JSON')

    return json.loads(text, parse_constant=fail)


def test_null_non_finite_floats_replaces_nan_and_inf_at_any_depth(warnings_logged):
    value = {
        'nan': math.nan,
        'inf': math.inf,
        'neg_inf': -math.inf,
        'nested': {'deeper': {'nan': math.nan}},
        'in_list': [1.0, math.nan, {'inf': math.inf}],
    }

    cleaned = null_non_finite_floats(value, 'CPG000001', 'qc')

    assert cleaned == {
        'nan': None,
        'inf': None,
        'neg_inf': None,
        'nested': {'deeper': {'nan': None}},
        'in_list': [1.0, None, {'inf': None}],
    }
    assert len(warnings_logged) == 6  # noqa: PLR2004
    assert all('CPG000001' in message for message in warnings_logged)
    assert any('qc.nested.deeper.nan' in message for message in warnings_logged)
    assert any('qc.in_list[2].inf' in message for message in warnings_logged)
    # The input is not mutated.
    assert math.isnan(value['nan'])


def test_null_non_finite_floats_leaves_finite_values_untouched(warnings_logged):
    value = {'f': 0.9, 'zero': 0.0, 'neg': -1.5, 'int': 3, 'bool': False, 'str': 'nan', 'none': None, 'list': [1, 'a']}

    assert null_non_finite_floats(value, 'CPG000001', 'qc') == value
    assert warnings_logged == []


def test_run_sends_null_for_nan_sex_imputation_and_warns(monkeypatch, tmp_path, warnings_logged):
    # compute_f_stat returns NaN for both when somalier called no chrX sites.
    sex_imputation = {'CPG000001': {'f_stat_raw': math.nan, 'x_het_rate': math.nan, 'n_called_x': 0, 'y_calls': 3}}

    sent_meta = run_register(monkeypatch, tmp_path, swap_check_path=None, sex_imputation=sex_imputation)

    qc = sent_meta['CPG000001']['qc']
    assert qc['f_stat_raw'] is None
    assert qc['x_het_rate'] is None
    assert qc['n_called_x'] == 0
    assert qc['y_calls'] == 3  # noqa: PLR2004
    assert qc['contamination_dragen'] == CONTAMINATION
    assert any('CPG000001' in message and 'qc.f_stat_raw' in message for message in warnings_logged)
    assert any('CPG000001' in message and 'qc.x_het_rate' in message for message in warnings_logged)
    # Every value sent is strict-JSON serialisable, as the metamist client requires.
    json.dumps(sent_meta, allow_nan=False)


def test_run_sends_null_for_nested_non_finite_swap_check_value(monkeypatch, tmp_path, warnings_logged):
    swap_check = {'CPG000001': {'status': 'no_match', 'best_relatedness': math.nan, 'worst_relatedness': math.inf}}

    sent_meta = run_register(monkeypatch, tmp_path, swap_check_path=SWAP_CHECK_PATH, swap_check=swap_check)

    assert sent_meta['CPG000001']['qc']['swap_check'] == {
        'status': 'no_match',
        'best_relatedness': None,
        'worst_relatedness': None,
    }
    assert any('qc.swap_check.best_relatedness' in message for message in warnings_logged)
    assert any('qc.swap_check.worst_relatedness' in message for message in warnings_logged)
    json.dumps(sent_meta, allow_nan=False)


def test_run_registered_output_json_has_no_nan(monkeypatch, tmp_path):
    sex_imputation = {'CPG000001': {'f_stat_raw': math.nan, 'x_het_rate': math.nan}}
    swap_check = {'CPG000001': {'status': 'no_match', 'best_relatedness': math.nan}}

    run_register(
        monkeypatch, tmp_path, swap_check_path=SWAP_CHECK_PATH, sex_imputation=sex_imputation, swap_check=swap_check
    )

    registered = parse_json_without_non_finite((tmp_path / 'registered.json').read_text())['CPG000001']
    assert registered['f_stat_raw'] is None
    assert registered['x_het_rate'] is None
    assert registered['swap_check']['best_relatedness'] is None
    assert registered['contamination_dragen'] == CONTAMINATION


def test_run_leaves_finite_values_and_logs_no_warning(monkeypatch, tmp_path, warnings_logged):
    sent_meta = run_register(monkeypatch, tmp_path, swap_check_path=SWAP_CHECK_PATH)

    qc = sent_meta['CPG000001']['qc']
    assert qc['f_stat_raw'] == F_STAT_RAW
    assert qc['swap_check'] == SWAP_CHECK['CPG000001']
    assert not any('non-finite' in message for message in warnings_logged)
