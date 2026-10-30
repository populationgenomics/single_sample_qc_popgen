"""Unit tests for the swap-check pure logic.

Covers the queue-time mapping classifier and path helpers in
``metamist_utils`` and the pairs.tsv parsing / classification / Slack
formatting in ``jobs.classify_swaps``, and the ``swap_check.enabled`` switch
in ``stages``. All inputs are synthetic; no metamist round-trip is involved.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from single_sample_qc_popgen import stages
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
    psam.write_text('#FID\tIID\tSEX\n0\tSYN1\t1\n0\tSYN2\t2\n')
    assert read_psam_array_sgs(psam) == {'SYN1', 'SYN2'}


def test_read_psam_array_sgs_skips_blank_and_comment_lines(tmp_path):
    psam = tmp_path / 'cohort.psam'
    psam.write_text('#IID\tSEX\nSYN1\t1\n\nSYN2\t2\n')
    assert read_psam_array_sgs(psam) == {'SYN1', 'SYN2'}


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
    msg = build_swap_detected_slack_message('COH1', 'ourdna-test', swap_check)
    assert msg is not None
    assert 'ourdna-test' in msg
    assert 'COH1' in msg
    assert 'WGS_SWAP' in msg
    assert 'ARR_EXP' in msg
    assert 'ARR_BEST' in msg
    assert 'WGS_OK' not in msg  # concordant SGs are not listed


# --- resolve_array_pgen_paths (dev override gating) ------------------------

DEV_OVERRIDE = {
    'pgen_path': 'gs://bucket/dir/test_subset.pgen',
    'pvar_path': 'gs://bucket/dir/test_subset.pvar',
    'psam_path': 'gs://bucket/dir/test_subset_swapped.psam',
}


def test_resolve_array_pgen_paths_override_allowed_in_test(monkeypatch):
    monkeypatch.setattr(stages, 'config_retrieve', lambda _keys, _default=None: DEV_OVERRIDE)
    monkeypatch.setattr(stages, 'get_access_level', lambda: 'test')
    assert stages.resolve_array_pgen_paths() == (
        DEV_OVERRIDE['pgen_path'],
        DEV_OVERRIDE['pvar_path'],
        DEV_OVERRIDE['psam_path'],
    )


def test_resolve_array_pgen_paths_override_blocked_in_main(monkeypatch):
    monkeypatch.setattr(stages, 'config_retrieve', lambda _keys, _default=None: DEV_OVERRIDE)
    monkeypatch.setattr(stages, 'get_access_level', lambda: 'main')
    with pytest.raises(RuntimeError, match='only be set under test access'):
        stages.resolve_array_pgen_paths()


def test_resolve_array_pgen_paths_falls_back_to_metamist(monkeypatch):
    def fake_config(keys, default=None) -> str | None:
        if keys == ['workflow', 'swap_check', 'rolling_aggregate_cohort_id']:
            return 'COH_ARRAY'
        return default

    queried: list[str] = []

    def fake_query(cohort_id) -> str:
        queried.append(cohort_id)
        return 'gs://bucket/dir/cohort.pgen'

    monkeypatch.setattr(stages, 'config_retrieve', fake_config)
    monkeypatch.setattr(stages, 'get_access_level', lambda: 'main')
    monkeypatch.setattr(stages, 'query_array_pgen_path', fake_query)
    assert stages.resolve_array_pgen_paths() == (
        'gs://bucket/dir/cohort.pgen',
        'gs://bucket/dir/cohort.pvar',
        'gs://bucket/dir/cohort.psam',
    )
    assert queried == ['COH_ARRAY']


def test_resolve_array_pgen_paths_requires_rolling_aggregate_cohort_id(monkeypatch):
    monkeypatch.setattr(stages, 'config_retrieve', lambda _keys, default=None: default)
    monkeypatch.setattr(stages, 'get_access_level', lambda: 'main')
    with pytest.raises(RuntimeError, match='rolling_aggregate_cohort_id'):
        stages.resolve_array_pgen_paths()


# --- swap_check.enabled switch ---------------------------------------------

SWAP_CHECK_STAGES = {
    'PrepareSampleSwap',
    'SwapCheckExportVcf',
    'SwapCheckSomalierExtract',
    'SwapCheckSomalierRelate',
    'SwapCheckClassify',
}


@pytest.fixture
def stage_init_without_workflow(monkeypatch):
    """Stage.__init__ reads get_workflow().status_reporter; unit tests have no workflow."""
    monkeypatch.setattr('cpg_flow.stage.get_workflow', lambda: SimpleNamespace(status_reporter=None))


def reachable_stage_names(required_stages) -> set[str]:
    """Names of the given stages and every stage they transitively require."""
    names: set[str] = set()
    todo = list(required_stages)
    while todo:
        stage_decorator = todo.pop()
        if stage_decorator.__name__ in names:
            continue
        names.add(stage_decorator.__name__)
        todo.extend(stage_decorator().required_stages_classes)
    return names


@pytest.mark.usefixtures('stage_init_without_workflow')
def test_enabled_register_stage_requires_swap_check_classify():
    # The test config (conftest) has the swap check enabled; the disabled branch is
    # covered by test_disabled_config_drops_swap_check_from_the_decorated_stage.
    assert stages.SWAP_CHECK_ENABLED is True
    assert stages.RegisterQcMetricsToMetamist().required_stages_classes == [
        stages.RunMultiQc,
        stages.CheckMultiQc,
        stages.SwapCheckClassify,
    ]


@pytest.mark.usefixtures('stage_init_without_workflow')
def test_enabled_register_stage_reaches_every_swap_check_stage():
    reachable = reachable_stage_names(stages.RegisterQcMetricsToMetamist().required_stages_classes)
    assert reachable >= SWAP_CHECK_STAGES


DEFAULT_CONFIG = Path(__file__).parent.parent / 'config' / 'single_sample_qc_popgen.toml'

PRINT_REGISTER_REQUIRED_STAGES = """
from types import SimpleNamespace
from unittest import mock

with mock.patch('cpg_flow.stage.get_workflow', lambda: SimpleNamespace(status_reporter=None)):
    from single_sample_qc_popgen import stages

    print([s.__name__ for s in stages.RegisterQcMetricsToMetamist().required_stages_classes])
"""


def import_stages_with_config(tmp_path: Path, config_text: str) -> subprocess.CompletedProcess[str]:
    """Imports stages in a fresh interpreter, since required_stages is fixed at import time."""
    config = tmp_path / 'config.toml'
    config.write_text(config_text)
    return subprocess.run(
        [sys.executable, '-c', PRINT_REGISTER_REQUIRED_STAGES],  # noqa: S603
        env=os.environ | {'CPG_CONFIG_PATH': str(config)},
        capture_output=True,
        text=True,
        check=False,
    )


def test_disabled_config_drops_swap_check_from_the_decorated_stage(tmp_path):
    default = DEFAULT_CONFIG.read_text()
    assert 'enabled = true' in default
    result = import_stages_with_config(tmp_path, default.replace('enabled = true', 'enabled = false'))

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "['RunMultiQc', 'CheckMultiQc']"


def test_missing_enabled_key_fails_on_import(tmp_path):
    default = DEFAULT_CONFIG.read_text()
    result = import_stages_with_config(tmp_path, default.replace('enabled = true\n', ''))

    assert result.returncode != 0
    assert 'workflow.swap_check.enabled is not set; set it to true' in result.stderr


def test_quoted_enabled_value_fails_on_import(tmp_path):
    # 'false' as a string is truthy and would silently enable the swap check.
    default = DEFAULT_CONFIG.read_text()
    result = import_stages_with_config(tmp_path, default.replace('enabled = true', "enabled = 'false'"))

    assert result.returncode != 0
    assert "workflow.swap_check.enabled must be a TOML boolean (true or false, unquoted); got 'false'" in result.stderr


class FakeInputs:
    def __init__(self) -> None:
        self.as_str_stages: list[str] = []

    def as_str(self, target, stage, key=None) -> str:  # noqa: ARG002
        self.as_str_stages.append(stage.__name__)
        return f'gs://bucket/{stage.__name__}/{key}'


def queue_register_stage(monkeypatch, swap_check_enabled: bool) -> tuple[FakeInputs, dict[str, Any]]:
    """Runs RegisterQcMetricsToMetamist.queue_jobs against fakes.

    Returns the fake inputs and the kwargs the stage passed to register_qc_metamist.run.
    """
    calls: list[dict[str, Any]] = []

    class FakePythonJob:
        def call(self, _func, **kwargs: Any) -> None:
            calls.append(kwargs)

    monkeypatch.setattr(stages, 'SWAP_CHECK_ENABLED', swap_check_enabled)
    monkeypatch.setattr(stages, 'initialise_python_job', lambda **_kwargs: FakePythonJob())
    stage_class = stages.RegisterQcMetricsToMetamist.__wrapped__  # pyright: ignore[reportFunctionMemberAccess]
    stage = stage_class.__new__(stage_class)  # skips Stage.__init__, which needs a workflow
    monkeypatch.setattr(stage, 'expected_outputs', lambda cohort: 'gs://bucket/registered.json')  # noqa: ARG005
    monkeypatch.setattr(stage, 'make_outputs', lambda **kwargs: kwargs)

    inputs = FakeInputs()
    stage.queue_jobs(SimpleNamespace(id='COH0001'), inputs)
    assert len(calls) == 1
    return inputs, calls[0]


def test_register_stage_passes_swap_check_path_when_enabled(monkeypatch):
    inputs, kwargs = queue_register_stage(monkeypatch, swap_check_enabled=True)

    assert kwargs['swap_check_path'] == 'gs://bucket/SwapCheckClassify/None'
    assert 'SwapCheckClassify' in inputs.as_str_stages


def test_register_stage_skips_swap_check_input_when_disabled(monkeypatch):
    inputs, kwargs = queue_register_stage(monkeypatch, swap_check_enabled=False)

    assert kwargs['swap_check_path'] is None
    assert 'SwapCheckClassify' not in inputs.as_str_stages


# --- stage output versions ----------------------------------------------------


def fake_version_config(values: dict[tuple[str, ...], str]) -> Any:
    def fake(keys: list[str], default: str | None = None) -> str | None:
        return values.get(tuple(keys), default)

    return fake


def test_qc_stages_default_to_v3_and_swap_check_stages_to_v1(monkeypatch):
    # v1 exome MultiQC JSONs lack target_bed_* metrics; v1 CheckMultiQc failures predate
    # per-sequencing-type thresholds; v1 registrations predate the swap check.
    monkeypatch.setattr(stages, 'config_retrieve', fake_version_config({}))

    assert stages.resolve_stage_version('RunMultiQc') == 'v3'
    assert stages.resolve_stage_version('CheckMultiQc') == 'v3'
    assert stages.resolve_stage_version('RegisterQcMetricsToMetamist') == 'v3'
    assert stages.resolve_stage_version('SwapCheckClassify') == 'v1'


def test_workflow_version_overrides_stage_defaults(monkeypatch):
    monkeypatch.setattr(stages, 'config_retrieve', fake_version_config({('workflow', 'version'): 'v7'}))

    assert stages.resolve_stage_version('RegisterQcMetricsToMetamist') == 'v7'
    assert stages.resolve_stage_version('RunMultiQc') == 'v7'


def test_per_stage_output_version_overrides_everything(monkeypatch):
    monkeypatch.setattr(
        stages,
        'config_retrieve',
        fake_version_config({
            ('workflow', 'version'): 'v7',
            ('workflow', 'output_versions', 'RegisterQcMetricsToMetamist'): 'v1',
        }),
    )

    assert stages.resolve_stage_version('RegisterQcMetricsToMetamist') == 'v1'
    assert stages.resolve_stage_version('RunMultiQc') == 'v7'
