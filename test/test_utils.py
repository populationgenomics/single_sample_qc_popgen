"""Tests for shared job helpers."""

from types import SimpleNamespace

import pytest

from single_sample_qc_popgen import utils
from single_sample_qc_popgen.utils import get_cohort_sequencing_type


def make_cohort(*sg_types: str) -> SimpleNamespace:
    sgs = [SimpleNamespace(id=f'CPG00000{i}', sequencing_type=t) for i, t in enumerate(sg_types, start=1)]
    return SimpleNamespace(get_sequencing_groups=lambda: sgs)


@pytest.fixture
def configured_type(monkeypatch):
    def set_type(sequencing_type: str) -> None:
        monkeypatch.setattr(utils, 'config_retrieve', lambda _keys: sequencing_type)

    return set_type


def test_get_cohort_sequencing_type_returns_matching_type(configured_type):
    configured_type('exome')
    assert get_cohort_sequencing_type(make_cohort('exome', 'exome')) == 'exome'  # pyright: ignore[reportArgumentType]


def test_get_cohort_sequencing_type_raises_on_mismatch(configured_type):
    configured_type('genome')
    with pytest.raises(ValueError, match=r"'genome' but these SGs differ: \['CPG000001 \(exome\)'\]"):
        get_cohort_sequencing_type(make_cohort('exome'))  # pyright: ignore[reportArgumentType]


def test_get_cohort_sequencing_type_lists_only_mismatched_sgs_in_mixed_cohort(configured_type):
    configured_type('genome')
    with pytest.raises(ValueError, match=r"\['CPG000002 \(exome\)'\]$"):
        get_cohort_sequencing_type(make_cohort('genome', 'exome'))  # pyright: ignore[reportArgumentType]
