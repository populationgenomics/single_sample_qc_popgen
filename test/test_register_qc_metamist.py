"""Tests for MultiQC general-stats section resolution and metric extraction."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from single_sample_qc_popgen.jobs.register_qc_metamist import (
    METRIC_MAP,
    build_sg_multiqc_meta_dict,
    resolve_metric_sections,
)


CONTAMINATION = 0.001
AVG_GC_CONTENT = 41.0
TI_TV_RATIO = 2.0


def make_general_stats(sg_ids: list[str]) -> dict:
    """Section layout mirroring a production multiqc_data.json
    report_general_stats_data (note the non-contiguous DRAGEN_N suffixes)."""
    return {
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


def test_resolve_metric_sections_maps_every_metric():
    general_stats = make_general_stats(['CPG000001'])
    section_by_metric = resolve_metric_sections(general_stats, [m for _, m in METRIC_MAP])

    assert section_by_metric['Estimated sample contamination'] == 'DRAGEN'
    assert section_by_metric['Ti/Tv ratio'] == 'DRAGEN_3'
    assert section_by_metric['Ploidy estimation'] == 'DRAGEN_4'
    assert section_by_metric['wgs pct of genome with coverage [20x:inf)'] == 'DRAGEN_5'
    assert section_by_metric['avg_gc_content_percent'] == 'dragen-fastqc'
    assert set(section_by_metric) == {m for _, m in METRIC_MAP}


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
    with pytest.raises(ValueError, match=r"'FREEMIX' found in 0 sections"):
        resolve_metric_sections(general_stats, ['FREEMIX'])


def test_resolve_metric_sections_ambiguous_metric_raises():
    general_stats = make_general_stats(['CPG000001'])
    general_stats['DRAGEN_3']['CPG000001']['Ploidy estimation'] = 'XX'
    with pytest.raises(ValueError, match=r"'Ploidy estimation' found in 2 sections"):
        resolve_metric_sections(general_stats, ['Ploidy estimation'])


def test_build_sg_multiqc_meta_dict_extracts_values_and_nones_missing_sgs():
    general_stats = make_general_stats(['CPG000001'])
    sgs = [SimpleNamespace(id='CPG000001'), SimpleNamespace(id='CPG000002')]

    extracted = build_sg_multiqc_meta_dict(sgs, general_stats)  # pyright: ignore[reportArgumentType]

    assert extracted['CPG000001']['contamination_dragen'] == CONTAMINATION
    assert extracted['CPG000001']['ploidy_estimation'] == 'XY'
    assert extracted['CPG000001']['avg_gc_content'] == AVG_GC_CONTENT
    # CPG000002 is absent from every section: every metric is None, not an error.
    assert set(extracted['CPG000002']) == {meta_key for meta_key, _ in METRIC_MAP}
    assert all(value is None for value in extracted['CPG000002'].values())


def test_build_sg_multiqc_meta_dict_nones_metric_missing_for_one_sg():
    # The metric still resolves to a section via CPG000002; it is only
    # CPG000001's row that lacks it, which yields None rather than an error.
    general_stats = make_general_stats(['CPG000001', 'CPG000002'])
    del general_stats['DRAGEN_3']['CPG000001']['Het/Hom ratio']
    sgs = [SimpleNamespace(id='CPG000001')]

    extracted = build_sg_multiqc_meta_dict(sgs, general_stats)  # pyright: ignore[reportArgumentType]

    assert extracted['CPG000001']['het_hom_ratio'] is None
    assert extracted['CPG000001']['ti_tv_ratio'] == TI_TV_RATIO
