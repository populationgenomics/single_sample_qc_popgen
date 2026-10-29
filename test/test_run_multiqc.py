"""Tests for the explicit MultiQC input-path construction and staging."""

from __future__ import annotations

import dataclasses
import subprocess
from types import SimpleNamespace
from typing import TYPE_CHECKING

import hailtop.batch as hb
import pytest
from cpg_utils import to_path

from single_sample_qc_popgen.constants import get_sequencing_type_profile
from single_sample_qc_popgen.jobs import run_multiqc

if TYPE_CHECKING:
    from pathlib import Path

# The MultiQC input files staged before they were split into tiers: 14 for
# genome and the same 14 plus four target_bed_* files for exome. The union of
# each profile's tiers must equal these exactly.
GENOME_INPUT_SUFFIXES = {
    'mapping_metrics.csv',
    'vc_metrics.csv',
    'ploidy_estimation_metrics.csv',
    'fastqc_metrics.csv',
    'fragment_length_hist.csv',
    'trimmer_metrics.csv',
    'wgs_coverage_metrics.csv',
    'wgs_contig_mean_cov.csv',
    'wgs_fine_hist.csv',
    'wgs_overall_mean_cov.csv',
    'qc-coverage-region-1_coverage_metrics.csv',
    'qc-coverage-region-1_overall_mean_cov.csv',
    'qc-coverage-region-2_coverage_metrics.csv',
    'qc-coverage-region-2_overall_mean_cov.csv',
}
TARGET_BED_SUFFIXES = {
    'target_bed_coverage_metrics.csv',
    'target_bed_overall_mean_cov.csv',
    'target_bed_contig_mean_cov.csv',
    'target_bed_fine_hist.csv',
}
EXOME_INPUT_SUFFIXES = GENOME_INPUT_SUFFIXES | TARGET_BED_SUFFIXES
GENOME_INPUT_COUNT = 14
EXOME_INPUT_COUNT = 18
PREVIOUS_INPUT_SUFFIXES = {'genome': GENOME_INPUT_SUFFIXES, 'exome': EXOME_INPUT_SUFFIXES}

# Files that feed a QC threshold, a registered metric or a calculator.
REQUIRED_SUFFIXES = {
    'genome': {
        'mapping_metrics.csv',
        'vc_metrics.csv',
        'ploidy_estimation_metrics.csv',
        'fastqc_metrics.csv',
        'wgs_coverage_metrics.csv',
    },
    'exome': {
        'mapping_metrics.csv',
        'vc_metrics.csv',
        'ploidy_estimation_metrics.csv',
        'fastqc_metrics.csv',
        'target_bed_coverage_metrics.csv',
    },
}


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

    assert len(paths.required) == 2 * len(REQUIRED_SUFFIXES['genome'])
    assert len(paths.report_only) == 2 * (len(GENOME_INPUT_SUFFIXES) - len(REQUIRED_SUFFIXES['genome']))
    assert (
        to_path('gs://bucket/ica/dragen_3_7_8/output/dragen_metrics/CPG000001/CPG000001.mapping_metrics.csv')
        in paths.required
    )
    assert (
        to_path('gs://bucket/ica/dragen_3_7_8/output/dragen_metrics/CPG000002/CPG000002.vc_metrics.csv')
        in paths.required
    )
    assert (
        to_path('gs://bucket/ica/dragen_3_7_8/output/dragen_metrics/CPG000002/CPG000002.trimmer_metrics.csv')
        in paths.report_only
    )


@pytest.mark.usefixtures('fake_dragen_prefix')
def test_dragen_qc_paths_all_basenames_unique():
    paths = run_multiqc.dragen_qc_paths(['CPG000001', 'CPG000002', 'CPG000003'], 'genome')
    basenames = [p.name for p in paths.required + paths.report_only]
    assert len(set(basenames)) == len(basenames)


def test_dragen_qc_paths_excludes_time_metrics_and_unparsed_files():
    for sequencing_type in ('genome', 'exome'):
        profile = get_sequencing_type_profile(sequencing_type)
        suffixes = profile.required_input_suffixes + profile.report_only_input_suffixes
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


@pytest.mark.usefixtures('fake_dragen_prefix')
def test_dragen_qc_paths_exome_stages_target_bed_files_genome_does_not():
    exome_paths = run_multiqc.dragen_qc_paths(['CPG000001', 'CPG000002'], 'exome')
    genome_paths = run_multiqc.dragen_qc_paths(['CPG000001', 'CPG000002'], 'genome')

    exome_all = exome_paths.required + exome_paths.report_only
    genome_all = genome_paths.required + genome_paths.report_only

    assert len(exome_all) == 2 * len(EXOME_INPUT_SUFFIXES)
    assert (
        to_path('gs://bucket/ica/dragen_3_7_8/output/dragen_metrics/CPG000002/CPG000002.target_bed_fine_hist.csv')
        in exome_paths.report_only
    )
    assert (
        to_path(
            'gs://bucket/ica/dragen_3_7_8/output/dragen_metrics/CPG000002/CPG000002.target_bed_coverage_metrics.csv'
        )
        in exome_paths.required
    )
    assert len({p.name for p in exome_all}) == len(exome_all)
    assert not any('target_bed' in p.name for p in genome_all)


@pytest.mark.usefixtures('fake_dragen_prefix')
def test_dragen_qc_paths_unknown_sequencing_type_raises():
    with pytest.raises(ValueError, match="Unsupported workflow.sequencing_type 'transcriptome'"):
        run_multiqc.dragen_qc_paths(['CPG000001'], 'transcriptome')


@pytest.mark.usefixtures('fake_dragen_prefix')
def test_dragen_qc_paths_duplicate_basename_across_tiers_raises(monkeypatch):
    profile = get_sequencing_type_profile('genome')
    overlapping = dataclasses.replace(
        profile, report_only_input_suffixes=(*profile.report_only_input_suffixes, 'mapping_metrics.csv')
    )
    monkeypatch.setattr(run_multiqc, 'get_sequencing_type_profile', lambda _sequencing_type: overlapping)

    with pytest.raises(ValueError, match='Duplicate basenames'):
        run_multiqc.dragen_qc_paths(['CPG000001'], 'genome')


# --- Tiers in SEQUENCING_TYPE_PROFILES ------------------------------------


@pytest.mark.parametrize('sequencing_type', ['genome', 'exome'])
def test_profile_tiers_union_equals_previous_input_list(sequencing_type):
    profile = get_sequencing_type_profile(sequencing_type)
    all_suffixes = profile.required_input_suffixes + profile.report_only_input_suffixes

    assert set(all_suffixes) == PREVIOUS_INPUT_SUFFIXES[sequencing_type]
    # No suffix appears twice, in either tier or across them.
    assert len(all_suffixes) == len(set(all_suffixes))
    assert set(profile.required_input_suffixes).isdisjoint(profile.report_only_input_suffixes)


def test_profile_input_counts():
    genome = get_sequencing_type_profile('genome')
    exome = get_sequencing_type_profile('exome')

    assert len(genome.required_input_suffixes + genome.report_only_input_suffixes) == GENOME_INPUT_COUNT
    assert len(exome.required_input_suffixes + exome.report_only_input_suffixes) == EXOME_INPUT_COUNT


@pytest.mark.parametrize('sequencing_type', ['genome', 'exome'])
def test_profile_required_tier_contents(sequencing_type):
    profile = get_sequencing_type_profile(sequencing_type)

    assert set(profile.required_input_suffixes) == REQUIRED_SUFFIXES[sequencing_type]


def test_exome_wgs_coverage_metrics_is_report_only():
    exome = get_sequencing_type_profile('exome')

    assert 'wgs_coverage_metrics.csv' in exome.report_only_input_suffixes
    assert 'target_bed_coverage_metrics.csv' not in exome.report_only_input_suffixes


# --- Staging command ------------------------------------------------------

# Stands in for gcloud with a local directory as the bucket ($FAKE_BUCKET maps
# gs://X to $FAKE_BUCKET/X). `storage cp -I DEST` copies every listed object
# that exists, then exits 1 naming the ones that matched nothing, which is what
# gcloud does for missing objects whether or not it runs in parallel.
FAKE_GCLOUD = """#!/usr/bin/env bash
if [ "$1 $2" = "auth list" ]; then
    exit 0
fi
if [ "$1 $2 $3" != "storage cp -I" ] || [ "$#" -ne 4 ]; then
    echo "fake gcloud: unexpected arguments: $*" >&2
    exit 2
fi
dest=$4
unmatched=""
while IFS= read -r url || [ -n "$url" ]; do
    source_file="$FAKE_BUCKET/${url#gs://}"
    if [ -f "$source_file" ]; then
        cp "$source_file" "$dest/"
    else
        unmatched="$unmatched$url"$'\n'
    fi
done
if [ -n "$unmatched" ]; then
    printf 'ERROR: (gcloud.storage.cp) The following URLs matched no objects or files:\n%s' "$unmatched" >&2
    exit 1
fi
"""

REQUIRED_URLS = ['gs://dragen/CPG1.mapping_metrics.csv', 'gs://dragen/CPG2.mapping_metrics.csv']
REPORT_ONLY_URLS = [
    'gs://dragen/CPG1.trimmer_metrics.csv',
    'gs://dragen/CPG2.trimmer_metrics.csv',
    'gs://dragen/CPG3.trimmer_metrics.csv',
]


def run_staging(tmp_path: Path, absent: set[str]) -> tuple[subprocess.CompletedProcess[str], Path]:
    """Runs stage_inputs_command under the job's `set -euo pipefail` against a fake bucket.

    Every URL in REQUIRED_URLS and REPORT_ONLY_URLS exists in the fake bucket except those in absent.
    """
    fake_bin = tmp_path / 'bin'
    fake_bin.mkdir()
    gcloud = fake_bin / 'gcloud'
    gcloud.write_text(FAKE_GCLOUD)
    gcloud.chmod(0o755)

    bucket = tmp_path / 'bucket'
    (bucket / 'dragen').mkdir(parents=True)
    for url in REQUIRED_URLS + REPORT_ONLY_URLS:
        if url not in absent:
            (bucket / url.removeprefix('gs://')).write_text('data')

    # Like the job, the lists have no trailing newline.
    required_list = tmp_path / 'required.txt'
    required_list.write_text('\n'.join(REQUIRED_URLS))
    report_only_list = tmp_path / 'report_only.txt'
    report_only_list.write_text('\n'.join(REPORT_ONLY_URLS))
    staged = tmp_path / 'staged'
    staged.mkdir()

    script = 'set -euo pipefail\n' + run_multiqc.stage_inputs_command(
        str(required_list), str(report_only_list), str(staged)
    )
    env = {'PATH': f'{fake_bin}:/usr/bin:/bin', 'FAKE_BUCKET': str(bucket)}
    result = subprocess.run(['bash', '-c', script], env=env, capture_output=True, text=True, check=False)  # noqa: S603, S607
    return result, staged


def test_stage_inputs_all_present(tmp_path):
    result, staged = run_staging(tmp_path, absent=set())

    assert result.returncode == 0, result.stderr
    assert len(list(staged.iterdir())) == len(REQUIRED_URLS) + len(REPORT_ONLY_URLS)
    assert 'Report-only inputs: 0 of 3 missing and skipped' in result.stdout
    assert 'WARNING' not in result.stderr


def test_stage_inputs_missing_report_only_is_logged_and_skipped(tmp_path):
    # CPG3 is the last line of the list, which has no trailing newline.
    absent = {'gs://dragen/CPG2.trimmer_metrics.csv', 'gs://dragen/CPG3.trimmer_metrics.csv'}
    result, staged = run_staging(tmp_path, absent=absent)

    assert result.returncode == 0, result.stderr
    assert sorted(p.name for p in staged.iterdir()) == [
        'CPG1.mapping_metrics.csv',
        'CPG1.trimmer_metrics.csv',
        'CPG2.mapping_metrics.csv',
    ]
    assert 'report-only input missing, skipped: CPG2.trimmer_metrics.csv' in result.stderr
    assert 'report-only input missing, skipped: CPG3.trimmer_metrics.csv' in result.stderr
    assert 'skipped: CPG1' not in result.stderr
    assert 'Report-only inputs: 2 of 3 missing and skipped' in result.stdout


def test_stage_inputs_missing_required_fails(tmp_path):
    result, _staged = run_staging(tmp_path, absent={'gs://dragen/CPG2.mapping_metrics.csv'})

    assert result.returncode != 0
    assert 'gs://dragen/CPG2.mapping_metrics.csv' in result.stderr
    assert 'Report-only inputs' not in result.stdout


def test_stage_inputs_missing_required_and_report_only_fails(tmp_path):
    absent = {'gs://dragen/CPG1.mapping_metrics.csv', 'gs://dragen/CPG1.trimmer_metrics.csv'}
    result, _staged = run_staging(tmp_path, absent=absent)

    assert result.returncode != 0


def test_run_multiqc_job_stages_required_strictly_and_report_only_leniently(monkeypatch, tmp_path):
    """Builds the job on a local (never run) Hail batch and checks the command it carries."""
    backend = hb.LocalBackend(tmp_dir=str(tmp_path / 'hail'))
    batch = hb.Batch(backend=backend)
    list_dir = tmp_path / 'lists'
    list_dir.mkdir()
    monkeypatch.setattr(run_multiqc, 'get_batch', lambda: batch)
    monkeypatch.setattr(run_multiqc, 'get_qc_path', lambda filename, category=None: to_path(str(list_dir / filename)))  # noqa: ARG005
    monkeypatch.setattr(run_multiqc, 'image_path', lambda name, version: f'image/{name}:{version}')
    monkeypatch.setattr(run_multiqc, 'get_cohort_sequencing_type', lambda _cohort: 'genome')
    monkeypatch.setattr(
        run_multiqc, 'get_dragen_output_path', lambda filename: to_path(f'gs://bucket/dragen/{filename}')
    )
    cohort = SimpleNamespace(
        name='cohort',
        id='COH1',
        get_sequencing_groups=lambda: [SimpleNamespace(name='CPG1'), SimpleNamespace(name='CPG2')],
        get_job_attrs=dict,
    )
    outputs = {
        'multiqc_report_html': to_path(str(tmp_path / 'r.html')),
        'multiqc_json': to_path(str(tmp_path / 'r.json')),
    }

    job = run_multiqc.run_multiqc(cohort, outputs)  # pyright: ignore[reportArgumentType]
    backend.close()

    command = job._command[0]  # noqa: SLF001
    lines = [line.strip() for line in command.splitlines()]
    required_copies = [line for line in lines if line.startswith('cat ') and 'required_input.txt' in line]
    report_only_copies = [line for line in lines if line.startswith('cat ') and 'report_only_input.txt' in line]
    assert len(required_copies) == 1
    assert len(report_only_copies) == 1
    assert 'gcloud storage cp -I' in required_copies[0]
    assert 'gcloud storage cp -I' in report_only_copies[0]
    assert not required_copies[0].endswith('|| true')
    assert report_only_copies[0].endswith('|| true')
    assert lines.index('set -euo pipefail') < lines.index(required_copies[0]) < lines.index(report_only_copies[0])
    assert '--continue-on-error' not in command

    # Each list file holds exactly that tier's paths.
    expected = run_multiqc.dragen_qc_paths(['CPG1', 'CPG2'], 'genome')
    required_listed = (list_dir / 'cohort_multiqc_required_input.txt').read_text().splitlines()
    report_only_listed = (list_dir / 'cohort_multiqc_report_only_input.txt').read_text().splitlines()
    assert required_listed == [str(p) for p in expected.required]
    assert report_only_listed == [str(p) for p in expected.report_only]
