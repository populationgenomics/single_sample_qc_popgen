from dataclasses import dataclass
from typing import Final

from cpg_utils.config import config_retrieve

FAILURE_RATE_THRESHOLD: Final = 0.05  # 5% failure rate threshold for QC metrics
DRAGEN_VERSION: Final = config_retrieve(['ica', 'pipelines', 'dragen_version'])
# OurDNA controls carry this prefix in their sample external ID (e.g. NA12878-...).
OURDNA_CONTROL: Final = 'NA12878'

# DRAGEN per-SG CSV outputs that MultiQC parses for every sequencing type,
# staged by RunMultiQc as f'{sg_name}.{suffix}'. See the README and
# docs/dragen-output-schema.md for what is excluded and why.
MULTIQC_BASE_INPUT_SUFFIXES: Final = (
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
)


@dataclass(frozen=True)
class CoverageMetric:
    """A coverage metric whose MultiQC general-stats name depends on the sequencing type.

    Attributes:
        meta_key: Key under sg.meta['qc'] in metamist and in qc_thresholds.
        multiqc_name: Metric name in the MultiQC general-stats sections.
        display_name: Label used in QC log lines and the Slack report.
    """

    meta_key: str
    multiqc_name: str
    display_name: str


@dataclass(frozen=True)
class SequencingTypeProfile:
    """Everything that differs between sequencing types in MultiQC input and metric names."""

    multiqc_input_suffixes: tuple[str, ...]
    mean_coverage: CoverageMetric
    median_coverage: CoverageMetric
    pct_gt_20x: CoverageMetric

    @property
    def coverage_metrics(self) -> tuple[CoverageMetric, CoverageMetric, CoverageMetric]:
        return (self.mean_coverage, self.median_coverage, self.pct_gt_20x)


# Exome DRAGEN outputs add target-bed coverage files alongside the wgs_* and
# qc-coverage-region-* files in the base list; the exome coverage metrics
# below come from the target_bed_* files.
_EXOME_EXTRA_MULTIQC_INPUT_SUFFIXES: Final = (
    'target_bed_coverage_metrics.csv',
    'target_bed_overall_mean_cov.csv',
    'target_bed_contig_mean_cov.csv',
    'target_bed_fine_hist.csv',
)

# The exome meta key is pct_target_gt_20x, not pct_genome_gt_20x: the
# denominator is the target region, not the genome.
SEQUENCING_TYPE_PROFILES: Final = {
    'genome': SequencingTypeProfile(
        multiqc_input_suffixes=MULTIQC_BASE_INPUT_SUFFIXES,
        mean_coverage=CoverageMetric('mean_coverage', 'Average sequenced coverage over genome', 'Mean Coverage'),
        median_coverage=CoverageMetric(
            'median_coverage', 'wgs median autosomal coverage over genome', 'Median Coverage'
        ),
        pct_gt_20x=CoverageMetric(
            'pct_genome_gt_20x', 'wgs pct of genome with coverage [20x:inf)', 'Pct Genome @ >20x'
        ),
    ),
    'exome': SequencingTypeProfile(
        multiqc_input_suffixes=MULTIQC_BASE_INPUT_SUFFIXES + _EXOME_EXTRA_MULTIQC_INPUT_SUFFIXES,
        mean_coverage=CoverageMetric(
            'mean_coverage', 'Average sequenced coverage over target region', 'Mean Coverage'
        ),
        median_coverage=CoverageMetric(
            'median_coverage', 'target bed median autosomal coverage over target region', 'Median Coverage'
        ),
        pct_gt_20x=CoverageMetric(
            'pct_target_gt_20x', 'target bed pct of target region with coverage [20x:inf)', 'Pct Target @ >20x'
        ),
    ),
}


def get_sequencing_type_profile(sequencing_type: str) -> SequencingTypeProfile:
    """Returns the profile for a workflow.sequencing_type.

    Raises:
        ValueError: If sequencing_type is not a supported type. There is no
            fallback to genome: exome data QC'd with genome metric names yields
            wrong values rather than an error.
    """
    try:
        return SEQUENCING_TYPE_PROFILES[sequencing_type]
    except KeyError:
        raise ValueError(
            f'Unsupported workflow.sequencing_type {sequencing_type!r}; supported: {sorted(SEQUENCING_TYPE_PROFILES)}'
        ) from None
