from typing import Final

from cpg_utils.config import config_retrieve

FAILURE_RATE_THRESHOLD: Final = 0.05  # 5% failure rate threshold for QC metrics
DRAGEN_VERSION: Final = config_retrieve(['ica', 'pipelines', 'dragen_version'])
# OurDNA controls carry this prefix in their sample external ID (e.g. NA12878-...).
OURDNA_CONTROL: Final = 'NA12878'

# The DRAGEN 3.7.8 per-SG CSV outputs that MultiQC 1.30 parses, staged for the
# RunMultiQc stage as f'{sg_name}.{suffix}'. Everything else under
# dragen_metrics/<SG>/ is either unparsed by MultiQC or a nested duplicate that
# must not be staged (see docs/dragen-output-schema.md). Time metrics are
# deliberately absent: they are run-provenance, not sample QC, and the
# subsequent-vintage <SG>.dragen.time_metrics.csv injects a phantom
# '<SG>.dragen' sample into the report.
MULTIQC_INPUT_SUFFIXES: Final = (
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
