from typing import Final

from cpg_utils.config import config_retrieve

FAILURE_RATE_THRESHOLD: Final = 0.05  # 5% failure rate threshold for QC metrics
DRAGEN_VERSION: Final = config_retrieve(['ica', 'pipelines', 'dragen_version'])
# OurDNA controls carry this prefix in their sample external ID (e.g. NA12878-...).
OURDNA_CONTROL: Final = 'NA12878'
