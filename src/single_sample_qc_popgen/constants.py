from typing import Final

import cpg_utils
from cpg_utils.config import config_retrieve, output_path

_reads_type = config_retrieve(['workflow', 'reads_type'], default=None)
assert _reads_type in {'fastq', 'cram'}, (
    f'Unsupported reads type: {_reads_type}. Valid options are fastq or cram. Please set this in the configuration using [workflow][reads_type].'  # noqa: E501
)
READS_TYPE: Final = _reads_type.lower()
BUCKET: Final = cpg_utils.to_path(output_path(suffix=''))
BUCKET_NAME: Final = str(BUCKET).removeprefix('gs://').removesuffix('/')
DRAGEN_VERSION: Final = config_retrieve(['ica', 'pipelines', 'dragen_version'])
