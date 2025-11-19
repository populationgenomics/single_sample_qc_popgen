from typing import Final

from cpg_utils.config import config_retrieve

DRAGEN_VERSION: Final = config_retrieve(['ica', 'pipelines', 'dragen_version'])
