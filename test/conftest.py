"""Pytest bootstrap.

`single_sample_qc_popgen.constants` calls cpg_utils.config.config_retrieve()
at import time, which requires CPG_CONFIG_PATH. Point at the production
config so modules that pull in constants (transitively via utils.py) can be
imported in the test environment.
"""
import os
from pathlib import Path

os.environ.setdefault(
    'CPG_CONFIG_PATH',
    str(Path(__file__).parent.parent / 'config' / 'single_sample_qc_popgen.toml'),
)
