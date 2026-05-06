"""Pytest bootstrap.

`single_sample_qc_popgen.constants` calls cpg_utils.config.config_retrieve()
at import time, which requires CPG_CONFIG_PATH. Set a stub config before any
test module is imported so modules that pull in constants (transitively via
utils.py) can be imported in the test environment.
"""
import os
from pathlib import Path

os.environ.setdefault(
    'CPG_CONFIG_PATH',
    str(Path(__file__).parent / '_config_stub.toml'),
)
