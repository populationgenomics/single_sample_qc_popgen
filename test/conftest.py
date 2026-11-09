"""Pytest bootstrap.

`single_sample_qc_popgen.constants` calls cpg_utils.config.config_retrieve()
at import time, which requires CPG_CONFIG_PATH. Point at the production
config so modules that pull in constants (transitively via utils.py) can be
imported in the test environment.
"""
import os
from pathlib import Path

import pytest
from loguru import logger

os.environ.setdefault(
    'CPG_CONFIG_PATH',
    str(Path(__file__).parent.parent / 'config' / 'single_sample_qc_popgen.toml'),
)


@pytest.fixture
def warnings_logged():
    """Collects the loguru WARNING (and above) messages emitted during a test."""
    messages: list[str] = []
    handler_id = logger.add(lambda message: messages.append(message.record['message']), level='WARNING')
    yield messages
    logger.remove(handler_id)
