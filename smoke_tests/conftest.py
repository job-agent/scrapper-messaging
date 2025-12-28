"""Pytest configuration and fixtures for smoke tests.

Smoke tests verify basic package health:
- Package imports
- Python syntax
- Type checking
"""

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).parent.parent
SRC_DIR = PROJECT_ROOT / "src"
PACKAGE_DIR = SRC_DIR / "scrapper_messaging"


@pytest.fixture
def project_root() -> Path:
    """Return the project root directory path."""
    return PROJECT_ROOT


@pytest.fixture
def src_dir() -> Path:
    """Return the src directory path."""
    return SRC_DIR


@pytest.fixture
def package_dir() -> Path:
    """Return the scrapper_messaging package directory path."""
    return PACKAGE_DIR
