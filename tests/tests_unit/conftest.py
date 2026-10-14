"""Shared pytest fixtures for unit tests"""

from pathlib import Path
from typing import Any

import pytest

TEST_LOGGER_NAME = "tests.tests_unit.test_logging"


@pytest.fixture
def temp_file(tmp_path: Path) -> Path:
    """Create a temporary file for testing"""
    file_path = tmp_path / "test_file.txt"
    file_path.write_text("test content")
    return file_path


@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for testing"""
    return tmp_path


@pytest.fixture
def logging_config() -> dict[str, Any]:
    """Create a logging config dict"""
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
            }
        },
        "loggers": {
            TEST_LOGGER_NAME: {"level": "DEBUG", "handlers": ["console"]},
        },
    }


@pytest.fixture
def delta_config() -> dict[str, Any]:
    """Create a delta logging config dict"""
    return {"loggers": {TEST_LOGGER_NAME: {"level": "INFO"}}}
