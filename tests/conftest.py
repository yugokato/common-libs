"""Shared pytest fixtures and configuration"""

from pathlib import Path

import pytest
import yaml


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
def logging_config_file(tmp_path: Path) -> Path:
    """Create a temporary logging config file"""
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
            }
        },
        "loggers": {
            "tests.test_logging": {"level": "DEBUG", "handlers": ["console"]},
        },
    }
    config_path = tmp_path / "logging.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)
    return config_path


@pytest.fixture
def delta_config_file(tmp_path: Path) -> Path:
    """Create a temporary delta logging config file"""
    config = {"loggers": {"tests.test_logging": {"level": "INFO"}}}
    config_path = tmp_path / "logging_delta.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)
    return config_path
