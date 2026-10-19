"""Shared pytest fixtures for integration tests, which run against real services in Docker containers"""

import os
from contextlib import closing

import docker
import pytest
from docker.errors import DockerException

DOCKER_PING_TIMEOUT = 60
NOT_CI_VALUES = frozenset({"", "0", "false"})


@pytest.fixture(scope="session", autouse=True)
def _require_docker() -> None:
    """Skip the integration tests when Docker is unreachable, or fail them on CI where Docker must be available"""
    try:
        with closing(docker.from_env(timeout=DOCKER_PING_TIMEOUT)) as client:
            client.ping()
    except (DockerException, OSError) as e:
        reason = f"Docker is unreachable: {e}"
        if _is_ci():
            pytest.fail(f"{reason}. The integration tests must run on CI", pytrace=False)
        pytest.skip(reason)


def _is_ci() -> bool:
    return os.environ.get("CI", "").lower() not in NOT_CI_VALUES
