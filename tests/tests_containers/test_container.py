"""Tests for common_libs.containers.container module"""

from unittest.mock import MagicMock

import docker.errors
import pytest
from pytest_mock import MockFixture

from common_libs.containers.container import BaseContainer
from common_libs.exceptions import CommandError


class TestRequiresContainerDecorator:
    """Tests for the requires_container decorator"""

    def test_raises_when_container_is_none(self) -> None:
        """Test that RuntimeError is raised when container is not set"""
        ctn = BaseContainer(image="alpine", tag="latest")
        # container is None since run() was not called
        with pytest.raises(RuntimeError, match="Container object has not been set"):
            ctn.exec_run("echo hello")

    def test_calls_function_when_container_set(self, mocker: MockFixture) -> None:
        """Test that function is called when container is set"""
        ctn = BaseContainer(image="alpine", tag="latest")

        mock_container = mocker.MagicMock()
        mock_container.exec_run.return_value = (0, b"hello\n")
        ctn.container = mock_container

        result = ctn.exec_run("echo hello")
        assert result is not None


class TestRequiresDockerdRuntimeDecorator:
    """Tests for the requires_dockerd_runtime decorator"""

    def test_raises_for_containerd_runtime(self, mocker: MockFixture) -> None:
        """Test that NotImplementedError is raised for containerd runtime for functionality that is dockerd-only"""
        ctn = BaseContainer(image="alpine", name="my-container", is_containerd=True)
        # Set a mock container so requires_container passes
        ctn.container = mocker.MagicMock()
        ctn.container.id = "abc123"

        with pytest.raises(NotImplementedError, match="not supported for containerd runtime"):
            ctn.run()

    def test_allows_dockerd_runtime(self, mock_docker_from_env: MagicMock, mocker: MockFixture) -> None:
        """Test that dockerd runtime can call dockerd-only methods"""
        mock_container = mocker.MagicMock()
        mock_container.id = "container-xyz"
        mock_docker_from_env.containers.run.return_value = mock_container
        mock_docker_from_env.containers.list.return_value = []

        container = BaseContainer(image="alpine", tag="latest")
        container.run()
        assert container.container is not None


class TestBaseContainerInit:
    """Tests for BaseContainer initialization"""

    def test_init_dockerd_mode(self) -> None:
        """Test initialization in dockerd mode"""
        image = "myimage"
        tag = "v1"
        ctn = BaseContainer(image=image, tag=tag)
        assert ctn.image == image
        assert ctn.tag == tag
        assert ctn.is_containerd is False
        assert ctn.docker_client is not None

    def test_init_containerd_mode_requires_name(self) -> None:
        """Test that containerd mode requires container name"""
        with pytest.raises(ValueError, match="existing container name is required"):
            BaseContainer(image="myimage", is_containerd=True)

    def test_init_containerd_mode(self) -> None:
        """Test initialization in containerd mode"""
        name = "my-container"
        ctn = BaseContainer(image="myimage", name=name, is_containerd=True)
        assert ctn.is_containerd is True
        assert ctn.docker_client is None
        assert ctn.name == name

    def test_init_docker_exception_reraises(self, mocker: MockFixture) -> None:
        """Test that Docker connection errors are re-raised"""
        mock_docker = mocker.patch("common_libs.containers.container.docker.from_env")
        mock_docker.side_effect = docker.errors.DockerException("Cannot connect to Docker daemon")

        with pytest.raises(docker.errors.DockerException):
            BaseContainer(image="myimage")

    def test_container_property_initially_none(self) -> None:
        """Test that container property is None before run()"""
        ctn = BaseContainer(image="alpine")
        assert ctn.container is None


class TestBaseContainerRun:
    """Tests for BaseContainer.run()"""

    def test_run_creates_and_stores_container(self, mock_docker_from_env: MagicMock, mocker: MockFixture) -> None:
        """Test that run() creates a container and stores it"""
        mock_docker_from_env.containers.list.return_value = []

        mock_container = mocker.MagicMock()
        mock_container.id = "new-container-123"
        mock_docker_from_env.containers.run.return_value = mock_container

        ctn = BaseContainer(image="alpine", tag="latest")
        result = ctn.run()

        assert ctn.container is mock_container
        assert result is ctn  # returns self

    def test_run_deletes_existing_containers(self, mock_docker_from_env: MagicMock, mocker: MockFixture) -> None:
        """Test that run() removes existing containers before starting new one"""
        existing_container = mocker.MagicMock()
        existing_container.remove = mocker.MagicMock()
        mock_docker_from_env.containers.list.return_value = [existing_container]

        new_docker_container = mocker.MagicMock()
        new_docker_container.id = "new-container-456"
        mock_docker_from_env.containers.run.return_value = new_docker_container

        ctn = BaseContainer(image="alpine", tag="latest")
        ctn.run()

        existing_container.remove.assert_called_once_with(force=True)


class TestBaseContainerDelete:
    """Tests for BaseContainer.delete()"""

    def test_delete_removes_container(self, mocker: MockFixture) -> None:
        """Test that delete() removes the container and sets it to None"""
        ctn = BaseContainer(image="alpine")
        mock_container = mocker.MagicMock()
        ctn.container = mock_container

        ctn.delete()

        mock_container.remove.assert_called_once_with(force=True)
        assert ctn.container is None

    def test_delete_skips_when_no_container(self) -> None:
        """Test that delete() is a no-op when container is None"""
        ctn = BaseContainer(image="alpine")
        # No container set, should not raise
        ctn.delete()


class TestBaseContainerExecRun:
    """Tests for BaseContainer.exec_run()"""

    def test_exec_run_success(self, mocker: MockFixture) -> None:
        """Test successful command execution"""
        ctn = BaseContainer(image="alpine")

        mock_container = mocker.MagicMock()
        mock_container.exec_run.return_value = (0, b"hello world\n")
        ctn.container = mock_container

        result = ctn.exec_run("echo hello world")
        assert result is not None
        exit_code, output = result
        assert exit_code == 0
        assert b"hello world" in output

    def test_exec_run_grep_error_raises_command_error(self, mocker: MockFixture) -> None:
        """Test that exit code 1 with grep raises CommandError with grep message"""
        ctn = BaseContainer(image="alpine")

        mock_container = mocker.MagicMock()
        mock_container.exec_run.return_value = (1, b"")
        ctn.container = mock_container

        with pytest.raises(CommandError, match="grep pattern"):
            ctn.exec_run("echo hello", grep="nonexistent_pattern")

    def test_exec_run_timeout_raises_command_error(self, mocker: MockFixture) -> None:
        """Test that exit code 124 with timeout raises CommandError with timeout message"""
        ctn = BaseContainer(image="alpine")

        mock_container = mocker.MagicMock()
        mock_container.exec_run.return_value = (124, b"")
        ctn.container = mock_container

        with pytest.raises(CommandError, match="timeout"):
            ctn.exec_run("sleep 100", timeout=1)

    def test_exec_run_generic_error_raises_command_error(self, mocker: MockFixture) -> None:
        """Test that non-zero exit raises CommandError"""
        ctn = BaseContainer(image="alpine")

        mock_container = mocker.MagicMock()
        mock_container.exec_run.return_value = (2, b"command not found")
        ctn.container = mock_container

        with pytest.raises(CommandError, match="non-zero code"):
            ctn.exec_run("nonexistent_cmd")

    def test_exec_run_ignore_error(self, mocker: MockFixture) -> None:
        """Test that ignore_error=True suppresses CommandError"""
        ctn = BaseContainer(image="alpine")

        mock_container = mocker.MagicMock()
        mock_container.exec_run.return_value = (1, b"some error")
        ctn.container = mock_container

        # Should not raise
        ctn.exec_run("false", ignore_error=True)


class TestEscapeGrepPattern:
    """Tests for BaseContainer._escape_grep_pattern()"""

    def test_basic_pattern_unchanged(self) -> None:
        """Test that simple patterns are escaped properly"""
        pattern = "hello"
        ctn = BaseContainer(image="alpine")
        result = ctn._escape_grep_pattern(pattern)
        assert result == pattern

    def test_or_pattern_preserved(self) -> None:
        """Test that | is preserved as OR separator"""
        ctn = BaseContainer(image="alpine")
        result = ctn._escape_grep_pattern("foo|bar")
        assert "|" in result

    def test_and_pattern_preserved(self) -> None:
        """Test that .* is preserved as AND separator"""
        ctn = BaseContainer(image="alpine")
        result = ctn._escape_grep_pattern("foo.*bar")
        assert ".*" in result

    def test_special_chars_escaped(self) -> None:
        """Test that special regex chars (except | and .*) are escaped"""
        ctn = BaseContainer(image="alpine")
        result = ctn._escape_grep_pattern("foo[bar]")
        # Square brackets should be escaped
        assert "\\[" in result
