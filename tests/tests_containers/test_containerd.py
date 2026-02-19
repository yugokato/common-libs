"""Tests for common_libs.containers.containerd module"""

from unittest.mock import MagicMock

import pytest
from pytest_mock import MockFixture

from common_libs.containers.containerd import Containerd
from common_libs.exceptions import CommandError, NotFound


class TestContainerd:
    """Tests for Containerd class"""

    def test_init_default_values(self) -> None:
        """Test that default values are set correctly"""
        ctnd = Containerd()
        assert ctnd.containerd_sock == "/run/containerd/containerd.sock"
        assert ctnd.namespace == "k8s.io"

    def test_init_custom_values(self) -> None:
        """Test that custom values are set correctly"""
        sock = "/custom/path.sock"
        namespace = "custom.ns"
        ctnd = Containerd(containerd_sock=sock, namespace=namespace)
        assert ctnd.containerd_sock == sock
        assert ctnd.namespace == namespace

    def test_get_containers_success(self, mock_grpc_stub: MagicMock, mocker: MockFixture) -> None:
        """Test successful container listing"""
        mock_container = mocker.MagicMock()
        mock_container.id = "container-123"
        mock_grpc_stub.ListContainers.return_value.containers = [mock_container]

        ctnd = Containerd()
        result = ctnd.get_containers(name="my-container")

        assert result == [mock_container]
        mock_grpc_stub.ListContainers.assert_called_once()

    def test_get_containers_not_found(self, mock_grpc_stub: MagicMock) -> None:
        """Test NotFound raised when no containers match"""
        mock_grpc_stub.ListContainers.return_value.containers = []

        ctnd = Containerd()
        with pytest.raises(NotFound):
            ctnd.get_containers(name="nonexistent")

    def test_get_containers_multiple_raises(self, mock_grpc_stub: MagicMock, mocker: MockFixture) -> None:
        """Test RuntimeError raised when multiple containers with same name found"""
        mock_container1 = mocker.MagicMock()
        mock_container2 = mocker.MagicMock()
        mock_grpc_stub.ListContainers.return_value.containers = [mock_container1, mock_container2]

        ctnd = Containerd()
        with pytest.raises(RuntimeError, match="More then one containers"):
            ctnd.get_containers(name="my-container")

    def test_exec_run_success(self, mock_grpc_stub: MagicMock, mocker: MockFixture) -> None:
        """Test successful command execution"""
        mock_response = mocker.MagicMock()
        mock_response.exit_code = 0
        mock_response.stdout = b"hello\n"
        mock_response.stderr = b""
        mock_grpc_stub.ExecSync.return_value = mock_response

        ctnd = Containerd()
        exit_code, output = ctnd.exec_run("container-abc", "echo hello")

        assert exit_code == 0
        assert "hello" in output

    def test_exec_run_non_zero_raises_command_error(self, mock_grpc_stub: MagicMock, mocker: MockFixture) -> None:
        """Test that non-zero exit code raises CommandError when raise_on_error=True"""
        mock_response = mocker.MagicMock()
        mock_response.exit_code = 1
        mock_response.stdout = b""
        mock_response.stderr = b"error occurred\n"
        mock_grpc_stub.ExecSync.return_value = mock_response

        ctnd = Containerd()
        with pytest.raises(CommandError) as exc_info:
            ctnd.exec_run("container-abc", "false")

        assert exc_info.value.exit_code == 1

    def test_exec_run_non_zero_no_raise(self, mock_grpc_stub: MagicMock, mocker: MockFixture) -> None:
        """Test that non-zero exit code is returned when raise_on_error=False"""
        mock_response = mocker.MagicMock()
        mock_response.exit_code = 2
        mock_response.stdout = b""
        mock_response.stderr = b"some error\n"
        mock_grpc_stub.ExecSync.return_value = mock_response

        ctnd = Containerd()
        exit_code, _ = ctnd.exec_run("container-abc", "false", raise_on_error=False)
        assert exit_code == 2

    def test_get_containers_without_name_filter(self, mock_grpc_stub: MagicMock, mocker: MockFixture) -> None:
        """Test listing containers without name filter still returns containers"""
        mock_container = mocker.MagicMock()
        mock_grpc_stub.ListContainers.return_value.containers = [mock_container]

        ctnd = Containerd()
        result = ctnd.get_containers(name=None)

        assert result == [mock_container]
        mock_grpc_stub.ListContainers.assert_called_once()
