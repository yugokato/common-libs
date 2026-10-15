"""Tests for common_libs.containers.container module"""

import subprocess
import sys
import tarfile
from pathlib import Path
from unittest.mock import MagicMock

import docker.errors
import pytest
from pytest_mock import MockFixture

from common_libs.containers.container import BaseContainer
from common_libs.exceptions import CommandError

pytestmark = [pytest.mark.unittest]

MISSING_CONTAINERD_ERROR = "optional containerd dependency"
IPV4_HOST_IP = "0.0.0.0"
IPV6_HOST_IP = "::"
IPV4_HOST_PORT = 49153
IPV6_HOST_PORT = 49154
DOWNLOADED_FILE = "root.crt"


def make_binding(host_ip: str, host_port: int) -> dict[str, str]:
    """Build a published port binding in the format Docker reports for a container"""
    return {"HostIp": host_ip, "HostPort": str(host_port)}


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

    def test_init_docker_exception_raises_runtime_error(self, mocker: MockFixture) -> None:
        """Test that a Docker connection error raises RuntimeError with a helpful message"""
        mock_docker = mocker.patch("common_libs.containers.container.docker.from_env")
        mock_docker.side_effect = docker.errors.DockerException("Cannot connect to Docker daemon")

        with pytest.raises(
            RuntimeError, match=r"Unable to connect to the Docker daemon\. Is the docker daemon running on this host?"
        ):
            BaseContainer(image="myimage")

    def test_container_property_initially_none(self) -> None:
        """Test that container property is None before run()"""
        ctn = BaseContainer(image="alpine")
        assert ctn.container is None


class TestMissingContainerdDependency:
    """Tests for BaseContainer when the optional containerd dependency is not installed"""

    @pytest.mark.usefixtures("mock_missing_containerd_dependency")
    def test_init_containerd_mode_raises(self) -> None:
        """Test that containerd mode raises a user-friendly RuntimeError at construction"""
        with pytest.raises(RuntimeError, match=MISSING_CONTAINERD_ERROR):
            BaseContainer(image="myimage", name="my-container", is_containerd=True)

    @pytest.mark.usefixtures("mock_missing_containerd_dependency")
    def test_init_dockerd_mode_succeeds(self) -> None:
        """Test that dockerd mode is not affected by the missing containerd dependency"""
        ctn = BaseContainer(image="myimage")
        assert ctn.is_containerd is False

    def test_import_succeeds(self) -> None:
        """Test that the container module can be imported when grpc and cri_api are not installed"""
        script = "\n".join(
            [
                "import sys",
                "sys.modules['grpc'] = None",
                "sys.modules['cri_api'] = None",
                "from common_libs.containers.container import BaseContainer",
                "BaseContainer(image='myimage', name='my-container', is_containerd=True)",
            ]
        )
        result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)

        # The friendly error (not an ImportError raised by the import itself) proves the module was imported
        assert result.returncode != 0
        assert MISSING_CONTAINERD_ERROR in result.stderr


class TestBaseContainerHostPort:
    """Tests for BaseContainer.host_port()"""

    @pytest.mark.parametrize(
        "bindings",
        [
            [make_binding(IPV4_HOST_IP, IPV4_HOST_PORT)],
            [make_binding(IPV4_HOST_IP, IPV4_HOST_PORT), make_binding(IPV6_HOST_IP, IPV6_HOST_PORT)],
            [make_binding(IPV6_HOST_IP, IPV6_HOST_PORT), make_binding(IPV4_HOST_IP, IPV4_HOST_PORT)],
        ],
        ids=["ipv4-only", "ipv4-first", "ipv6-first"],
    )
    def test_returns_port_of_the_ipv4_binding(self, mocker: MockFixture, bindings: list[dict[str, str]]) -> None:
        """Test that the host port of the IPv4 binding is returned in whichever order Docker lists the bindings"""
        ctn = BaseContainer(image="alpine", tag="latest")
        ctn.container = mocker.MagicMock(ports={"3025/tcp": bindings})

        assert ctn.host_port(3025) == IPV4_HOST_PORT

    def test_returns_port_published_for_the_requested_container_port(self, mocker: MockFixture) -> None:
        """Test that the binding of the requested container port is used when several ports are published"""
        published = {3025: 49153, 3143: 49154}
        ctn = BaseContainer(image="alpine", tag="latest")
        ctn.container = mocker.MagicMock(
            ports={f"{port}/tcp": [make_binding(IPV4_HOST_IP, host_port)] for port, host_port in published.items()}
        )

        assert ctn.host_port(3143) == published[3143]


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


class TestBaseContainerContextManager:
    """Tests for BaseContainer as a context manager"""

    @pytest.fixture
    def ctn(self) -> BaseContainer:
        """A dockerd BaseContainer instance"""
        return BaseContainer(image="alpine", tag="latest")

    @pytest.fixture
    def mock_container(self, mock_docker_from_env: MagicMock, mocker: MockFixture) -> MagicMock:
        """A mock container returned by the mocked docker client when a container is started"""
        mock_docker_from_env.containers.list.return_value = []
        container = mocker.MagicMock()
        container.id = "context-manager-container"
        mock_docker_from_env.containers.run.return_value = container
        return container

    def test_enter_runs_container_and_returns_self(self, ctn: BaseContainer, mock_container: MagicMock) -> None:
        """Test that entering the context starts a container and returns the same instance"""
        with ctn as entered:
            assert entered is ctn
            assert ctn.container is mock_container

    def test_exit_deletes_container(self, ctn: BaseContainer, mock_container: MagicMock) -> None:
        """Test that exiting the context removes the container and resets the container object"""
        with ctn:
            pass

        mock_container.remove.assert_called_once_with(force=True)
        assert ctn.container is None

    def test_exit_deletes_container_when_block_raises(self, ctn: BaseContainer, mock_container: MagicMock) -> None:
        """Test that the container is removed and the exception propagates when the block raises"""
        mock_container.exec_run.return_value = (1, b"command failed")

        with pytest.raises(CommandError, match="non-zero code"), ctn:
            ctn.exec_run("false")

        mock_container.remove.assert_called_once_with(force=True)
        assert ctn.container is None

    def test_enter_skips_run_when_container_already_running(
        self, mock_docker_from_env: MagicMock, ctn: BaseContainer, mock_container: MagicMock
    ) -> None:
        """Test that entering the context does not start another container if one was already started"""
        with ctn.run() as entered:
            assert entered.container is mock_container

        mock_docker_from_env.containers.run.assert_called_once()
        mock_container.remove.assert_called_once_with(force=True)

    def test_enter_raises_for_containerd_runtime(self) -> None:
        """Test that entering the context raises NotImplementedError for containerd runtime"""
        ctn = BaseContainer(image="alpine", name="my-container", is_containerd=True)

        with pytest.raises(NotImplementedError, match="not supported for containerd runtime"), ctn:
            pass


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

    @pytest.mark.parametrize(
        "grep_pattern",
        ["hello", "foo|bar", "foo.*bar", "foo[bar]"],
        ids=["simple", "OR", "AND", "special-chars"],
    )
    def test_exec_run_grep_pattern_types(self, mocker: MockFixture, grep_pattern: str) -> None:
        """Test exec_run succeeds with various grep pattern types including OR, AND, and special chars"""
        ctn = BaseContainer(image="alpine")
        mock_container = mocker.MagicMock()
        mock_container.exec_run.return_value = (0, b"matching output\n")
        ctn.container = mock_container

        result = ctn.exec_run("echo hello world", grep=grep_pattern)
        assert result is not None


class TestBaseContainerDownloadFile:
    """Tests for BaseContainer.download_file()"""

    @pytest.mark.parametrize(
        ("has_data_filter", "expected_kwargs"),
        [(True, {"filter": "data"}), (False, {})],
        ids=["with-data-filter", "without-data-filter"],
    )
    def test_passes_the_data_filter_only_when_tarfile_has_it(
        self,
        mocker: MockFixture,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        has_data_filter: bool,
        expected_kwargs: dict[str, str],
    ) -> None:
        """Test that extract() gets the data filter only when tarfile supports it (Python 3.11.4 and later)"""
        if has_data_filter:
            monkeypatch.setattr(tarfile, "data_filter", mocker.Mock(), raising=False)
        else:
            monkeypatch.delattr(tarfile, "data_filter", raising=False)
        container = mocker.MagicMock()
        container.get_archive.return_value = ([b"archive"], {"name": DOWNLOADED_FILE})
        ctn = BaseContainer(image="alpine", tag="latest")
        ctn.container = container
        tar = mocker.patch("common_libs.containers.container.tarfile.open").return_value.__enter__.return_value

        downloaded = ctn.download_file(f"/data/{DOWNLOADED_FILE}", tmp_path)

        tar.extract.assert_called_once_with(DOWNLOADED_FILE, path=tmp_path, **expected_kwargs)
        assert downloaded == tmp_path / DOWNLOADED_FILE
