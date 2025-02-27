import os
import re
import shlex
import sys
import tarfile
from collections.abc import Callable
from functools import cached_property, wraps
from pathlib import Path
from typing import Self, cast

import docker
import docker.errors
from docker.models.containers import Container as DockerdContainer

from common_libs.ansi_colors import ColorCodes
from common_libs.exceptions import CommandError
from common_libs.files import create_tar_file
from common_libs.logging import get_logger
from common_libs.signals import register_exit_handler

from .containerd import Container as ContainerdContainer
from .containerd import Containerd

logger = get_logger(__name__)

APP_NAME = os.getenv("APP_NAME", default="default")


def requires_container(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        self: BaseContainer = args[0]
        if self.container is None:
            err = "Container object has not been set."
            if not self.is_containerd:
                err += f" Call <{type(self).__name__}>.run() first"
            raise RuntimeError(err)
        else:
            try:
                return f(*args, **kwargs)
            except docker.errors.NotFound as e:
                if (
                    e.status_code == 404
                    and "No such container" in str(e)
                    and self.enable_automatic_recovery_on_404
                    and not self.is_containerd
                ):
                    logger.warning(f"The container {self.container} no longer exists. Starting a new container...")
                    self.run()
                    return f(*args, **kwargs)
                else:
                    raise

    return wrapper


def requires_dockerd_runtime(f):
    """Requires dockerd container runtime. This restricts the usage in the containerd runtime"""

    @wraps(f)
    def wrapper(*args, **kwargs):
        self: BaseContainer = args[0]
        if self.is_containerd:
            raise NotImplementedError(f"<{type(self).__name__}>.{f.__name__}() is not supported for containerd runtime")
        return f(*args, **kwargs)

    return wrapper


class BaseContainer:
    """Base class for managing containers.

    The container runtime can be either dockerd (default) or containerd, but the supported fuctionality for
    containerd is currently limited to exec_run().
    When used in the containerd runtime, the existing container must be up and running, and the container name must
    be given as `name`.
    To support a separate container per environment, pass {'env': <env>} as `labels`.

    :param image: The image name
    :param tag: The image tag
    :param name: The container name
    :param labels: Custom labels (The comma separated base label will be automatically prepended to each key). Labels
                   will be used to find a reusable container in addition to image:tag
    :param timeout: Default timeout for API calls, in seconds
    :param is_containerd: The container is running in containerd runtime instead of dockerd
    :param enable_automatic_recovery_on_404: Automatically start a new container if a 404 Not Found is returned from
                                             the existing one. This option will be ignored if is_containerd is True
    """

    label_base = "com.example"  # Adjust the value for your use case

    def __init__(
        self,
        image: str,
        tag: str = "latest",
        name: str | None = None,
        labels: dict[str, str] | None = None,
        timeout: int = 60,
        is_containerd: bool = False,
        enable_automatic_recovery_on_404: bool = False,
    ):
        if is_containerd:
            if not name:
                raise ValueError("The existing container name is required when is_containerd=True")
            self.docker_client = None
        else:
            try:
                self.docker_client = docker.from_env(timeout=timeout)
                self.docker_client.ping()
            except docker.errors.DockerException:
                err = "ERROR: Unable to connect to the Docker daemon. Is the docker daemon running on this host?"
                logger.error(err)
                raise

        self.image = image
        self.tag = tag
        self.name = name
        self.labels = labels
        self.timeout = timeout
        self.is_containerd = is_containerd
        self.enable_automatic_recovery_on_404 = enable_automatic_recovery_on_404
        self.tmp_dir = "/tmp"

        self._container = None

    @property
    def container(self) -> DockerdContainer | ContainerdContainer | None:
        """Return container object"""
        return self._container

    @container.setter
    def container(self, container_obj: DockerdContainer | ContainerdContainer):
        """Set container object"""
        self._container = container_obj

    @cached_property
    def containerd(self) -> Containerd:
        """Accessor to the containerd runtime"""
        if not self.is_containerd:
            raise ValueError("Not supported when is_containerd=False")
        return Containerd()

    @requires_dockerd_runtime
    def run(
        self, detach: bool = True, remove: bool = True, stdin_open: bool = True, tty: bool = True, **kwargs
    ) -> Self:
        """Run container"""
        self._delete_existing_containers()
        logger.info(f"Starting a container {self.image}:{self.tag}", color_code=ColorCodes.YELLOW)
        labels = {f"{BaseContainer.label_base}.app": APP_NAME}
        if self.labels:
            labels.update({f"{BaseContainer.label_base}.{k}": v for k, v in self.labels.items() if v is not None})
        self.container = self.docker_client.containers.run(
            name=self.name,
            image=f"{self.image}:{self.tag}",
            detach=detach,
            remove=remove,
            stdin_open=stdin_open,
            tty=tty,
            labels=labels,
            **kwargs,
        )
        assert self.container
        logger.info(f"Started: {self.container.id}")
        register_exit_handler(self.delete)

        self.container.reload()

        return self

    @requires_dockerd_runtime
    def delete(self):
        """Delete container"""
        if self.container:
            logger.info(f"Deleting a container {self.container.id}", color_code=ColorCodes.YELLOW)
            try:
                self.container.remove(force=True)
            except docker.errors.NotFound:
                pass
            self.container = None

    @requires_container
    def exec_run(
        self,
        cmd: str,
        grep: str | None = None,
        grep_v: str | None = None,
        highlight: str | None = None,
        set_x: bool = False,
        pipes: list[str] | None = None,
        timeout: int | None = None,
        ignore_error: bool = False,
        suppress_output: bool = False,
        quiet: bool = False,
        output_parser: Callable | None = None,
        # raw parameters from <Container>.exec_run()
        stream: bool = False,
        detach: bool = False,
        **kwargs,
    ) -> tuple[int, str] | None:
        """Execute a command inside a container

        :param cmd: Command to execute
        :param grep: PATTERNS passed to "grep -E" command
        :param grep_v: PATTERNS passed to "grep -Ev" command
        :param highlight: Highlight the word(s) matched. For matching multiple words, use "|". This option is ignored
                          if `grep` is specified
        :param set_x: Add 'set -x;' to the command and echo the command executed
        :param pipes: Pipe each command in the list in the same order
        :param timeout: Wait up to the timeout seconds until the command returns
        :param ignore_error: Ignore non-zero code
        :param suppress_output: Suppress stdout output. stderr will be logged regardless of this option
        :param quiet: Suppress both command logging and stdout output. stderr will be logged regardless of this option
        :param output_parser: A custom parser function for parsing output. Pass a partial function if the function takes
                              arguments. For parsing streaming output, make sure to inspect chunk(s) and yield a
                              complete log line
        :param stream: Stream response data
        :param detach: If true, detach from the exec command
        :param kwargs: Any other parameters supported by <Container>.exec_run()
                       See: https://docker-py.readthedocs.io/en/stable/containers.html#docker.models.containers.Container.exec_run

        Notes on grep/grep_v:
            - To match multiple patterns as OR, use "|". eg. "a|b"
            - To match multiple patterns as AND (order matters), use ".*". eg. "a.*b"
            - Character escaping will be automatically handled within this function. Specify raw pattern(s)
        """
        if self.is_containerd and stream:
            raise ValueError("stream option is not supported for containerd")

        if set_x:
            cmd = 'date +"%Y-%m-%dT%H:%M:%S.%3N%z"; set -x; ' + cmd
        if grep:
            cmd += f' | GREP_COLOR="1;32" stdbuf -o0 grep -E "{self._escape_grep_pattern(grep)}" --color=always'
        elif highlight:
            cmd += f' | GREP_COLOR="1;32" stdbuf -o0 grep -E "{highlight}|$" --color=always'
        if grep_v:
            cmd += f' | stdbuf -o0 grep -Ev "{self._escape_grep_pattern(grep_v)}"'
        if pipes:
            pipe = " | "
            cmd += pipe + pipe.join(pipes)

        if self.is_containerd and detach:
            # "ExecRequest" seems to provide the similar behavior of "detach", but I could not figure out how to
            # actually connect to the streaming server using the URL (http://127.0.0.1:<port>/exec/<token>) returned as
            # ExecResponse. Looks like the command won't be actually executed without starting streaming.
            # https://aws.plainenglish.io/kubernetes-deep-dive-cri-container-runtime-interface-f1d005d5a458
            #
            # As a workaround, we use ExecSyncRequest with "&" and run the command as a background process.
            # https://stackoverflow.com/questions/49244935/running-background-process-with-kubectl-exec
            cmd = f"{cmd} > /dev/null 2> /dev/null &"

        _cmd = f"sh -c {shlex.quote(cmd)}"
        if timeout:
            _cmd = f"timeout {timeout} {_cmd}"

        if not quiet:
            logger.info(
                f"Executing command{' via containerd.sock' if self.is_containerd else ''}: {_cmd}",
                color_code=ColorCodes.YELLOW,
            )

        if self.is_containerd:
            exit_code, resp = self.containerd.exec_run(self.container.id, _cmd, raise_on_error=not ignore_error)
        else:
            exit_code, resp = self.container.exec_run(_cmd, detach=detach, stream=stream, **kwargs)

        if detach:
            return
        else:
            if stream:
                try:
                    if output_parser:
                        for line in output_parser(resp):
                            sys.stdout.write(line + "\n")
                            sys.stdout.flush()
                    else:
                        for chunk in resp:
                            if chunk.rstrip():
                                decoded_chunk = chunk.decode("utf-8")
                                for line in decoded_chunk.splitlines():
                                    sys.stdout.write(line + "\n")
                                    sys.stdout.flush()
                except KeyboardInterrupt:
                    print("Stopped")  # noqa: T201
            else:
                if isinstance(resp, bytes):
                    try:
                        resp = resp.decode("utf-8")
                    except UnicodeDecodeError:
                        pass
                if exit_code == 0:
                    if output_parser:
                        resp = output_parser(resp)
                    if not quiet:
                        if suppress_output:
                            logger.info("Suppressed output")
                        else:
                            logger.info(f"output:\n{resp}")
                else:
                    if ignore_error:
                        logger.error(resp)
                    else:
                        if exit_code == 1 and grep:
                            raise CommandError(
                                f'Nothing matched with the specified grep pattern: "{grep}"', exit_code=exit_code
                            )
                        elif exit_code == 124 and timeout:
                            raise CommandError(
                                f"The command did not complete within the specified timeout ({timeout} seconds)",
                                exit_code=exit_code,
                            )
                        else:
                            raise CommandError(
                                f"The command returned non-zero code ({exit_code}): {resp}", exit_code=exit_code
                            )
                return exit_code, cast(str, resp.rstrip())

    @requires_dockerd_runtime
    @requires_container
    def upload_file(self, source_file_path: Path | str, dest_dir_path: str | None = None):
        """Upload a file to the container as a tar archive

        :param source_file_path: Local file path
        :param dest_dir_path: A directory path inside the container. Defaults to /tmp
        """
        if not dest_dir_path:
            dest_dir_path = self.tmp_dir
        if not tarfile.is_tarfile(source_file_path):
            source_file_path = create_tar_file(source_file_path)
        with open(source_file_path, "rb") as f:
            uploaded = self.container.put_archive(dest_dir_path, f.read())
            if not uploaded:
                raise RuntimeError(f"Failed to upload {source_file_path} to the container's {dest_dir_path}")

    @requires_dockerd_runtime
    @requires_container
    def download_file(self, source_file_path: str, dest_dir_path: Path | str, extract_file: bool = True) -> Path:
        """Download a file from the container as a tar archive (gzip)

        :param source_file_path: A file path in the container to download from
        :param dest_dir_path: A local directory path to save to
        :param extract_file: Extract the file
        """
        logger.info(f"Downloading a file: {source_file_path}")
        data, stat = self.container.get_archive(source_file_path, encode_stream=True)
        assert data
        logger.info(f"File info: {stat}")
        file_name = stat["name"]
        dest_gz_file_path = Path(dest_dir_path, file_name + ".tar.gz")
        logger.info(f"Saving the data as {dest_gz_file_path}...")
        with open(dest_gz_file_path, "wb") as f:
            for chunk in data:
                f.write(chunk)

        downloaded_file_path = Path(dest_dir_path, file_name)
        if extract_file:
            logger.info("Extracting file...")
            with tarfile.open(dest_gz_file_path, mode="r") as f:
                f.extract(file_name, path=dest_dir_path)
            logger.info(f"Extracted: {downloaded_file_path}")
            Path(dest_gz_file_path).unlink()
        return downloaded_file_path

    def get_existing_containers(self) -> list[DockerdContainer | ContainerdContainer]:
        if self.is_containerd:
            return self.containerd.get_containers(name=self.name)
        else:
            label = [f"{BaseContainer.label_base}.app={APP_NAME}"]
            if self.labels:
                label.extend(f"{BaseContainer.label_base}.{k}={v}" for k, v in self.labels.items() if v is not None)
            filters = {"ancestor": f"{self.image}:{self.tag}", "label": label}
            if self.name:
                filters.update(name=self.name)
            return self.docker_client.containers.list(filters=filters)

    @requires_dockerd_runtime
    def _delete_existing_containers(self):
        if existing_containers := self.get_existing_containers():
            logger.info(f"Deleting an existing container {existing_containers}", color_code=ColorCodes.YELLOW)
            for c in existing_containers:
                try:
                    c.remove(force=True)
                except Exception as e:
                    logger.error(e)

    def _escape_grep_pattern(self, raw_pattern: str) -> str:
        """Escape grep pattern

        We escape the specified grep pattern as some characters affect the grep functionality.
        NOTE: Single quotes will be handled in shlex.quote()
        """
        sub_and = "___AND___"
        sub_or = "___OR___"
        p = re.sub(r"\.\*", sub_and, re.sub(r"\|", sub_or, raw_pattern))
        escaped_pattern = re.escape(p).replace(sub_or, "|").replace(sub_and, ".*").replace('"', '\\"')
        return escaped_pattern
