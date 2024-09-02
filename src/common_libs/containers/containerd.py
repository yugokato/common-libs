import shlex

import grpc
from cri_api import (
    Container,
    ContainerFilter,
    ExecSyncRequest,
    ExecSyncResponse,
    ListContainersRequest,
    RuntimeServiceStub,
)

from common_libs.exceptions import CommandError, NotFound
from common_libs.logging import get_logger

logger = get_logger(__name__)


class Containerd:
    """A class for managing containers in the containerd runtime via containerd.sock

    References:
        https://pypi.org/project/container-runtime-interface-api/
        https://github.com/kubernetes/cri-api/blob/master/pkg/apis/runtime/v1alpha2/api.pb.go
        https://github.com/kubernetes/cri-api
    """

    def __init__(self, containerd_sock: str = "/run/containerd/containerd.sock", namespace: str = "k8s.io"):
        self.containerd_sock = containerd_sock
        self.namespace = namespace

    def get_containers(self, name: str = None) -> list[Container]:
        """Get containers in the containerd runtime by container name

        :param name: Container name to filter

        NOTE: Currently the supported container filtering options are limited to container ID, state, and labels.
              We only support the filtering by container name using labels
        """

        with grpc.insecure_channel(f"unix://{self.containerd_sock}") as channel:
            runtime_stub = RuntimeServiceStub(channel)
            filter = ContainerFilter(label_selector={"io.kubernetes.container.name": name}) if name else None
            request = ListContainersRequest(filter=filter)
            containers = runtime_stub.ListContainers(request).containers
            if not containers:
                raise NotFound(f"Could not find a container with name={name}")
            if len(containers) > 1:
                raise RuntimeError(f"More then one containers with the same name '{name}' were found")
            return containers

    def exec_run(self, container_id: str, cmd: str, raise_on_error: bool = True) -> tuple[int, str]:
        """Execute a command inside a container in the containerd runtime

        :param container_id: Container ID
        :param cmd: Command to execute
        :param raise_on_error: Raise RuntimeError when non-zero exit code is returned
        """
        with grpc.insecure_channel(f"unix://{self.containerd_sock}") as channel:
            runtime_stub = RuntimeServiceStub(channel)
            request = ExecSyncRequest(container_id=container_id, cmd=shlex.split(cmd))
            response: ExecSyncResponse = runtime_stub.ExecSync(request)

        # Some of our use cases get both stderr and stdout. So capture both
        output = (response.stderr + response.stdout).decode("utf-8")
        if raise_on_error and response.exit_code:
            logger.error(output)
            raise CommandError(
                f"The command returned non-zero code ({response.exit_code}): {output}", exit_code=response.exit_code
            )
        return response.exit_code, output
