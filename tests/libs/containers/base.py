"""Base class of the containers that the integration tests run against"""

import time
import uuid
from collections.abc import Callable, Sequence
from typing import Any, Self

from common_libs.containers.container import BaseContainer

HOST = "127.0.0.1"
READY_TIMEOUT = 60.0
READY_POLL_INTERVAL = 0.5
STOPPED_STATUSES = frozenset({"exited", "dead"})


class ServiceContainer(BaseContainer):
    """A container that runs a service for the integration tests

    Every instance gets its own session label. `BaseContainer.run()` deletes the existing containers that match the
    image and labels, so parallel workers would otherwise delete each other's containers. The container is kept when
    the start-up crashes, so that its logs can be read, and it is deleted if the service does not become ready.

    A subclass lists the checks that tell the service is ready in `_readiness_steps()`. A step is retried while it
    raises one of `transient_errors`, until `ready_timeout` seconds have passed.

    The module of a subclass defines `XDIST_GROUP`, which every test module that uses the service puts in its
    `xdist_group` mark. The tests of a service then run on one worker, and its session fixture starts the container
    once per run instead of once per module.

    :param image: The image name
    :param tag: The image tag
    """

    ready_timeout = READY_TIMEOUT
    transient_errors: tuple[type[Exception], ...] = (OSError,)

    def __init__(self, image: str, tag: str) -> None:
        super().__init__(image, tag=tag, labels={"session": uuid.uuid4().hex})

    def run(self, **kwargs: Any) -> Self:
        """Run the container and wait until the service is ready

        :param kwargs: Options for the container, which are passed on to the base class
        """
        super().run(remove=False, **kwargs)
        try:
            self._wait_until_ready()
        except BaseException:
            self.delete()
            raise
        return self

    def _readiness_steps(self) -> Sequence[Callable[[], object]]:
        raise NotImplementedError

    def _wait_until_ready(self) -> None:
        deadline = time.monotonic() + self.ready_timeout
        for step in self._readiness_steps():
            while True:
                try:
                    step()
                    break
                except Exception as e:
                    if self._has_stopped():
                        raise self._not_ready_error("stopped while starting up", e) from None
                    if not isinstance(e, self.transient_errors):
                        raise
                    if time.monotonic() > deadline:
                        raise self._not_ready_error(
                            f"did not become ready in {self.ready_timeout:g} seconds", e
                        ) from None
                    time.sleep(READY_POLL_INTERVAL)

    def _has_stopped(self) -> bool:
        assert self.container is not None
        self.container.reload()
        return bool(self.container.status in STOPPED_STATUSES)

    def _not_ready_error(self, reason: str, cause: Exception) -> RuntimeError:
        assert self.container is not None
        logs = self.container.logs().decode(errors="replace")
        return RuntimeError(f"The {self.image} container {reason} ({cause!r}):\n{logs}")
