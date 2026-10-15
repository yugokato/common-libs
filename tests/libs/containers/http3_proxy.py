import ssl
import tempfile
from collections.abc import Callable, Sequence
from typing import Any, Self

import docker.errors
import httpx2

from tests.libs.containers.base import HOST, ServiceContainer
from tests.libs.containers.mock_server import PORT, PROXY_PORT, MockServerContainer

ROOT_CA_PATH = "/data/caddy/pki/authorities/local/root.crt"
CHECK_TIMEOUT = 5.0
# `default_sni` makes the server answer a client that connects to an IP address, which sends no server name
CADDYFILE = f"""\
{{
\tdefault_sni {HOST}
\tauto_https disable_redirects
}}

https://{HOST}:{PROXY_PORT} {{
\treverse_proxy localhost:{PORT}
}}
"""
START_SCRIPT = 'printf "%s" "$CADDYFILE" > /tmp/Caddyfile && exec caddy run --config /tmp/Caddyfile --adapter caddyfile'


class Http3ProxyContainer(ServiceContainer):
    """A Caddy container that serves HTTP/3 in front of a mock server container

    The released images of the mock server cannot load their native QUIC library, so they cannot serve HTTP/3 on their
    own. This container joins the network of the mock server container, and proxies the requests it receives over
    HTTP/3 (and HTTPS) to the mock server with HTTP/1.1. The mock server records them as HTTP/1.1 requests with a `Via`
    header that tells the protocol of the proxy: `3.0 Caddy`.

    Caddy issues the certificate of the proxy from its own certificate authority.

    :param mock_server: The running mock server container to put the proxy in front of
    """

    transient_errors = (docker.errors.NotFound, httpx2.TransportError)

    def __init__(self, mock_server: MockServerContainer) -> None:
        super().__init__("caddy", "2.11.7-alpine")
        self._mock_server = mock_server
        self._ca_certificate: str | None = None

    @property
    def url(self) -> str:
        """The base URL to connect to over HTTP/3"""
        return f"https://{HOST}:{self._mock_server.udp_port(PROXY_PORT)}"

    def run(self, **kwargs: Any) -> Self:
        """Run the container and wait until the proxy answers over HTTPS

        :param kwargs: Extra options for the container, which are passed on to the base class
        """
        assert self._mock_server.container is not None, "The mock server container must be running"
        return super().run(
            network_mode=f"container:{self._mock_server.container.id}",
            environment={"CADDYFILE": CADDYFILE},
            command=["sh", "-c", START_SCRIPT],
            **kwargs,
        )

    def ca_certificate(self) -> str:
        """Return the PEM of the certificate authority that signed the certificate of the proxy"""
        assert self._ca_certificate is not None, "The container has not been run yet"
        return self._ca_certificate

    def _readiness_steps(self) -> Sequence[Callable[[], object]]:
        return [self._read_ca_certificate, self._check_https]

    def _read_ca_certificate(self) -> None:
        # Caddy creates its certificate authority while it starts up
        with tempfile.TemporaryDirectory() as directory:
            self._ca_certificate = self.download_file(ROOT_CA_PATH, directory).read_text()

    def _check_https(self) -> None:
        # Any answer proves that the certificate is issued and the route works. The HTTP/3 listener belongs to the same
        # server, and the TCP port is published to check it, because the client for HTTP/3 is asynchronous
        context = ssl.create_default_context(cadata=self.ca_certificate())
        httpx2.get(f"https://{HOST}:{self._mock_server.host_port(PROXY_PORT)}/", verify=context, timeout=CHECK_TIMEOUT)
