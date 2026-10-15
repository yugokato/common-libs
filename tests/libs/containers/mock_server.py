import base64
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Self

import httpx2

from tests.libs.containers.base import HOST, ServiceContainer

XDIST_GROUP = "mock-server"
PORT = 1080
# The port of the proxy container that joins the network of this container to serve HTTP/3 (UDP) and HTTPS (TCP)
PROXY_PORT = 8443
PORTS = {f"{PORT}/tcp": (HOST, None), f"{PROXY_PORT}/tcp": (HOST, None), f"{PROXY_PORT}/udp": (HOST, None)}
ADMIN_TIMEOUT = 10.0
MEMORY_LIMIT = "512m"
ENVIRONMENT = {
    "MOCKSERVER_LOG_LEVEL": "WARN",
    # The image enables the usage analytics of the browser dashboard, which the tests never open
    "MOCKSERVER_DASHBOARD_ANALYTICS_ENABLED": "false",
    # Without this, every container signs with the same default CA, whose private key is public and which some
    # developers trust on their machines. A CA that is unique to the container is trusted by nobody
    "MOCKSERVER_DYNAMICALLY_CREATE_CERTIFICATE_AUTHORITY_CERTIFICATE": "true",
}


@dataclass(frozen=True)
class MockResponse:
    """A response that the mock server returns for a matched request

    :param status: The status code
    :param body: A dict or a list is sent as JSON, a str as text, and bytes as they are
    :param headers: The response headers. A list of values sends the header several times
    :param delay: Seconds to wait before responding
    :param chunk_size: Send the body in chunks of this many bytes
    :param chunk_delay: Seconds to wait between the chunks
    """

    status: int = 200
    body: Any = None
    headers: Mapping[str, str | Sequence[str]] = field(default_factory=dict)
    delay: float = 0.0
    chunk_size: int | None = None
    chunk_delay: float = 0.0

    def to_json(self) -> dict[str, Any]:
        """Return the response in the format of the expectations of the mock server"""
        response: dict[str, Any] = {"statusCode": self.status}
        if self.headers:
            response["headers"] = {
                name: [values] if isinstance(values, str) else list(values) for name, values in self.headers.items()
            }
        if self.body is not None:
            response["body"] = _encode_body(self.body)
        if self.delay:
            response["delay"] = _to_delay(self.delay)
        if self.chunk_size:
            response["connectionOptions"] = {"chunkSize": self.chunk_size, "chunkDelay": _to_delay(self.chunk_delay)}
        return response


@dataclass(frozen=True)
class ReceivedRequest:
    """A request that the mock server received

    :param method: The request method
    :param path: The request path
    :param protocol: The protocol the request arrived with: `HTTP_1_1` or `HTTP_2`
    :param secure: Whether the request arrived over TLS
    :param query: The query parameters, with all values of a parameter
    :param headers: The request headers with lower-cased names, with all values of a header
    :param body: The decoded JSON, the text, or the bytes of the request body. None without a body
    """

    method: str
    path: str
    protocol: str
    secure: bool
    query: Mapping[str, list[str]]
    headers: Mapping[str, list[str]]
    body: Any

    def header(self, name: str) -> str | None:
        """Return the first value of a request header, or None if the request did not have it

        :param name: The header name, in any case
        """
        values = self.headers.get(name.lower())
        return values[0] if values else None


class MockServerContainer(ServiceContainer):
    """A MockServer container that answers requests the way the tests tell it to

    https://www.mock-server.com/

    It serves HTTP/1.1, HTTP/2 over TLS, and HTTP/2 over cleartext (h2c) on the same port. The image has no shell, so
    everything is done through its REST API. HTTP/3 is not available in the image, which cannot load its native QUIC
    library. A proxy container that joins the network of this container serves it instead.
    """

    transient_errors = (httpx2.TransportError,)

    def __init__(self) -> None:
        super().__init__("mockserver/mockserver", "8.0.0")
        self._admin_client: httpx2.Client | None = None

    def run(self, **kwargs: Any) -> Self:
        """Run the container and wait until the mock server answers

        :param kwargs: Extra options for the container, which are passed on to the base class
        """
        return super().run(ports=PORTS, environment=ENVIRONMENT, mem_limit=MEMORY_LIMIT, **kwargs)

    def delete(self) -> None:
        """Close the connection to the REST API and delete the container"""
        if self._admin_client is not None:
            self._admin_client.close()
            self._admin_client = None
        super().delete()

    def url(self, scheme: Literal["http", "https"] = "http") -> str:
        """Return the base URL of the mock server. Both schemes use the same port

        :param scheme: `http` also serves h2c, and `https` serves HTTP/1.1 and HTTP/2 (ALPN)
        """
        return f"{scheme}://{HOST}:{self.host_port(PORT)}"

    def udp_port(self, container_port: int) -> int:
        """Return the port on the host that the UDP port of the container is published on

        :param container_port: The port inside the container
        """
        assert self.container is not None
        return int(self.container.ports[f"{container_port}/udp"][0]["HostPort"])

    def ca_certificate(self) -> str:
        """Return the PEM of the certificate authority that signed the certificate of the HTTPS port"""
        response = self._admin.get("/proxyConfiguration")
        response.raise_for_status()
        return str(response.json()["caCertificatePem"])

    def expect(
        self,
        path: str,
        *responses: MockResponse,
        method: str | None = None,
        headers: Mapping[str, str] | None = None,
        priority: int = 0,
    ) -> None:
        """Respond to the requests that match

        With several responses, they are returned one by one in order, and start over after the last one.

        :param path: The request path
        :param responses: The responses. A 200 response without a body if none is given
        :param method: Only match this request method
        :param headers: Only match requests with these headers. A value is a regular expression
        :param priority: The expectation with the highest priority wins when several match
        """
        request: dict[str, Any] = {"path": path}
        if method:
            request["method"] = method
        if headers:
            request["headers"] = {name: [pattern] for name, pattern in headers.items()}
        expectation: dict[str, Any] = {"httpRequest": request, "priority": priority}
        if len(responses) > 1:
            expectation["httpResponses"] = [response.to_json() for response in responses]
            expectation["responseMode"] = "SEQUENTIAL"
        else:
            expectation["httpResponse"] = (responses or (MockResponse(),))[0].to_json()
        self._put("/expectation", json=expectation)

    def expect_dropped_connection(self, path: str, *, method: str | None = None) -> None:
        """Close the connection without a response when a request matches

        :param path: The request path
        :param method: Only match this request method
        """
        request = {"path": path} | ({"method": method} if method else {})
        self._put("/expectation", json={"httpRequest": request, "httpError": {"dropConnection": True}})

    def received(self, path: str) -> list[ReceivedRequest]:
        """Return the requests the mock server received for a path, in the order they arrived

        :param path: The request path
        """
        response = self._put("/retrieve", params={"type": "requests", "format": "json"}, json={"path": path})
        return [
            ReceivedRequest(
                method=item["method"],
                path=item["path"],
                protocol=item["protocol"],
                secure=item["secure"],
                query=item.get("queryStringParameters", {}),
                headers={name.lower(): values for name, values in item.get("headers", {}).items()},
                body=_decode_body(item.get("body")),
            )
            for item in response.json()
        ]

    def reset(self) -> None:
        """Remove all expectations and received requests"""
        self._put("/reset")

    @property
    def _admin(self) -> httpx2.Client:
        if self._admin_client is None:
            self._admin_client = httpx2.Client(
                base_url=f"http://{HOST}:{self.host_port(PORT)}/mockserver", timeout=ADMIN_TIMEOUT
            )
        return self._admin_client

    def _readiness_steps(self) -> Sequence[Callable[[], object]]:
        return [self._check_status]

    def _check_status(self) -> None:
        self._put("/status")

    def _put(self, path: str, **kwargs: Any) -> httpx2.Response:
        response = self._admin.put(path, **kwargs)
        response.raise_for_status()
        return response


def _encode_body(body: Any) -> Any:
    if isinstance(body, bytes):
        return {"type": "BINARY", "base64Bytes": base64.b64encode(body).decode()}
    if isinstance(body, dict | list):
        return {"type": "JSON", "json": body}
    return body


def _decode_body(body: Any) -> Any:
    if not isinstance(body, dict):
        return body
    match body.get("type"):
        case "JSON":
            return body["json"]
        case "STRING":
            return body["string"]
        case "BINARY":
            return base64.b64decode(body["base64Bytes"])
        case _:
            return body


def _to_delay(seconds: float) -> dict[str, Any]:
    return {"timeUnit": "MILLISECONDS", "value": round(seconds * 1000)}
