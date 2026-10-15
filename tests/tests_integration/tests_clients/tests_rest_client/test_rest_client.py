"""Integration tests for common_libs.clients.rest_client.rest_client module"""

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

import pytest

from common_libs.clients.rest_client import AsyncRestClient, RestClient
from common_libs.clients.rest_client.types import StreamMode
from tests.libs.containers.http3_proxy import Http3ProxyContainer
from tests.libs.containers.mock_server import XDIST_GROUP, MockResponse, MockServerContainer

pytestmark = [pytest.mark.integrationtest, pytest.mark.xdist_group(XDIST_GROUP)]

HTTP3_TIMEOUT = 15.0
CONCURRENT_REQUESTS = 20
LINES = [f"line {number}" for number in range(5)]
# The server sends one line per chunk, so a stream is received in several pieces
CHUNKED_BODY = "".join(f"{line}\n" for line in LINES)
CHUNK_SIZE = len("line 0\n")
CHUNK_DELAY = 0.05


class TestRequests:
    """Tests for the request methods of RestClient against a real server"""

    @pytest.mark.parametrize("method", ["get", "options"])
    def test_query_parameters_are_sent_in_the_url(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str, method: str
    ) -> None:
        """Test that the keyword arguments of get() and options() are sent as query parameters"""
        mock_server.expect(path, MockResponse(body={"ok": True}))

        response = getattr(client_factory(), method)(path, page=2, tag="a b")

        request = mock_server.received(path)[0]
        assert (response.status_code, response.response) == (200, {"ok": True})
        assert (request.method, request.query) == (method.upper(), {"page": ["2"], "tag": ["a b"]})

    @pytest.mark.parametrize("method", ["post", "put", "patch", "delete"])
    def test_payload_is_sent_as_json(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str, method: str
    ) -> None:
        """Test that the keyword arguments of post(), put(), patch() and delete() are sent as a JSON body"""
        mock_server.expect(path, MockResponse(status=201))

        response = getattr(client_factory(), method)(path, name="alice", tags=[1, 2])

        request = mock_server.received(path)[0]
        assert response.status_code == 201
        assert (request.method, request.body, request.header("content-type")) == (
            method.upper(),
            {"name": "alice", "tags": [1, 2]},
            "application/json",
        )

    def test_post_with_files_sends_multipart_form_data(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that post() sends the files and the other keyword arguments as multipart form data"""
        mock_server.expect(path)

        client_factory().post(path, files={"file": ("report.txt", b"file content", "text/plain")}, title="monthly")

        request = mock_server.received(path)[0]
        assert (request.header("content-type") or "").startswith("multipart/form-data; boundary=")
        for part in ['name="title"', "monthly", 'name="file"; filename="report.txt"', "file content"]:
            assert part in request.body

    @pytest.mark.parametrize(
        ("response", "expected"),
        [
            (MockResponse(body={"a": [1, 2]}), {"a": [1, 2]}),
            (MockResponse(body="plain text", headers={"Content-Type": "text/plain"}), "plain text"),
            (MockResponse(body=b"\xff\xfe\x00", headers={"Content-Type": "application/octet-stream"}), b"\xff\xfe\x00"),
        ],
        ids=["json", "text", "binary"],
    )
    def test_response_body_is_parsed_by_its_content(
        self,
        client_factory: Callable[..., RestClient],
        mock_server: MockServerContainer,
        path: str,
        response: MockResponse,
        expected: Any,
    ) -> None:
        """Test that the response is parsed into a dict for JSON, a str for text, and bytes otherwise"""
        mock_server.expect(path, response)

        assert client_factory().get(path).response == expected

    def test_request_id_is_sent_to_the_server(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that the request ID of the response is the X-Request-ID header the server received"""
        mock_server.expect(path)

        response = client_factory().get(path)

        assert mock_server.received(path)[0].header("x-request-id") == response.request_id


class TestProtocols:
    """Tests for the HTTP versions RestClient negotiates with a real server"""

    @pytest.mark.parametrize(
        ("scheme", "options", "protocol", "secure"),
        [
            ("https", {}, "HTTP_2", True),
            ("https", {"http2": False}, "HTTP_1_1", True),
            ("http", {}, "HTTP_1_1", False),
            ("http", {"http1": False}, "HTTP_2", False),
        ],
        ids=["https-h2", "https-http1", "http-http1", "http-h2c"],
    )
    def test_negotiates_the_protocol(
        self,
        client_factory: Callable[..., RestClient],
        mock_server: MockServerContainer,
        path: str,
        scheme: Literal["http", "https"],
        options: dict[str, Any],
        protocol: str,
        secure: bool,
    ) -> None:
        """Test that HTTP/2 is negotiated over TLS (ALPN), HTTP/1.1 is used otherwise, and h2c needs prior knowledge"""
        mock_server.expect(path)

        client_factory(base_url=mock_server.url(scheme), **options).get(path)

        request = mock_server.received(path)[0]
        assert (request.protocol, request.secure) == (protocol, secure)

    async def test_concurrent_requests_are_all_served_over_http2(
        self, async_client_factory: Callable[..., AsyncRestClient], mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that the requests an async client sends concurrently all succeed over HTTP/2"""
        mock_server.expect(path, MockResponse(delay=0.2))
        client = async_client_factory()

        responses = await asyncio.gather(*(client.get(path, number=number) for number in range(CONCURRENT_REQUESTS)))

        received = mock_server.received(path)
        assert {response.status_code for response in responses} == {200}
        assert (len(received), {request.protocol for request in received}) == (CONCURRENT_REQUESTS, {"HTTP_2"})


class TestStreaming:
    """Tests for the streaming of responses by RestClient and AsyncRestClient against a real server"""

    @pytest.fixture(autouse=True)
    def chunked_response(self, mock_server: MockServerContainer, path: str) -> None:
        """Make the server send the body in delayed chunks"""
        mock_server.expect(path, MockResponse(body=CHUNKED_BODY, chunk_size=CHUNK_SIZE, chunk_delay=CHUNK_DELAY))

    @pytest.mark.parametrize("scheme", ["http", "https"])
    def test_stream_yields_the_lines_of_the_body(
        self,
        client_factory: Callable[..., RestClient],
        mock_server: MockServerContainer,
        path: str,
        scheme: Literal["http", "https"],
    ) -> None:
        """Test that a streamed response is read line by line over HTTP/1.1 and HTTP/2, without being buffered"""
        with client_factory(base_url=mock_server.url(scheme)).stream("GET", path) as response:
            lines = list(response.stream(mode="line"))

        assert (lines, response.is_stream, response.response, response.response_time) == (LINES, True, None, None)

    @pytest.mark.parametrize(
        ("mode", "separator", "expected"),
        [
            ("text", "", CHUNKED_BODY),
            ("bytes", b"", CHUNKED_BODY.encode()),
            ("raw", b"", CHUNKED_BODY.encode()),
        ],
    )
    def test_stream_yields_the_body_in_several_pieces(
        self,
        client_factory: Callable[..., RestClient],
        path: str,
        mode: StreamMode,
        separator: str | bytes,
        expected: str | bytes,
    ) -> None:
        """Test that the text, bytes and raw modes yield the body as it arrives, which joins up to the whole body"""
        with client_factory().stream("GET", path) as response:
            pieces: list[Any] = list(response.stream(mode=mode))

        assert (len(pieces) > 1, separator.join(pieces)) == (True, expected)

    async def test_astream_yields_the_lines_of_the_body(
        self, async_client_factory: Callable[..., AsyncRestClient], path: str
    ) -> None:
        """Test that an async client reads a streamed response line by line"""
        async with async_client_factory().stream("GET", path) as response:
            lines = [line async for line in response.astream(mode="line")]

        assert (lines, response.is_stream) == (LINES, True)


class TestRedirects:
    """Tests for how RestClient follows redirects against a real server"""

    def test_redirect_is_followed_with_the_same_request_id(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that a redirect is followed when allowed, and that all hops carry the same X-Request-ID"""
        landed = f"{path}/landed"
        mock_server.expect(path, MockResponse(status=302, headers={"Location": landed}))
        mock_server.expect(landed, MockResponse(body={"landed": True}))

        response = client_factory(follow_redirects=True).get(path)

        request_ids = [mock_server.received(p)[0].header("x-request-id") for p in (path, landed)]
        assert (response.response, request_ids) == ({"landed": True}, [response.request_id] * 2)

    def test_redirect_is_returned_when_not_allowed(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that the redirect response itself is returned by default"""
        mock_server.expect(path, MockResponse(status=302, headers={"Location": f"{path}/landed"}))

        response = client_factory().get(path)

        assert (response.status_code, len(mock_server.received(f"{path}/landed"))) == (302, 0)


class TestHttp3:
    """Tests for AsyncRestClient.http3() against a real server, through a proxy that serves HTTP/3"""

    async def test_get_is_answered_over_http3(
        self, http3_proxy: Http3ProxyContainer, http3_ca_file: Path, mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that a request is sent over HTTP/3 to a server whose certificate is verified with the given CA"""
        mock_server.expect(path, MockResponse(body={"hello": "h3"}))

        async with AsyncRestClient.http3(http3_proxy.url, verify=http3_ca_file, retry_policy=None) as client:
            async with asyncio.timeout(HTTP3_TIMEOUT):
                response = await client.get(path, page=1)

        request = mock_server.received(path)[0]
        assert (response.status_code, response.response) == (200, {"hello": "h3"})
        assert (request.query, request.header("via")) == ({"page": ["1"]}, "3.0 Caddy")

    async def test_post_sends_a_json_body_over_http3(
        self, http3_proxy: Http3ProxyContainer, http3_ca_file: Path, mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that the body of a request is sent over HTTP/3"""
        mock_server.expect(path, MockResponse(status=201))

        async with AsyncRestClient.http3(http3_proxy.url, verify=http3_ca_file, retry_policy=None) as client:
            async with asyncio.timeout(HTTP3_TIMEOUT):
                response = await client.post(path, name="alice", tags=[1, 2])

        request = mock_server.received(path)[0]
        assert response.status_code == 201
        assert (request.method, request.body, request.header("via")) == (
            "POST",
            {"name": "alice", "tags": [1, 2]},
            "3.0 Caddy",
        )

    async def test_verification_can_be_disabled(
        self, http3_proxy: Http3ProxyContainer, mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that verify=False accepts a certificate that is not signed by a trusted CA"""
        mock_server.expect(path)

        async with AsyncRestClient.http3(http3_proxy.url, verify=False, retry_policy=None) as client:
            async with asyncio.timeout(HTTP3_TIMEOUT):
                response = await client.get(path)

        assert response.status_code == 200
