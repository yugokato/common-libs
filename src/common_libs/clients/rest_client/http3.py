import asyncio
import logging
from collections import deque
from collections.abc import AsyncIterator
from typing import Any

import httpx
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3Connection
from aioquic.h3.events import DataReceived, H3Event, Headers, HeadersReceived
from aioquic.quic.events import QuicEvent

logger = logging.getLogger("client")

# https://github.com/aiortc/aioquic/blob/6d36838d008c2202c337142fa07e8bf80e96bac8/examples/httpx_client.py


class H3ResponseStream(httpx.AsyncByteStream):
    """Async byte stream wrapping an async iterator for HTTP/3 response bodies."""

    def __init__(self, aiterator: AsyncIterator[bytes]) -> None:
        self._aiterator = aiterator

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for part in self._aiterator:
            yield part


class H3Transport(QuicConnectionProtocol, httpx.AsyncBaseTransport):
    """httpx async transport that sends requests over an HTTP/3 (QUIC) connection."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self._http = H3Connection(self._quic)
        self._read_queue: dict[int, deque[H3Event]] = {}
        self._read_ready: dict[int, asyncio.Event] = {}

    def _cleanup_stream(self, stream_id: int) -> None:
        """Remove per-stream state after the response has been fully consumed.

        :param stream_id: The QUIC stream ID to clean up.
        """
        self._read_queue.pop(stream_id, None)
        self._read_ready.pop(stream_id, None)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Send an HTTP/3 request and return the response.

        :param request: The httpx request to send.
        """
        assert isinstance(request.stream, httpx.AsyncByteStream)

        stream_id = self._quic.get_next_available_stream_id()
        self._read_queue[stream_id] = deque()
        self._read_ready[stream_id] = asyncio.Event()

        # prepare request
        self._http.send_headers(
            stream_id=stream_id,
            headers=[
                (b":method", request.method.encode()),
                (b":scheme", request.url.raw_scheme),
                (b":authority", request.url.netloc),
                (b":path", request.url.raw_path),
            ]
            + [(k.lower(), v) for (k, v) in request.headers.raw if k.lower() not in (b"connection", b"host")],
        )
        async for data in request.stream:
            self._http.send_data(stream_id=stream_id, data=data, end_stream=False)
        self._http.send_data(stream_id=stream_id, data=b"", end_stream=True)

        # transmit request
        self.transmit()

        # process response
        status_code, headers, stream_ended = await self._receive_response(stream_id)

        return httpx.Response(
            status_code=status_code,
            headers=headers,
            stream=H3ResponseStream(self._receive_response_data_with_cleanup(stream_id, stream_ended)),
            extensions={
                "http_version": b"HTTP/3",
            },
        )

    def http_event_received(self, event: H3Event) -> None:
        """Dispatch an incoming HTTP/3 event to the appropriate stream queue.

        :param event: The H3Event received from the connection.
        """
        if isinstance(event, (HeadersReceived, DataReceived)):
            stream_id = event.stream_id
            if stream_id in self._read_queue:
                self._read_queue[event.stream_id].append(event)
                self._read_ready[event.stream_id].set()

    def quic_event_received(self, event: QuicEvent) -> None:
        """Forward a QUIC-level event to the HTTP/3 layer.

        :param event: The QuicEvent received from the QUIC connection.
        """
        # pass event to the HTTP layer
        if self._http is not None:
            for http_event in self._http.handle_event(event):
                self.http_event_received(http_event)

    async def _receive_response(self, stream_id: int) -> tuple[int, Headers, bool]:
        """
        Read the response status and headers.
        """
        while True:
            event = await self._wait_for_http_event(stream_id)
            if isinstance(event, HeadersReceived):
                stream_ended = event.stream_ended
                break

        headers = []
        status_code = 0
        for header, value in event.headers:
            if header == b":status":
                status_code = int(value.decode())
            else:
                headers.append((header, value))
        return status_code, headers, stream_ended

    async def _receive_response_data_with_cleanup(self, stream_id: int, stream_ended: bool) -> AsyncIterator[bytes]:
        """Yield response body bytes and clean up stream state when done.

        Handles both the no-body case (stream_ended=True) and the normal streaming
        case. Cleanup always runs in the finally block.

        :param stream_id: The QUIC stream ID to read from.
        :param stream_ended: Whether the stream was already ended after headers.
        """
        try:
            if not stream_ended:
                async for chunk in self._receive_response_data(stream_id):
                    yield chunk
        finally:
            self._cleanup_stream(stream_id)

    async def _receive_response_data(self, stream_id: int) -> AsyncIterator[bytes]:
        """Read the response data.

        :param stream_id: The QUIC stream ID to read from.
        """
        stream_ended = False
        while not stream_ended:
            event = await self._wait_for_http_event(stream_id)
            if isinstance(event, DataReceived):
                stream_ended = event.stream_ended
                yield event.data
            elif isinstance(event, HeadersReceived):
                stream_ended = event.stream_ended

    async def _wait_for_http_event(self, stream_id: int) -> H3Event:
        """
        Returns the next HTTP/3 event for the given stream.
        """
        if not self._read_queue[stream_id]:
            await self._read_ready[stream_id].wait()
        event = self._read_queue[stream_id].popleft()
        if not self._read_queue[stream_id]:
            self._read_ready[stream_id].clear()
        return event
