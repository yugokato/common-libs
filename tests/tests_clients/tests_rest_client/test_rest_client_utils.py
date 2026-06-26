"""Tests for common_libs.clients.rest_client.utils module"""

import errno
import inspect
import json
from collections.abc import Callable
from http import HTTPStatus
from typing import Any

import pytest
from pytest_mock import MockFixture

from common_libs.clients.rest_client.rest_client import AsyncRestClient
from common_libs.clients.rest_client.types import RestResponse
from common_libs.clients.rest_client.utils import (
    TRUNCATE_LEN,
    get_response_reason,
    get_supported_request_parameters,
    is_connection_reset,
    manage_content_type,
    mask_sensitive_headers,
    mask_sensitive_value,
    parse_query_strings,
    process_request_body,
    process_response,
    truncate_body,
)


class TestIsConnectionReset:
    """Tests for is_connection_reset helper"""

    def test_returns_true_for_connection_reset_error(self) -> None:
        """Test that a direct ConnectionResetError is detected"""
        exc = ConnectionResetError()
        assert is_connection_reset(exc) is True

    def test_returns_true_for_oserror_with_econnreset_errno(self) -> None:
        """Test that an OSError with errno.ECONNRESET is detected via the errno path, not string matching"""
        exc = OSError(errno.ECONNRESET, "socket error")
        assert is_connection_reset(exc) is True

    def test_returns_true_when_reset_in_cause_chain(self) -> None:
        """Test that is_connection_reset walks the __cause__ chain to find the reset"""
        inner = OSError(errno.ECONNRESET, "socket error")
        outer = RuntimeError("wrapper")
        outer.__cause__ = inner
        assert is_connection_reset(outer) is True

    def test_returns_true_when_reset_in_context_chain(self) -> None:
        """Test that is_connection_reset walks the implicit __context__ chain when __cause__ is absent"""
        inner = OSError(errno.ECONNRESET, "socket error")
        try:
            try:
                raise inner
            except OSError:
                raise RuntimeError("wrapper")
        except RuntimeError as outer:
            assert is_connection_reset(outer) is True

    def test_returns_false_for_unrelated_oserror(self) -> None:
        """Test that an OSError with an unrelated errno is not detected as a connection reset"""
        exc = OSError(errno.ECONNREFUSED, "Connection refused")
        assert is_connection_reset(exc) is False

    def test_returns_false_for_generic_runtime_error(self) -> None:
        """Test that a plain RuntimeError without reset context returns False"""
        exc = RuntimeError("some other error")
        assert is_connection_reset(exc) is False

    def test_returns_true_for_string_fallback(self) -> None:
        """Test the string-match fallback when errno is not set"""
        exc = RuntimeError("Connection reset by peer")
        assert is_connection_reset(exc) is True

    def test_is_cycle_safe(self) -> None:
        """Test that is_connection_reset does not loop infinitely on a cyclic exception chain"""
        a = RuntimeError("a")
        b = RuntimeError("b")
        a.__cause__ = b
        b.__cause__ = a
        assert is_connection_reset(a) is False


class TestMaskSensitiveValue:
    """Tests for mask_sensitive_value function"""

    def test_password_field_masked(self) -> None:
        """Test that password field value is masked with asterisks"""
        username = "admin"
        password = "secret123"
        body: dict[str, Any] = {"username": username, "password": password}
        result = mask_sensitive_value(body, "application/json")
        assert result["username"] == username
        assert result["password"] == "*" * len(password)

    def test_nested_dict_password_masked(self) -> None:
        """Test that nested dict password is also masked"""
        password = "mypass"
        body: dict[str, Any] = {"credentials": {"password": password}}
        mask_sensitive_value(body, "application/json")
        assert body["credentials"]["password"] == "*" * len(password)

    def test_list_with_dicts_password_masked(self) -> None:
        """Test that passwords in list items are masked"""
        body: dict[str, Any] = {"users": [{"password": "pw1"}, {"password": "pw2"}]}
        mask_sensitive_value(body, "application/json")
        assert body["users"][0]["password"] == "***"
        assert body["users"][1]["password"] == "***"

    def test_non_sensitive_dict_unchanged(self) -> None:
        """Test that non-sensitive fields are not modified"""
        username = "admin"
        email = "admin@example.com"
        body: dict[str, Any] = {"username": username, "email": email}
        result = mask_sensitive_value(body, "application/json")
        assert result["username"] == username
        assert result["email"] == email

    def test_form_encoded_password_masked(self) -> None:
        """Test that form-encoded password is masked"""
        body = "username=admin&password=secret"
        result = mask_sensitive_value(body, "application/x-www-form-urlencoded")
        assert "password=secret" not in result
        assert "username=admin" in result
        assert f"password={'*' * len('secret')}" in result

    def test_non_dict_non_string_returned_unchanged(self) -> None:
        """Test that non-dict, non-string values are returned unchanged"""
        result = mask_sensitive_value(42, "application/json")
        assert result == 42

    def test_field_name_containing_password(self) -> None:
        """Test that field name containing 'password' (e.g., old_password) is also masked"""
        old_pass = "oldpass"
        new_pass = "newpass"
        body: dict[str, Any] = {"old_password": old_pass, "new_password": new_pass}
        result = mask_sensitive_value(body, "application/json")
        assert result["old_password"] == "*" * len(old_pass)
        assert result["new_password"] == "*" * len(new_pass)

    def test_form_encoded_value_containing_equals_sign(self) -> None:
        """Test that form-encoded values containing '=' are handled correctly"""
        body = "token=abc=def&other=value"
        result = mask_sensitive_value(body, "application/x-www-form-urlencoded")
        assert isinstance(result, str)
        assert "other=value" in result

    def test_password_field_masked_case_insensitive(self) -> None:
        """Test that password field matching is case-insensitive"""
        password = "secret"
        body: dict[str, Any] = {"Password": password, "PASSWORD": password}
        result = mask_sensitive_value(body, "application/json")
        assert result["Password"] == "*" * len(password)
        assert result["PASSWORD"] == "*" * len(password)

    def test_token_field_masked(self) -> None:
        """Test that fields containing 'token' (e.g. access_token, refresh_token) are masked"""
        token = "eyJhbGc.payload.sig"
        body: dict[str, Any] = {"access_token": token, "refresh_token": token}
        result = mask_sensitive_value(body, "application/json")
        assert result["access_token"] == "*" * len(token)
        assert result["refresh_token"] == "*" * len(token)

    def test_secret_field_masked(self) -> None:
        """Test that fields containing 'secret' (e.g. client_secret) are masked"""
        secret = "my-client-secret"
        body: dict[str, Any] = {"client_secret": secret}
        result = mask_sensitive_value(body, "application/json")
        assert result["client_secret"] == "*" * len(secret)

    def test_api_key_field_masked(self) -> None:
        """Test that fields containing 'api_key' or 'apikey' are masked"""
        api_key = "sk-1234567890"
        body: dict[str, Any] = {"api_key": api_key, "apikey": api_key}
        result = mask_sensitive_value(body, "application/json")
        assert result["api_key"] == "*" * len(api_key)
        assert result["apikey"] == "*" * len(api_key)

    def test_non_string_sensitive_value_masked_with_placeholder(self) -> None:
        """Test that non-string sensitive field values are replaced with a placeholder instead of raising"""
        body: dict[str, Any] = {"password": 12345, "token": None, "secret": True}
        result = mask_sensitive_value(body, "application/json")
        assert result["password"] == "***"
        assert result["token"] == "***"
        assert result["secret"] == "***"

    def test_non_sensitive_numeric_field_unchanged(self) -> None:
        """Test that numeric values on non-sensitive fields are not affected"""
        body: dict[str, Any] = {"age": 30, "count": 0}
        result = mask_sensitive_value(body, "application/json")
        assert result["age"] == 30
        assert result["count"] == 0

    def test_sensitive_field_with_list_of_strings_masked(self) -> None:
        """Test that a sensitive key whose value is a list of strings has each string element masked"""
        body: dict[str, Any] = {"password": ["s3cret", "another"]}
        result = mask_sensitive_value(body, "application/json")
        assert result["password"] == ["*" * len("s3cret"), "*" * len("another")]

    def test_sensitive_field_with_list_of_mixed_types_masked(self) -> None:
        """Test that a sensitive key with a mixed-type list masks strings by length and non-strings with placeholder"""
        body: dict[str, Any] = {"token": ["abc", 123, None]}
        result = mask_sensitive_value(body, "application/json")
        assert result["token"] == ["***", "***", "***"]


class TestMaskSensitiveHeaders:
    """Tests for mask_sensitive_headers function"""

    def test_authorization_header_masked(self) -> None:
        """Test that Authorization header value is replaced with asterisks"""
        headers = {"Authorization": "Bearer secret-token", "Content-Type": "application/json"}
        result = mask_sensitive_headers(headers)
        assert result["Authorization"] == "***"
        assert result["Content-Type"] == "application/json"

    def test_cookie_header_masked(self) -> None:
        """Test that Cookie header value is replaced with asterisks"""
        headers = {"Cookie": "session=abc123"}
        result = mask_sensitive_headers(headers)
        assert result["Cookie"] == "***"

    def test_set_cookie_header_masked(self) -> None:
        """Test that Set-Cookie header value is replaced with asterisks"""
        headers = {"Set-Cookie": "session=abc123; Path=/; HttpOnly"}
        result = mask_sensitive_headers(headers)
        assert result["Set-Cookie"] == "***"

    def test_proxy_authorization_header_masked(self) -> None:
        """Test that Proxy-Authorization header value is replaced with asterisks"""
        headers = {"Proxy-Authorization": "Basic dXNlcjpwYXNz"}
        result = mask_sensitive_headers(headers)
        assert result["Proxy-Authorization"] == "***"

    def test_x_api_key_header_masked(self) -> None:
        """Test that X-Api-Key header value is replaced with asterisks"""
        headers = {"X-Api-Key": "my-api-key"}
        result = mask_sensitive_headers(headers)
        assert result["X-Api-Key"] == "***"

    def test_api_key_header_masked(self) -> None:
        """Test that Api-Key header value is replaced with asterisks"""
        headers = {"Api-Key": "my-api-key"}
        result = mask_sensitive_headers(headers)
        assert result["Api-Key"] == "***"

    def test_matching_is_case_insensitive(self) -> None:
        """Test that header name matching is case-insensitive"""
        headers = {"authorization": "Bearer token", "COOKIE": "sid=123"}
        result = mask_sensitive_headers(headers)
        assert result["authorization"] == "***"
        assert result["COOKIE"] == "***"

    def test_non_sensitive_headers_unchanged(self) -> None:
        """Test that non-sensitive headers are returned unchanged"""
        headers = {"Content-Type": "application/json", "Accept": "application/json", "X-Request-ID": "abc"}
        result = mask_sensitive_headers(headers)
        assert result == headers

    def test_returns_copy_not_mutating_original(self) -> None:
        """Test that mask_sensitive_headers returns a new dict and does not mutate the input"""
        headers = {"Authorization": "Bearer token"}
        result = mask_sensitive_headers(headers)
        assert result is not headers
        assert headers["Authorization"] == "Bearer token"

    def test_empty_headers_returns_empty_dict(self) -> None:
        """Test that an empty input returns an empty dict"""
        assert mask_sensitive_headers({}) == {}


class TestParseQueryStrings:
    """Tests for parse_query_strings function"""

    def test_with_query_string(self) -> None:
        """Test parsing URL with query parameters"""
        expected = {"foo": "bar", "baz": "qux"}
        result = parse_query_strings("http://example.com/api?foo=bar&baz=qux")
        assert result == expected

    def test_without_query_string(self) -> None:
        """Test that URL without query string returns None"""
        result = parse_query_strings("http://example.com/api")
        assert result is None

    def test_multiple_values_for_same_key(self) -> None:
        """Test that multiple values for the same key are returned as a list"""
        expected = ["a", "b", "c"]
        result = parse_query_strings("http://example.com/api?tag=a&tag=b&tag=c")
        assert result is not None
        assert result["tag"] == expected

    def test_single_value_returned_as_string(self) -> None:
        """Test that a single value is returned as a string, not a list"""
        expected = "value"
        result = parse_query_strings(f"http://example.com/api?key={expected}")
        assert result is not None
        assert result["key"] == expected


class TestGetResponseReason:
    """Tests for get_response_reason function"""

    def test_with_reason_phrase(self, mocker: MockFixture) -> None:
        """Test that reason_phrase is returned when available"""
        mock_response = mocker.MagicMock()
        mock_response.reason_phrase = "OK"
        mock_response.status_code = 200
        assert get_response_reason(mock_response) == "OK"

    def test_without_reason_phrase_resolved_from_status(self, mocker: MockFixture) -> None:
        """Test that reason is resolved from HTTPStatus when reason_phrase is empty"""
        mock_response = mocker.MagicMock()
        mock_response.reason_phrase = ""
        mock_response.status_code = 404
        result = get_response_reason(mock_response)
        assert result == HTTPStatus(404).phrase

    def test_unknown_status_code_returns_empty(self, mocker: MockFixture) -> None:
        """Test that unknown status code returns empty string"""
        mock_response = mocker.MagicMock()
        mock_response.reason_phrase = ""
        mock_response.status_code = 999
        result = get_response_reason(mock_response)
        assert result == ""


class TestProcessResponse:
    """Tests for process_response function"""

    def test_json_response(self, mocker: MockFixture) -> None:
        """Test that JSON response content is returned as parsed object"""
        expected = {"key": "value"}
        mock_response = mocker.MagicMock()
        mock_response.is_stream = False
        mock_response.json.return_value = expected
        result = process_response(mock_response)
        assert result == expected

    def test_non_json_response_falls_back_to_content(self, mocker: MockFixture) -> None:
        """Test that non-JSON response falls back to decoded content"""
        expected = "plain text response"
        mock_response = mocker.MagicMock()
        mock_response.is_stream = False
        mock_response.json.side_effect = json.JSONDecodeError("", "", 0)
        mock_response.content = expected.encode()
        result = process_response(mock_response)
        assert result == expected

    def test_prettify_formats_json(self, mocker: MockFixture) -> None:
        """Test that prettify=True formats the JSON response"""
        expected = {"key": "value"}
        mock_response = mocker.MagicMock()
        mock_response.is_stream = False
        mock_response.json.return_value = expected
        result = process_response(mock_response, prettify=True)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed == expected

    def test_stream_success_raises_not_implemented(self, mocker: MockFixture) -> None:
        """Test that processing a successful stream raises NotImplementedError"""
        mock_response = mocker.MagicMock()
        mock_response.is_stream = True
        mock_response.is_success = True
        with pytest.raises(NotImplementedError):
            process_response(mock_response)

    def test_rest_response_unwrapped(self, mocker: MockFixture) -> None:
        """Test that RestResponse is unwrapped to its inner response"""
        expected = {"wrapped": True}
        mock_inner = mocker.MagicMock()
        mock_inner.is_stream = False
        mock_inner.json.return_value = expected
        mock_request = mocker.MagicMock()
        mock_request.request_id = "test-id"
        mock_inner.request = mock_request
        mock_inner.status_code = 200
        mock_inner.is_success = True
        mock_inner.elapsed.total_seconds.return_value = 0.1
        rest_response = RestResponse(mock_inner)
        result = process_response(rest_response)
        assert result == expected


class TestProcessRequestBody:
    """Tests for process_request_body function"""

    def test_json_body_parsed(self, mocker: MockFixture) -> None:
        """Test that JSON body is parsed to dict"""
        expected = {"key": "value"}
        mock_request = mocker.MagicMock()
        mock_request.read.return_value = b'{"key": "value"}'
        mock_request.headers = {"Content-Type": "application/json"}
        result = process_request_body(mock_request, hide_sensitive_values=False)
        assert result == expected

    def test_password_masked_in_json_body(self, mocker: MockFixture) -> None:
        """Test that password is masked in JSON body"""
        mock_request = mocker.MagicMock()
        mock_request.read.return_value = b'{"username": "admin", "password": "secret"}'
        mock_request.headers = {"Content-Type": "application/json"}
        result = process_request_body(mock_request, hide_sensitive_values=True)
        assert isinstance(result, dict)
        assert result["password"] == "******"

    def test_empty_body_returned_as_is(self, mocker: MockFixture) -> None:
        """Test that empty body is returned as is"""
        mock_request = mocker.MagicMock()
        mock_request.read.return_value = b""
        result = process_request_body(mock_request)
        assert not result

    def test_body_without_content_type_does_not_raise(self, mocker: MockFixture) -> None:
        """Test that process_request_body handles a missing Content-Type header without raising"""
        mock_request = mocker.MagicMock()
        mock_request.read.return_value = b'{"key": "value"}'
        mock_request.headers = {}
        result = process_request_body(mock_request, hide_sensitive_values=True)
        assert result == {"key": "value"}


class TestGetSupportedRequestParameters:
    """Tests for get_supported_request_parameters function"""

    def test_returns_list(self) -> None:
        """Test that function returns a list"""
        result = get_supported_request_parameters()
        assert isinstance(result, list)

    def test_contains_custom_parameters(self) -> None:
        """Test that custom parameters are included"""
        result = get_supported_request_parameters()
        assert "quiet" in result


class TestManageContentType:
    """Tests for common_libs.clients.rest_client.utils.manage_content_type()"""

    @pytest.fixture
    def client(self) -> AsyncRestClient:
        """AsyncRestClient instance used as the `self` argument to decorated functions."""
        return AsyncRestClient("http://example.com")

    def _make_decorated(self, mode: str, captured: dict[str, Any]) -> Callable[..., Any]:
        """Build a `manage_content_type`-decorated dummy for the given mode.

        :param mode: Either "sync" or "async".
        :param captured: Dict populated at execution time with the kwargs the dummy received
                         and a snapshot of `self.client.headers.get("Content-Type")`.
        """
        if mode == "sync":

            @manage_content_type
            def dummy(self_arg: AsyncRestClient, **kwargs: Any) -> None:
                captured["kwargs"] = dict(kwargs)
                captured["session_ct"] = self_arg.client.headers.get("Content-Type")

        else:

            @manage_content_type
            async def dummy(self_arg: AsyncRestClient, **kwargs: Any) -> None:
                captured["kwargs"] = dict(kwargs)
                captured["session_ct"] = self_arg.client.headers.get("Content-Type")

        return dummy

    async def invoke(self, f: Callable[..., Any], client: AsyncRestClient, **kwargs: Any) -> None:
        """Call the decorated function and await the result if it is a coroutine.

        :param f: Decorated function to invoke.
        :param client: Client instance passed as the first positional argument.
        :param kwargs: Keyword arguments forwarded to the decorated function.
        """
        result = f(client, **kwargs)
        if inspect.iscoroutine(result):
            await result

    def test_sets_content_type_for_json_payload(self, client: AsyncRestClient) -> None:
        """Test that Content-Type: application/json is injected when a json payload is provided"""
        captured: dict[str, Any] = {}
        dummy = self._make_decorated("sync", captured)
        dummy(client, json={"name": "alice"})
        assert captured["kwargs"]["headers"] == {"Content-Type": "application/json"}

    def test_does_not_set_content_type_when_no_body_params(self, client: AsyncRestClient) -> None:
        """Test that Content-Type is not injected when no json payload is provided (e.g. GET with params)"""
        captured: dict[str, Any] = {}
        dummy = self._make_decorated("sync", captured)
        dummy(client, params={"q": "search"})
        assert "headers" not in captured["kwargs"]

    def test_does_not_set_content_type_for_form_data(self, client: AsyncRestClient) -> None:
        """Test that Content-Type is not injected when form data is provided"""
        captured: dict[str, Any] = {}
        dummy = self._make_decorated("sync", captured)
        dummy(client, data={"field": "value"})
        assert "headers" not in captured["kwargs"]

    def test_does_not_set_content_type_for_files(self, client: AsyncRestClient) -> None:
        """Test that Content-Type is not injected when files are provided"""
        captured: dict[str, Any] = {}
        dummy = self._make_decorated("sync", captured)
        dummy(client, files={"upload": b"content"})
        assert "headers" not in captured["kwargs"]

    def test_does_not_override_existing_request_content_type(self, client: AsyncRestClient) -> None:
        """Test that an existing per-request Content-Type header is not overridden"""
        captured: dict[str, Any] = {}
        dummy = self._make_decorated("sync", captured)
        dummy(client, json={"x": 1}, headers={"Content-Type": "text/plain"})
        assert captured["kwargs"]["headers"]["Content-Type"] == "text/plain"

    def test_does_not_override_existing_session_content_type(self, client: AsyncRestClient) -> None:
        """Test that an existing session-level Content-Type header prevents injection"""
        client.client.headers["Content-Type"] = "application/xml"
        captured: dict[str, Any] = {}
        dummy = self._make_decorated("sync", captured)
        dummy(client, json={"x": 1})
        assert "headers" not in captured["kwargs"]
        assert client.client.headers.get("Content-Type") == "application/xml"

    def test_preserves_other_request_headers_when_injecting(self, client: AsyncRestClient) -> None:
        """Test that existing per-request headers are preserved when Content-Type is injected"""
        captured: dict[str, Any] = {}
        dummy = self._make_decorated("sync", captured)
        dummy(client, json={"x": 1}, headers={"X-Custom": "value"})
        injected = captured["kwargs"]["headers"]
        assert injected["Content-Type"] == "application/json"
        assert injected["X-Custom"] == "value"

    def test_empty_json_payload_sends_no_body(self, client: AsyncRestClient) -> None:
        """Test that an empty json={} payload is normalized to no body and no Content-Type is injected"""
        captured: dict[str, Any] = {}
        dummy = self._make_decorated("sync", captured)
        dummy(client, json={})
        assert captured["kwargs"].get("json") is None
        assert "headers" not in captured["kwargs"]

    def test_does_not_set_content_type_for_json_with_files(self, client: AsyncRestClient) -> None:
        """Test that Content-Type is not injected when both json and files are present"""
        captured: dict[str, Any] = {}
        dummy = self._make_decorated("sync", captured)
        dummy(client, json={"meta": "data"}, files={"upload": b"content"})
        assert "headers" not in captured["kwargs"]

    @pytest.mark.parametrize("mode", ["sync", "async"])
    async def test_content_type_is_request_local(self, client: AsyncRestClient, mode: str) -> None:
        """Test that Content-Type is injected per-request and session headers are not mutated"""
        captured: dict[str, Any] = {}
        dummy = self._make_decorated(mode, captured)
        await self.invoke(dummy, client, json={"key": "value"})
        assert captured["kwargs"]["headers"]["Content-Type"] == "application/json"
        assert captured["session_ct"] is None


class TestTruncateBody:
    """Tests for truncate_body utility"""

    def test_short_string_returned_unchanged(self) -> None:
        """Test that a short string is returned unchanged"""
        short = "hello"
        assert truncate_body(short) is short

    def test_long_string_is_truncated(self) -> None:
        """Test that a string exceeding TRUNCATE_LEN is replaced with a middle-truncated form containing a marker"""
        long_str = "x" * (TRUNCATE_LEN * 4)
        result = truncate_body(long_str)
        assert isinstance(result, str)
        assert len(result) < len(long_str)
        assert "TRUNCATED" in result

    def test_long_bytes_is_truncated(self) -> None:
        """Test that bytes exceeding TRUNCATE_LEN is replaced with a middle-truncated form containing a marker"""
        long_bytes = b"y" * (TRUNCATE_LEN * 4)
        result = truncate_body(long_bytes)
        assert isinstance(result, bytes)
        assert len(result) < len(long_bytes)

    def test_string_at_exact_limit_is_returned_unchanged(self) -> None:
        """Test that a string exactly at TRUNCATE_LEN is not truncated"""
        exact = "z" * TRUNCATE_LEN
        result = truncate_body(exact)
        assert result == exact
