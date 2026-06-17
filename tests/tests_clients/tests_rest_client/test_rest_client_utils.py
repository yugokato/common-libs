"""Tests for common_libs.clients.rest_client.utils module"""

import inspect
import json
from collections.abc import Callable
from http import HTTPStatus
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest import FixtureRequest
from pytest_mock import MockFixture

from common_libs.clients.rest_client.ext import RequestExt, RestResponse
from common_libs.clients.rest_client.rest_client import AsyncRestClient
from common_libs.clients.rest_client.utils import (
    get_response_reason,
    get_supported_request_parameters,
    manage_content_type,
    mask_sensitive_headers,
    mask_sensitive_value,
    parse_query_strings,
    process_request_body,
    process_response,
    retry_on,
    set_request_to_exception,
)


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


class TestRetryOn:
    """Tests for retry_on decorator"""

    @pytest.fixture(params=["sync", "async"])
    def mode(self, request: FixtureRequest) -> str:
        """Fixture parametrized over sync and async inner function modes."""
        return request.param

    @pytest.fixture(autouse=True)
    def _mock_async_sleep(self, mocker: MockFixture) -> MagicMock:
        return mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    async def invoke(self, f: Callable[[], Any]) -> Any:
        """Call function and await the result if it is a coroutine."""
        result = f()
        if inspect.iscoroutine(result):
            return await result
        return result

    async def test_no_retry_when_condition_doesnt_match(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that no retry happens when condition doesn't match"""
        status_to_retry = 500
        mock_resp = mock_response_factory(200)
        call_count = 0

        if mode == "sync":

            @retry_on(status_to_retry)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                return mock_resp

        else:

            @retry_on(status_to_retry)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                return mock_resp

        result = await self.invoke(f)
        assert result is mock_resp
        assert call_count == 1

    async def test_retries_on_matching_int_condition(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that retry happens when condition matches an int status code"""
        status_to_retry = 500
        mock_err = mock_response_factory(status_to_retry)
        mock_ok = mock_response_factory(200)
        call_count = 0

        if mode == "sync":

            @retry_on(status_to_retry)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                return mock_err if call_count == 1 else mock_ok

        else:

            @retry_on(status_to_retry)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                return mock_err if call_count == 1 else mock_ok

        result = await self.invoke(f)
        assert result is mock_ok
        assert call_count == 2

    async def test_retries_on_matching_list_condition(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that retry happens when condition matches a list of status codes"""
        status_to_retry = 502
        mock_err = mock_response_factory(status_to_retry)
        mock_ok = mock_response_factory(200)
        call_count = 0

        if mode == "sync":

            @retry_on([status_to_retry, 503])
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                return mock_err if call_count == 1 else mock_ok

        else:

            @retry_on([status_to_retry, 503])
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                return mock_err if call_count == 1 else mock_ok

        result = await self.invoke(f)
        assert result is mock_ok
        assert call_count == 2

    async def test_retries_on_callable_condition(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that retry happens when callable condition returns True"""
        status_to_retry = 503
        condition = lambda r: r.status_code >= 500
        mock_err = mock_response_factory(status_to_retry)
        mock_ok = mock_response_factory(200)
        call_count = 0

        if mode == "sync":

            @retry_on(condition)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                return mock_err if call_count == 1 else mock_ok

        else:

            @retry_on(condition)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                return mock_err if call_count == 1 else mock_ok

        result = await self.invoke(f)
        assert result is mock_ok
        assert call_count == 2

    async def test_retries_with_num_retries(self, mock_response_factory: Callable[..., MagicMock], mode: str) -> None:
        """Test that retry stops after num_retries attempts"""
        status_to_retry = 500
        mock_err = mock_response_factory(status_to_retry)
        call_count = 0
        num_retries = 3

        if mode == "sync":

            @retry_on(status_to_retry, num_retries=num_retries)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                return mock_err

        else:

            @retry_on(status_to_retry, num_retries=num_retries)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                return mock_err

        await self.invoke(f)
        assert call_count == num_retries + 1  # 1 initial + 3 retries

    async def test_retries_with_safe_methods_only_option(
        self, mocker: MockFixture, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that safe_methods_only=True skips retry for non-safe methods like POST"""
        status_to_retry = 500
        mock_logger = mocker.patch("common_libs.clients.rest_client.utils.logger")
        mock_resp = mock_response_factory(status_to_retry)
        mock_resp.request.method = "POST"
        call_count = 0

        if mode == "sync":

            @retry_on(status_to_retry, safe_methods_only=True)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                return mock_resp

        else:

            @retry_on(status_to_retry, safe_methods_only=True)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                return mock_resp

        result = await self.invoke(f)
        assert result is mock_resp
        assert call_count == 1
        assert mock_logger.warning.call_count == 1
        assert "safe_methods_only" in mock_logger.warning.call_args[0][0]

    async def test_retry_after_callable_not_called_when_condition_doesnt_match(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that retry_after callable is not called when condition doesn't match"""
        status_to_retry = 429
        mock_retry_after: MagicMock = MagicMock()
        mock_200 = mock_response_factory(200)

        if mode == "sync":

            @retry_on(status_to_retry, retry_after=mock_retry_after)
            def f() -> Any:
                return mock_200

        else:

            @retry_on(status_to_retry, retry_after=mock_retry_after)
            async def f() -> Any:
                return mock_200

        await self.invoke(f)
        mock_retry_after.assert_not_called()

    async def test_retry_after_callable_called_when_condition_matches(
        self, _mock_async_sleep: MagicMock, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that retry_after callable is called and its return value is used for asyncio.sleep"""
        status_to_retry = 429
        retry_after = 10
        mock_err = mock_response_factory(status_to_retry)
        mock_err.headers = {"Retry-After": retry_after}
        retry_after_func = lambda r: r.headers["Retry-After"]
        if mode == "sync":

            @retry_on(status_to_retry, retry_after=retry_after_func)
            def f() -> Any:
                return mock_err

        else:

            @retry_on(status_to_retry, retry_after=retry_after_func)
            async def f() -> Any:
                return mock_err

        await self.invoke(f)
        _mock_async_sleep.assert_called_once_with(retry_after)

    async def test_retry_after_static_value_used_for_sleep(
        self, _mock_async_sleep: MockFixture, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that static retry_after value is passed directly to asyncio.sleep"""
        status_to_retry = 500
        retry_after = 30
        mock_err = mock_response_factory(status_to_retry)
        mock_ok = mock_response_factory(200)
        call_count = 0

        if mode == "sync":

            @retry_on(status_to_retry, retry_after=retry_after)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                return mock_err if call_count == 1 else mock_ok

        else:

            @retry_on(status_to_retry, retry_after=retry_after)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                return mock_err if call_count == 1 else mock_ok

        await self.invoke(f)
        _mock_async_sleep.assert_called_once_with(retry_after)

    async def test_retried_response_chains_retried_attribute(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that the retried response's request.retried points to the original request"""
        status_to_retry = 500
        mock_err = mock_response_factory(status_to_retry)
        mock_ok = mock_response_factory(200)
        call_count = 0

        if mode == "sync":

            @retry_on(status_to_retry)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                return mock_err if call_count == 1 else mock_ok

        else:

            @retry_on(status_to_retry)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                return mock_err if call_count == 1 else mock_ok

        result = await self.invoke(f)
        assert result is mock_ok
        assert mock_ok.request.retried is mock_err.request

    async def test_retried_exception_chains_retried_attribute(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that a successful retry after an exception sets request.retried to the original request"""
        original_request = MagicMock(spec=RequestExt)
        mock_ok = mock_response_factory(200)
        call_count = 0

        if mode == "sync":

            @retry_on(ValueError)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    exc = ValueError("transient error")
                    set_request_to_exception(exc, original_request)
                    raise exc
                return mock_ok

        else:

            @retry_on(ValueError)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    exc = ValueError("transient error")
                    set_request_to_exception(exc, original_request)
                    raise exc
                return mock_ok

        result = await self.invoke(f)
        assert result is mock_ok
        assert mock_ok.request.retried is original_request

    async def test_invalid_condition_raises_value_error(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that an invalid condition type raises ValueError"""
        mock_resp = mock_response_factory(500)
        invalid_condition: Any = "invalid_condition"

        if mode == "sync":

            @retry_on(invalid_condition)
            def f() -> Any:
                return mock_resp

        else:

            @retry_on(invalid_condition)
            async def f() -> Any:
                return mock_resp

        with pytest.raises(ValueError, match="Invalid condition: "):
            await self.invoke(f)

    async def test_retries_on_single_exception_class(self, mode: str) -> None:
        """Test that retry happens when a single exception class is raised"""
        call_count = 0

        if mode == "sync":

            @retry_on(ValueError)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise ValueError("transient error")
                return "ok"

        else:

            @retry_on(ValueError)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise ValueError("transient error")
                return "ok"

        result = await self.invoke(f)
        assert result == "ok"
        assert call_count == 2

    async def test_retries_on_sequence_of_exception_classes(self, mode: str) -> None:
        """Test that retry happens when condition is a list of exception classes and a matching one is raised"""
        call_count = 0

        if mode == "sync":

            @retry_on([ValueError, RuntimeError])
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise RuntimeError("transient error")
                return "ok"

        else:

            @retry_on([ValueError, RuntimeError])
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise RuntimeError("transient error")
                return "ok"

        result = await self.invoke(f)
        assert result == "ok"
        assert call_count == 2

    async def test_no_retry_on_non_matching_exception(self, mode: str) -> None:
        """Test that non-matching exceptions propagate immediately without retry"""
        call_count = 0

        if mode == "sync":

            @retry_on(ValueError)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                raise RuntimeError("unexpected error")

        else:

            @retry_on(ValueError)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                raise RuntimeError("unexpected error")

        with pytest.raises(RuntimeError, match="unexpected error"):
            await self.invoke(f)
        assert call_count == 1

    async def test_exception_retry_exhausted_reraises(self, mode: str) -> None:
        """Test that after num_retries retries, the last exception is re-raised"""
        call_count = 0
        num_retries = 2

        if mode == "sync":

            @retry_on(ValueError, num_retries=num_retries)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                raise ValueError("persistent error")

        else:

            @retry_on(ValueError, num_retries=num_retries)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                raise ValueError("persistent error")

        with pytest.raises(ValueError, match="persistent error"):
            await self.invoke(f)
        assert call_count == num_retries + 1  # 1 initial + num_retries retries

    async def test_exception_retry_with_static_retry_after(self, _mock_async_sleep: MagicMock, mode: str) -> None:
        """Test that static retry_after value is passed to asyncio.sleep on exception retry"""
        retry_after = 10
        call_count = 0

        if mode == "sync":

            @retry_on(ValueError, retry_after=retry_after)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise ValueError("transient")
                return "ok"

        else:

            @retry_on(ValueError, retry_after=retry_after)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise ValueError("transient")
                return "ok"

        await self.invoke(f)
        _mock_async_sleep.assert_called_once_with(retry_after)

    async def test_exception_retry_with_callable_retry_after(self, _mock_async_sleep: MagicMock, mode: str) -> None:
        """Test that callable retry_after receives the raised exception and its return value is used for sleep"""
        expected_wait = 15
        retry_after_func: MagicMock = MagicMock(return_value=expected_wait)
        call_count = 0
        raised_exc = ValueError("transient error")

        if mode == "sync":

            @retry_on(ValueError, retry_after=retry_after_func)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise raised_exc
                return "ok"

        else:

            @retry_on(ValueError, retry_after=retry_after_func)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise raised_exc
                return "ok"

        await self.invoke(f)
        retry_after_func.assert_called_once_with(raised_exc)
        _mock_async_sleep.assert_called_once_with(expected_wait)

    async def test_safe_methods_only_skips_exception_retry_for_non_safe_method(
        self, mocker: MockFixture, mode: str
    ) -> None:
        """Test that safe_methods_only=True skips exception retry for non-safe methods like POST"""
        mock_logger = mocker.patch("common_libs.clients.rest_client.utils.logger")
        mock_request = MagicMock(spec=RequestExt)
        mock_request.method = "POST"
        call_count = 0

        if mode == "sync":

            @retry_on(ValueError, safe_methods_only=True)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                exc = ValueError("transient error")
                set_request_to_exception(exc, mock_request)
                raise exc

        else:

            @retry_on(ValueError, safe_methods_only=True)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                exc = ValueError("transient error")
                set_request_to_exception(exc, mock_request)
                raise exc

        with pytest.raises(ValueError, match="transient error"):
            await self.invoke(f)
        assert call_count == 1
        assert mock_logger.warning.call_count == 1
        assert "safe_methods_only" in mock_logger.warning.call_args[0][0]

    async def test_safe_methods_only_retries_on_exception_for_safe_method(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that safe_methods_only=True allows exception retry for safe methods like GET"""
        mock_request = MagicMock(spec=RequestExt)
        mock_request.method = "GET"
        mock_ok = mock_response_factory(200)
        call_count = 0

        if mode == "sync":

            @retry_on(ValueError, safe_methods_only=True)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    exc = ValueError("transient error")
                    set_request_to_exception(exc, mock_request)
                    raise exc
                return mock_ok

        else:

            @retry_on(ValueError, safe_methods_only=True)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    exc = ValueError("transient error")
                    set_request_to_exception(exc, mock_request)
                    raise exc
                return mock_ok

        result = await self.invoke(f)
        assert result is mock_ok
        assert call_count == 2

    async def test_safe_methods_only_skips_exception_retry_when_no_request_attached(
        self, mocker: MockFixture, mode: str
    ) -> None:
        """Test that safe_methods_only=True skips exception retry when no request is attached to the exception"""
        mock_logger = mocker.patch("common_libs.clients.rest_client.utils.logger")
        call_count = 0

        if mode == "sync":

            @retry_on(ValueError, safe_methods_only=True)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                raise ValueError("transient error")  # no set_original_request — method is unknown

        else:

            @retry_on(ValueError, safe_methods_only=True)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                raise ValueError("transient error")  # no set_original_request — method is unknown

        with pytest.raises(ValueError, match="transient error"):
            await self.invoke(f)
        assert call_count == 1
        mock_logger.warning.assert_called_once()
        assert "safe_methods_only" in mock_logger.warning.call_args[0][0]

    async def test_callable_condition_retries_on_exception(self, mode: str) -> None:
        """Test that a callable condition can match a raised exception and trigger a retry"""
        call_count = 0

        if mode == "sync":

            @retry_on(lambda x: isinstance(x, ValueError))
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise ValueError("transient error")
                return "ok"

        else:

            @retry_on(lambda x: isinstance(x, ValueError))
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise ValueError("transient error")
                return "ok"

        result = await self.invoke(f)
        assert result == "ok"
        assert call_count == 2

    async def test_callable_condition_does_not_retry_on_exception_for_response_only_callable(self, mode: str) -> None:
        """Test that a response-only callable that errors when given an exception does not cause a retry"""
        call_count = 0

        if mode == "sync":

            @retry_on(lambda r: r.status_code == 503)
            def f() -> Any:
                nonlocal call_count
                call_count += 1
                raise RuntimeError("unexpected error")

        else:

            @retry_on(lambda r: r.status_code == 503)
            async def f() -> Any:
                nonlocal call_count
                call_count += 1
                raise RuntimeError("unexpected error")

        with pytest.raises(RuntimeError, match="unexpected error"):
            await self.invoke(f)
        assert call_count == 1

    def test_empty_sequence_condition_raises_value_error(self) -> None:
        """Test that passing an empty sequence as condition raises ValueError immediately"""
        with pytest.raises(ValueError, match="condition sequence must not be empty"):

            @retry_on([])
            def f() -> Any:
                return "ok"


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

    def test_sets_content_type_when_no_body_params(self, client: AsyncRestClient) -> None:
        """Test that Content-Type is injected when no body parameters are provided"""
        captured: dict[str, Any] = {}
        dummy = self._make_decorated("sync", captured)
        dummy(client, params={"q": "search"})
        assert captured["kwargs"]["headers"] == {"Content-Type": "application/json"}

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

    @pytest.mark.parametrize("mode", ["sync", "async"])
    async def test_content_type_is_request_local(self, client: AsyncRestClient, mode: str) -> None:
        """Test that Content-Type is injected per-request and session headers are not mutated"""
        captured: dict[str, Any] = {}
        dummy = self._make_decorated(mode, captured)
        await self.invoke(dummy, client, json={"key": "value"})
        assert captured["kwargs"]["headers"]["Content-Type"] == "application/json"
        assert captured["session_ct"] is None
