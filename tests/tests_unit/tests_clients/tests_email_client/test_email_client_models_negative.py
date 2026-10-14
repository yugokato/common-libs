"""Negative tests for common_libs.clients.email_client.models module"""

import re
from collections.abc import Callable

import pytest
from pytest_data_loader import DataLoaderFixture

from common_libs.clients.email_client import Email, EmailClientError, LinkNotFoundError
from tests.libs.email_client.emails import UNPARSABLE_HEADERS, EmailSpec, build_email_with_unparsable_header

pytestmark = [pytest.mark.unittest]

UNUSABLE_CHARSETS = ["x-unknown", "idna"]


class TestEmailFromBytesEdgeCases:
    """Tests for `Email.from_bytes()` with incomplete or unusual emails"""

    def test_returns_empty_values_for_missing_headers(self) -> None:
        """Test that an email without any header still parses, with empty values"""
        email = Email.from_bytes(b"Content-Type: text/plain\r\n\r\nHello")

        assert (email.subject, email.sender, email.to, email.date) == ("", "", (), None)

    @pytest.mark.parametrize(
        "to", ["undisclosed-recipients:;", "Friends: ;", None], ids=["undisclosed", "group", "none"]
    )
    def test_returns_no_recipient_without_an_address(
        self, raw_email_factory: Callable[..., bytes], to: str | None
    ) -> None:
        """Test that an empty address group, or no To header at all (e.g. delivered via Bcc), yields no recipient"""
        assert Email.from_bytes(raw_email_factory(to=to)).to == ()

    def test_returns_no_date_for_invalid_date_header(self, raw_email_factory: Callable[..., bytes]) -> None:
        """Test that an unparsable Date header is reported as no date instead of raising"""
        assert Email.from_bytes(raw_email_factory(Date="yesterday-ish")).date is None

    @pytest.mark.parametrize(
        ("kwargs", "text", "html"),
        [
            ({"text": "Hello", "html": None}, "Hello\n", ""),
            ({"text": None, "html": "<p>Hello</p>"}, "", "<p>Hello</p>\n"),
        ],
        ids=["text-only", "html-only"],
    )
    def test_returns_empty_string_for_missing_body(
        self, raw_email_factory: Callable[..., bytes], kwargs: dict[str, str | None], text: str, html: str
    ) -> None:
        """Test that a missing plain text or HTML body is an empty string"""
        email = Email.from_bytes(raw_email_factory(**kwargs))

        assert (email.text, email.html) == (text, html)

    @pytest.mark.parametrize("charset", UNUSABLE_CHARSETS)
    @pytest.mark.parametrize(("subtype", "field"), [("plain", "text"), ("html", "html")])
    def test_reads_body_with_unusable_charset(self, subtype: str, field: str, charset: str) -> None:
        """Test that a body in a charset that Python does not know, or cannot decode text with, is still read"""
        raw = f'To: qa@example.com\r\nContent-Type: text/{subtype}; charset="{charset}"\r\n\r\nGo here\r\n'.encode()

        assert "Go here" in getattr(Email.from_bytes(raw), field)

    @pytest.mark.parametrize("charset", UNUSABLE_CHARSETS)
    def test_finds_links_in_html_with_unusable_charset(self, charset: str) -> None:
        """Test that the links of an HTML body in a charset that Python cannot use are still found"""
        raw = (
            f'To: qa@example.com\r\nContent-Type: text/html; charset="{charset}"\r\n\r\n'
            '<a href="https://e.com/go">Go</a>\r\n'
        ).encode()

        assert Email.from_bytes(raw).get_link("Go") == "https://e.com/go"

    @pytest.mark.parametrize("header", UNPARSABLE_HEADERS)
    @pytest.mark.parametrize(("uid", "described"), [(None, "the email"), (7, "email 7")], ids=["no-uid", "uid"])
    def test_raises_library_error_for_header_it_cannot_parse(
        self, header: str, uid: int | None, described: str
    ) -> None:
        """Test that an address header the standard library fails on is reported as a library error with the UID"""
        with pytest.raises(EmailClientError, match=f"Unable to parse {described}") as exc_info:
            Email.from_bytes(build_email_with_unparsable_header(header), uid=uid)

        assert exc_info.value.__cause__ is not None


class TestEmailGetLinkNegative:
    """Tests for `Email.get_link()` when no single link can be returned"""

    @pytest.mark.parametrize(
        "text",
        ["Activate", "activate account", "Activate account now", "Missing", re.compile(r"^Reset")],
        ids=["part-of-text", "other-case", "extra-word", "unrelated", "pattern"],
    )
    def test_raises_when_no_link_matches_the_text(
        self, raw_email_factory: Callable[..., bytes], text: str | re.Pattern[str]
    ) -> None:
        """Test that text must match the whole visible text case-sensitively, and a pattern must match a part of it"""
        email = Email.from_bytes(raw_email_factory(html='<a href="https://e.com/go">Activate account</a>'))

        with pytest.raises(LinkNotFoundError, match=re.escape(f"No link with text {text!r}")):
            email.get_link(text)

    def test_lists_available_link_texts_in_error(
        self, data_loader: DataLoaderFixture, workspace_invitation: EmailSpec
    ) -> None:
        """Test that the error lists the texts of the links that do exist, and skips the image-only one"""
        email = Email.from_bytes(data_loader(workspace_invitation.template_path))

        with pytest.raises(LinkNotFoundError) as exc_info:
            email.get_link("Missing")

        available = ", ".join(repr(text) for text in workspace_invitation.named_link_texts)
        assert str(exc_info.value).endswith(f"Links with text: {available}")

    def test_raises_for_plain_text_only_email(self, raw_email_factory: Callable[..., bytes]) -> None:
        """Test that URLs in a plain text body are not treated as links"""
        email = Email.from_bytes(raw_email_factory(text="Open https://e.com/go", html=None))

        with pytest.raises(LinkNotFoundError, match="Links with text: none"):
            email.get_link("https://e.com/go")

    @pytest.mark.parametrize("anchor", ["<a>Go</a>", '<a href="">Go</a>', '<a href="  ">Go</a>', '<a name="go">Go</a>'])
    def test_ignores_anchors_without_url(self, raw_email_factory: Callable[..., bytes], anchor: str) -> None:
        """Test that anchors without a usable href are not links"""
        with pytest.raises(LinkNotFoundError):
            Email.from_bytes(raw_email_factory(html=anchor)).get_link("Go")

    @pytest.mark.parametrize(
        ("html", "text", "listed"),
        [
            ('<a href="https://e.com/a">Go</a><a href="https://e.com/b">Go</a>', "Go", "['Go', 'Go']"),
            (
                '<a href="https://e.com/a">Go here</a><a href="https://e.com/b">Go there</a>',
                re.compile("^Go"),
                "['Go here', 'Go there']",
            ),
        ],
        ids=["same-text", "pattern"],
    )
    def test_raises_when_matching_links_differ(
        self, raw_email_factory: Callable[..., bytes], html: str, text: str | re.Pattern[str], listed: str
    ) -> None:
        """Test that matching links pointing to different URLs are reported as ambiguous instead of picking one"""
        email = Email.from_bytes(raw_email_factory(html=html))

        with pytest.raises(EmailClientError) as exc_info:
            email.get_link(text)

        assert str(exc_info.value) == f"Multiple different links match {text!r}: {listed}"

    @pytest.mark.parametrize("text", ["", "   ", "\n\t"], ids=["empty", "spaces", "line-break-and-tab"])
    def test_rejects_blank_text(self, data_loader: DataLoaderFixture, password_reset: EmailSpec, text: str) -> None:
        """Test that blank text is an error instead of silently selecting a link that has no text, such as a logo"""
        email = Email.from_bytes(data_loader(password_reset.template_path))

        with pytest.raises(ValueError, match="text must not be blank"):
            email.get_link(text)
