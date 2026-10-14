"""Tests for common_libs.clients.email_client.models module"""

import re
from collections.abc import Callable

import pytest
from pytest_data_loader import DataLoaderFixture

from common_libs.clients.email_client import Email
from tests.libs.email_client.emails import EmailSpec, parametrize_email_specs

pytestmark = [pytest.mark.unittest]


class TestEmailFromBytes:
    """Tests for `Email.from_bytes()`"""

    @parametrize_email_specs
    def test_decodes_subject_and_recipient(self, data_loader: DataLoaderFixture, spec: EmailSpec) -> None:
        """Test that encoded subjects are decoded and the recipient is reduced to its address"""
        email = Email.from_bytes(data_loader(spec.template_path))

        assert email.subject == spec.subject
        assert email.to == spec.to

    @parametrize_email_specs
    def test_parses_sender_and_date(self, data_loader: DataLoaderFixture, spec: EmailSpec) -> None:
        """Test that the sender and the timezone-aware date are parsed"""
        email = Email.from_bytes(data_loader(spec.template_path))

        assert email.sender == spec.sender
        assert email.date == spec.date

    @pytest.mark.parametrize("uid", [None, 7])
    def test_sets_uid(self, data_loader: DataLoaderFixture, password_reset: EmailSpec, uid: int | None) -> None:
        """Test that the uid is None unless given"""
        assert Email.from_bytes(data_loader(password_reset.template_path), uid=uid).uid == uid

    @parametrize_email_specs
    def test_decodes_plain_text_body(self, data_loader: DataLoaderFixture, spec: EmailSpec) -> None:
        """Test that quoted-printable and base64 text bodies are decoded"""
        assert spec.text_snippet in Email.from_bytes(data_loader(spec.template_path)).text

    @parametrize_email_specs
    def test_decodes_html_body(self, data_loader: DataLoaderFixture, spec: EmailSpec) -> None:
        """Test that the HTML body is found, including when nested in multipart/related"""
        assert spec.html_snippet in Email.from_bytes(data_loader(spec.template_path)).html

    @parametrize_email_specs
    def test_extracts_links_in_document_order(self, data_loader: DataLoaderFixture, spec: EmailSpec) -> None:
        """Test that all links are extracted with their visible text, in document order"""
        links = Email.from_bytes(data_loader(spec.template_path)).links

        assert tuple(link.text for link in links) == spec.link_texts

    @pytest.mark.parametrize(
        ("headers", "recipients"),
        [
            ("To: Anna <anna@example.com>, bob@example.com", ("anna@example.com", "bob@example.com")),
            ('To: "Schmidt, Anna" <anna@example.com>, bob@example.com', ("anna@example.com", "bob@example.com")),
            ("To: =?utf-8?q?J=C3=B6rg?= <joerg@example.com>", ("joerg@example.com",)),
            ("To: anna@example.com\r\nTo: bob@example.com", ("anna@example.com", "bob@example.com")),
        ],
        ids=["display-name", "comma-in-name", "encoded-name", "two-headers"],
    )
    def test_returns_all_recipients(self, headers: str, recipients: tuple[str, ...]) -> None:
        """Test that every address of every To header is returned without display names"""
        raw = f"{headers}\r\nContent-Type: text/plain\r\n\r\nHello".encode()

        assert Email.from_bytes(raw).to == recipients

    def test_finds_link_in_body_with_non_utf8_charset(self) -> None:
        """Test that a body in another charset than UTF-8 is decoded, so that non-ASCII link text can be matched"""
        raw = (
            "To: qa@example.com\r\nContent-Type: text/html; charset=iso-8859-1\r\n\r\n"
            '<a href="https://e.com/go">Konto bestätigen</a>\r\n'
        ).encode("iso-8859-1")

        assert Email.from_bytes(raw).get_link("Konto bestätigen") == "https://e.com/go"

    def test_ignores_html_attachment(self) -> None:
        """Test that an attached HTML file is not mistaken for the body, even when it comes first"""
        raw = (
            b"To: qa@example.com\r\nContent-Type: multipart/mixed; boundary=X\r\n\r\n"
            b"--X\r\nContent-Type: text/html\r\nContent-Disposition: attachment; filename=copy.html\r\n\r\n"
            b'<a href="https://e.com/attachment">Go</a>\r\n'
            b"--X\r\nContent-Type: text/html\r\n\r\n"
            b'<a href="https://e.com/body">Go</a>\r\n'
            b"--X--\r\n"
        )

        assert Email.from_bytes(raw).get_link("Go") == "https://e.com/body"


class TestEmailGetLink:
    """Tests for `Email.get_link()`"""

    @parametrize_email_specs
    def test_returns_main_button_link(self, data_loader: DataLoaderFixture, spec: EmailSpec) -> None:
        """Test that the URL behind the main button is returned with its HTML entities decoded once"""
        assert Email.from_bytes(data_loader(spec.template_path)).get_link(spec.button_text) == spec.url

    def test_searches_pattern_anywhere_in_the_text(
        self, data_loader: DataLoaderFixture, password_reset: EmailSpec
    ) -> None:
        """Test that a compiled pattern is searched anywhere in the link text, with its flags"""
        email = Email.from_bytes(data_loader(password_reset.template_path))

        assert email.get_link(re.compile(r"PASSWORD$", re.IGNORECASE)) == password_reset.url

    @parametrize_email_specs
    def test_reaches_link_without_text_with_a_pattern(self, data_loader: DataLoaderFixture, spec: EmailSpec) -> None:
        """Test that an image-only link (e.g. a logo) can still be selected by a pattern that matches empty text"""
        email = Email.from_bytes(data_loader(spec.template_path))

        assert email.get_link(re.compile("^$")) == spec.logo_url

    @pytest.mark.parametrize(
        ("html", "text"),
        [
            ('<a href="https://e.com/go">\n  Go  <b>to</b>\n site </a>', "Go to site"),
            ('<a href="https://e.com/go">\n  Go  <b>to</b>\n site </a>', "  Go   to site "),
            ('<a href="https://e.com/go">Go&nbsp;to&nbsp;site</a>', "Go to site"),
        ],
        ids=["nested-tags", "spaces-in-query", "non-breaking-spaces"],
    )
    def test_ignores_whitespace_differences_in_text(
        self, raw_email_factory: Callable[..., bytes], html: str, text: str
    ) -> None:
        """Test that nested tags and runs of whitespace of any kind in the link text do not matter"""
        email = Email.from_bytes(raw_email_factory(html=html))

        assert email.get_link(text) == "https://e.com/go"

    def test_accepts_repeated_links_to_the_same_url(self, raw_email_factory: Callable[..., bytes]) -> None:
        """Test that repeating the same link (e.g. in a header and a footer) is not ambiguous"""
        raw = raw_email_factory(html='<a href="https://e.com/go">Go</a><p><a href="https://e.com/go">Go</a></p>')

        assert Email.from_bytes(raw).get_link("Go") == "https://e.com/go"

    def test_does_not_unescape_the_url_twice(self, raw_email_factory: Callable[..., bytes]) -> None:
        """Test that an escaped ampersand in the URL itself is kept as written in the source"""
        raw = raw_email_factory(html='<a href="https://e.com/go?a=1&amp;amp;b=2">Go</a>')

        assert Email.from_bytes(raw).get_link("Go") == "https://e.com/go?a=1&amp;b=2"

    def test_strips_whitespace_around_the_url(self, raw_email_factory: Callable[..., bytes]) -> None:
        """Test that whitespace around the URL in the `href` attribute is not part of the URL"""
        raw = raw_email_factory(html='<a href="  https://e.com/go \n">Go</a>')

        assert Email.from_bytes(raw).get_link("Go") == "https://e.com/go"

    @pytest.mark.parametrize("name", ["region", "param", "timestamp"])
    def test_keeps_raw_ampersand_before_an_entity_name_in_url(
        self, raw_email_factory: Callable[..., bytes], name: str
    ) -> None:
        """Test that a raw `&` in the URL is kept even when the text after it starts with an entity name"""
        url = f"https://e.com/go?a=1&{name}=eu"
        raw = raw_email_factory(html=f'<a href="{url}">Go</a>')

        assert Email.from_bytes(raw).get_link("Go") == url
