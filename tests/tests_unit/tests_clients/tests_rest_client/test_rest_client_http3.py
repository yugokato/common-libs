"""Tests for common_libs.clients.rest_client.http3 module"""

import ssl
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from aioquic.h3.connection import H3_ALPN

from common_libs.clients.rest_client.http3 import create_quic_configuration

pytestmark = [pytest.mark.unittest]


class TestCreateQuicConfiguration:
    """Tests for create_quic_configuration()"""

    @pytest.mark.parametrize("kwargs", [{}, {"verify": True}], ids=["default", "true"])
    def test_verifies_against_certifi_bundle_by_default(self, kwargs: dict[str, Any]) -> None:
        """Test that the aioquic default (certifi bundle, certificate required) is kept when verify is True"""
        configuration = create_quic_configuration(**kwargs)

        assert (configuration.verify_mode, configuration.cafile, configuration.capath) == (None, None, None)

    def test_configures_a_client_for_http3(self) -> None:
        """Test that the configuration is for a client that negotiates the HTTP/3 ALPN protocols"""
        configuration = create_quic_configuration()

        assert (configuration.is_client, configuration.alpn_protocols) == (True, H3_ALPN)

    def test_disables_verification_when_verify_is_false(self) -> None:
        """Test that verify=False accepts any server certificate"""
        assert create_quic_configuration(verify=False).verify_mode == ssl.CERT_NONE

    @pytest.fixture(params=[str, Path], ids=["str", "path"])
    def to_verify(self, request: pytest.FixtureRequest) -> Callable[[Path], str | Path]:
        """Convert a CA path to each type that verify accepts for a path"""
        converter: Callable[[Path], str | Path] = request.param
        return converter

    @pytest.mark.parametrize(("is_dir", "location"), [(False, "cafile"), (True, "capath")], ids=["file", "directory"])
    def test_loads_ca_certificates_from_path(
        self, tmp_path: Path, to_verify: Callable[[Path], str | Path], is_dir: bool, location: str
    ) -> None:
        """Test that a file path is used as a CA bundle and a directory path as a CA directory, given as str or Path"""
        ca_path = tmp_path / "ca"
        if is_dir:
            ca_path.mkdir()
        else:
            ca_path.write_text("-----BEGIN CERTIFICATE-----\n")

        configuration = create_quic_configuration(verify=to_verify(ca_path))

        locations = {"cafile": configuration.cafile, "capath": configuration.capath}
        assert locations == {"cafile": None, "capath": None} | {location: str(ca_path)}

    def test_raises_when_ca_path_does_not_exist(self, tmp_path: Path, to_verify: Callable[[Path], str | Path]) -> None:
        """Test that a mistyped CA path, given as str or Path, fails at once instead of during the TLS handshake"""
        missing = tmp_path / "missing-ca.pem"

        with pytest.raises(FileNotFoundError, match=f"does not exist: {missing}"):
            create_quic_configuration(verify=to_verify(missing))

    @pytest.mark.parametrize(
        "verify",
        [ssl.create_default_context(), None, 1, b"ca.pem"],
        ids=["ssl-context", "none", "int", "bytes"],
    )
    def test_raises_for_unsupported_verify_type(self, verify: Any) -> None:
        """Test that a value that is neither a bool nor a str or Path is rejected instead of being ignored"""
        with pytest.raises(TypeError, match=f"bool or a path to CA certificates, not {type(verify).__name__}"):
            create_quic_configuration(verify=verify)
