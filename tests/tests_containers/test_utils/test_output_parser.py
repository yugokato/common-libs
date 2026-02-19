"""Tests for common_libs.containers.utils.output_parser module"""

import pytest

from common_libs.containers.utils.output_parser import parse_table_output


class TestParseTableOutput:
    """Tests for parse_table_output function"""

    def test_basic_table_parsing(self) -> None:
        """Test parsing a typical docker ps style output"""
        output = (
            "CONTAINER ID   IMAGE         COMMAND   CREATED       STATUS       PORTS     NAMES\n"
            'baee57c75f17   python:3.11   "bash"    5 hours ago   Up 5 hours             elegant_chatterjee'
        )
        expected = {
            "CONTAINER ID": "baee57c75f17",
            "IMAGE": "python:3.11",
            "COMMAND": '"bash"',
            "CREATED": "5 hours ago",
            "STATUS": "Up 5 hours",
            "PORTS": None,
            "NAMES": "elegant_chatterjee",
        }
        result = parse_table_output(output)
        assert len(result) == 1
        assert result[0] == expected

    def test_multiple_rows(self) -> None:
        """Test parsing table with multiple data rows"""
        output = "NAME         STATUS    AGE\npod-alpha    Running   1h\npod-beta     Pending   5m"
        row0 = {"NAME": "pod-alpha", "STATUS": "Running", "AGE": "1h"}
        row1 = {"NAME": "pod-beta", "STATUS": "Pending", "AGE": "5m"}
        result = parse_table_output(output)
        assert len(result) == 2
        assert result[0] == row0
        assert result[1] == row1

    def test_empty_cell_becomes_none(self) -> None:
        """Test that empty column values are returned as None"""
        output = "NAME         PORT      EXTRA\nservice      8080      \n"
        expected = {"NAME": "service", "PORT": "8080", "EXTRA": None}
        result = parse_table_output(output)
        assert result[0] == expected

    def test_raises_on_header_only(self) -> None:
        """Test ValueError raised when table has a header but no data rows"""
        output = "CONTAINER ID   IMAGE         COMMAND"
        with pytest.raises(ValueError, match="at least one row"):
            parse_table_output(output)

    def test_raises_on_empty_string(self) -> None:
        """Test ValueError raised on empty string"""
        with pytest.raises(ValueError):
            parse_table_output("")

    def test_leading_non_table_data_ignored(self) -> None:
        """Test that lines before the header that match single-space word pattern are skipped"""
        output = "Fetching containers...\nCONTAINER ID   IMAGE         COMMAND\nabc123         alpine        sh"
        expected = {"CONTAINER ID": "abc123", "IMAGE": "alpine", "COMMAND": "sh"}
        result = parse_table_output(output)
        assert len(result) == 1
        assert result[0] == expected

    def test_trailing_non_table_data_ignored(self) -> None:
        """Test that non-table lines after the rows stop parsing"""
        output = "NAME         STATUS\nalpha        Running\nDone."
        expected = {"NAME": "alpha", "STATUS": "Running"}
        result = parse_table_output(output)
        assert len(result) == 1
        assert result[0] == expected

    def test_single_column_table(self) -> None:
        """Test parsing table with a single column"""
        output = "NAME\nalpha\nbeta"
        result = parse_table_output(output)
        assert len(result) == 2
        assert result[0] == {"NAME": "alpha"}
        assert result[1] == {"NAME": "beta"}
