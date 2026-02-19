"""Tests for common_libs.files module"""

import re
import tarfile
from pathlib import Path

from common_libs.files import create_tar_file, generate_filename, generate_temp_file


class TestGenerateFilename:
    """Tests for generate_filename function"""

    def test_generate_filename_basic(self) -> None:
        """Test basic filename generation with timestamp"""
        result = generate_filename("test.txt")
        # Should have format: test_YYYYMMDD-HHMMSS.ffffff.txt
        assert result.startswith("test_")
        assert result.endswith(".txt")
        assert re.match(r"test_\d{8}-\d{6}\.\d+\.txt", result)

    def test_generate_filename_without_msec(self) -> None:
        """Test filename generation without milliseconds"""
        result = generate_filename("test.txt", add_msec=False)
        # Should have format: test_YYYYMMDD-HHMMSS.txt
        assert re.match(r"test_\d{8}-\d{6}\.txt", result)

    def test_generate_filename_normalizes_spaces(self) -> None:
        """Test that spaces are normalized to underscores"""
        result = generate_filename("my file.txt")
        assert " " not in result
        assert "_" in result

    def test_generate_filename_removes_special_chars(self) -> None:
        """Test that special characters are removed"""
        result = generate_filename("test@#$%.txt")
        assert "@" not in result
        assert "#" not in result
        assert "$" not in result
        assert "%" not in result

    def test_generate_filename_multiple_underscores(self) -> None:
        """Test that multiple consecutive underscores are collapsed"""
        result = generate_filename("test___file.txt")
        assert "___" not in result

    def test_generate_filename_no_extension(self) -> None:
        """Test filename without extension"""
        result = generate_filename("testfile")
        assert re.match(r"testfile_\d{8}-\d{6}\.\d+", result)


class TestGenerateTempFile:
    """Tests for generate_temp_file context manager"""

    def test_generate_temp_file_string_content(self) -> None:
        """Test creating temp file with string content"""
        content = "test content"
        with generate_temp_file(content) as path:
            assert path.exists()
            assert path.read_text() == content
            # Note: The temp file is closed after context exit, so we read before

    def test_generate_temp_file_bytes_content(self) -> None:
        """Test creating temp file with bytes content"""
        content = b"binary content"
        with generate_temp_file(content, mode="wb") as path:
            assert path.exists()
            assert path.read_bytes() == content

    def test_generate_temp_file_returns_path(self) -> None:
        """Test that the function returns a Path object"""
        with generate_temp_file("test") as path:
            assert isinstance(path, Path)


class TestCreateTarFile:
    """Tests for create_tar_file function"""

    def test_create_tar_file_gz(self, temp_file: Path, temp_dir: Path) -> None:
        """Test creating gzip tar archive"""
        result = create_tar_file(temp_file, temp_dir, mode="gz")
        assert result.exists()
        assert result.suffix == ".gz"
        assert ".tar" in result.name

    def test_create_tar_file_bz2(self, temp_file: Path, temp_dir: Path) -> None:
        """Test creating bzip2 tar archive"""
        result = create_tar_file(temp_file, temp_dir, mode="bz2")
        assert result.exists()
        assert result.name.endswith(".tar.bz2")

    def test_create_tar_file_default_dest(self, temp_file: Path) -> None:
        """Test creating tar file with default temp directory"""
        result = create_tar_file(temp_file)
        try:
            assert result.exists()
        finally:
            result.unlink()
            result.parent.rmdir()

    def test_create_tar_file_string_path(self, temp_file: Path, temp_dir: Path) -> None:
        """Test creating tar file with string path input"""
        result = create_tar_file(str(temp_file), str(temp_dir))
        assert result.exists()

    def test_create_tar_file_contents(self, temp_file: Path, temp_dir: Path) -> None:
        """Test that tar file contains the original file"""
        tar_path = create_tar_file(temp_file, temp_dir)
        with tarfile.open(tar_path, "r:gz") as tar:
            names = tar.getnames()
            assert temp_file.name in names
