import os
import re
import tarfile
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path


def generate_filename(base_filename: str, add_msec: bool = True) -> str:
    """Convert a base filename to a normalized one with timestamp been added"""
    if add_msec:
        timestr = datetime.now().strftime("%Y%m%d-%H%M%S.%f")
    else:
        timestr = datetime.now().strftime("%Y%m%d-%H%M%S")
    name, extension = os.path.splitext(base_filename)
    timestamped_filename = f"{name}_{timestr}{extension}"

    # Normalize
    normalized_filename = str(timestamped_filename).strip().replace(" ", "_")
    normalized_filename = re.sub(r"(?u)[^-\w._]", "_", normalized_filename)
    normalized_filename = re.sub(r"_+", "_", normalized_filename)
    return normalized_filename


@contextmanager
def generate_temp_file(content: str | bytes, mode: str = "w") -> Iterator[Path]:
    """Generate a temp file with the given file content and return its file path"""
    with tempfile.NamedTemporaryFile(mode=mode) as fp:
        fp.write(content)
        fp.seek(0)
        yield Path(fp.name)


def create_tar_file(file_path: Path | str, dest_dir_path: Path | str = None, mode: str = "gz") -> Path:
    """Create a tar archive in a specified directory, or temp directory if not specified"""
    if not dest_dir_path:
        dest_dir_path = Path(tempfile.mkdtemp())
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    file_name = file_path.name
    tar_file_name = file_name + f".tar.{mode}"
    dest_file_path = Path(dest_dir_path) / tar_file_name
    with tarfile.open(dest_file_path, mode=f"w:{mode}") as tar:
        tar.add(file_path, arcname=file_name)
    return dest_file_path
