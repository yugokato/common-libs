from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

try:
    __version__ = version("common-libs")
except PackageNotFoundError:
    pass


from .logging import setup_logging

setup_logging(Path(__file__).parent.parent / "cfg" / "logging.yaml")
