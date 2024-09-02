from pathlib import Path

from .logging import setup_logging

setup_logging(Path(__file__).parent.parent / "cfg" / "logging.yaml")
