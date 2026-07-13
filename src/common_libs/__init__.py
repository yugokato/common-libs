from importlib.metadata import PackageNotFoundError, version
from logging import NullHandler, getLogger

try:
    __version__ = version("common-libs")
except PackageNotFoundError:
    __version__ = "unknown"


# Downstream projects can opt in logging by calling `common_libs.logging.setup_logging()` explicitly. Until then,
# attach a NullHandler so `common_libs` loggers never trigger the "No handlers could be found" warning.
getLogger(__name__).addHandler(NullHandler())
