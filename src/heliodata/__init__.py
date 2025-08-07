"""HelioData: Open-source tool for downloading heliosphere data."""

from __future__ import annotations

__version__ = "0.1.1"
__author__ = "Mingyu Jeon"
__email__ = "mgjeon@khu.ac.kr"

from heliodata.config import DownloadConfig, setup_logging
from heliodata.download.util import get_times, get_respath, validate_fits_file, get_file_checksum
from heliodata.exceptions import (
    HelioDataError,
    DownloadError,
    ValidationError,
    ConfigurationError,
    NetworkError,
    DataNotFoundError,
)

__all__ = [
    "__version__",
    "__author__", 
    "__email__",
    "DownloadConfig",
    "setup_logging",
    "get_times",
    "get_respath", 
    "validate_fits_file",
    "get_file_checksum",
    "HelioDataError",
    "DownloadError",
    "ValidationError",
    "ConfigurationError", 
    "NetworkError",
    "DataNotFoundError",
]