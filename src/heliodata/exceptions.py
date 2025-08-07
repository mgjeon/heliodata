"""Custom exceptions for HelioData."""

from __future__ import annotations


class HelioDataError(Exception):
    """Base exception for HelioData operations."""
    pass


class DownloadError(HelioDataError):
    """Exception raised during data download operations."""
    pass


class ValidationError(HelioDataError):
    """Exception raised when data validation fails."""
    pass


class ConfigurationError(HelioDataError):
    """Exception raised for configuration-related errors.""" 
    pass


class NetworkError(DownloadError):
    """Exception raised for network-related download failures."""
    pass


class DataNotFoundError(DownloadError):
    """Exception raised when requested data is not available."""
    pass