from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List, Literal

from sunpy.net import attrs as a


IntervalType = Literal['year', 'month']


def get_times(start_year: int, end_year: int, interval: IntervalType) -> List[a.Time]:
    """
    Generate a list of time ranges based on the specified interval.
    
    Args:
        start_year: Starting year (inclusive)
        end_year: Ending year (inclusive)  
        interval: Time interval granularity ('year' or 'month')
        
    Returns:
        List of sunpy Time attribute objects representing time ranges
        
    Raises:
        ValueError: If interval is not 'year' or 'month'
        ValueError: If start_year > end_year
    """
    if interval not in ('year', 'month'):
        raise ValueError(f"interval must be 'year' or 'month', got '{interval}'")
    
    if start_year > end_year:
        raise ValueError(f"start_year ({start_year}) must be <= end_year ({end_year})")
    
    times = []
    
    if interval == 'year':
        for year in range(start_year, end_year + 1):
            times.append(a.Time(f'{year}-01-01T00:00:00', f'{year}-12-31T23:59:59'))
    
    elif interval == 'month':
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                dt = datetime(year, month, 1)
                if month < 12:
                    dtn = datetime(year, month + 1, 1)
                else:
                    dtn = datetime(year + 1, 1, 1)
                
                tr = a.Time(dt.strftime('%Y-%m-%dT%H:%M:%S'), dtn.strftime('%Y-%m-%dT%H:%M:%S'))
                times.append(tr)
    
    return times


def get_respath(resroot: Path, tr: a.Time, interval: IntervalType) -> Path:
    """
    Get the result path based on the time range and interval.
    
    Args:
        resroot: Root directory for results
        tr: Time range attribute object
        interval: Time interval granularity ('year' or 'month')
        
    Returns:
        Path object for the organized result directory
        
    Raises:
        ValueError: If interval is not 'year' or 'month'
    """
    if interval not in ('year', 'month'):
        raise ValueError(f"interval must be 'year' or 'month', got '{interval}'")
    
    if interval == 'year':
        respath = resroot / str(tr.start.datetime.year)
    elif interval == 'month':
        respath = resroot / str(tr.start.datetime.year) / f"{tr.start.datetime.month:02d}"
    
    respath.mkdir(exist_ok=True, parents=True)
    return respath


def validate_fits_file(filepath: Path) -> bool:
    """
    Validate that a FITS file is not corrupted.
    
    Args:
        filepath: Path to the FITS file
        
    Returns:
        True if file is valid, False otherwise
    """
    try:
        from astropy.io import fits
        with fits.open(filepath) as hdul:
            # Try to access the header to ensure file is readable
            _ = hdul[0].header
            return True
    except Exception:
        return False


def get_file_checksum(filepath: Path) -> str:
    """
    Calculate SHA256 checksum of a file.
    
    Args:
        filepath: Path to the file
        
    Returns:
        Hexadecimal string representation of SHA256 hash
    """
    import hashlib
    
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()