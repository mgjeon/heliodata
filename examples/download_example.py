#!/usr/bin/env python3
"""
HelioData Download Example Script

This script demonstrates the modern features of the upgraded HelioData package,
including configuration management, type-safe operations, error handling,
and data validation.

Author: Mingyu Jeon
License: MIT
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

from loguru import logger

import heliodata
from heliodata import (
    DownloadConfig,
    get_times, 
    get_respath,
    validate_fits_file,
    get_file_checksum,
    setup_logging,
)
from heliodata.exceptions import HelioDataError, ConfigurationError


def create_sample_config(config_path: Path) -> DownloadConfig:
    """Create and save a sample configuration."""
    logger.info("Creating sample configuration...")
    
    config = DownloadConfig(
        ds_path="./data",
        start_year=2023,
        end_year=2023,
        cadence=24,
        interval="month",
        email="user@example.com",  # Replace with your email
        log_level="INFO",
        validate_files=True,
        compute_checksums=True,
        mission_configs={
            "sdo_aia": {
                "series": "euv_12s",
                "wavelengths": ["171", "193", "211"]  # Subset for quick testing
            },
            "soho_eit": {
                "wavelengths": ["171", "195"]  # Subset for testing
            }
        }
    )
    
    config.save_to_file(config_path)
    logger.success(f"Configuration saved to: {config_path}")
    return config


def demonstrate_time_utilities(config: DownloadConfig) -> List:
    """Demonstrate the upgraded time utilities with type safety."""
    logger.info("Demonstrating time utilities...")
    
    try:
        # Generate time ranges with proper type hints
        times = get_times(
            start_year=config.start_year,
            end_year=config.end_year, 
            interval=config.interval  # type: Literal['year', 'month']
        )
        
        logger.info(f"Generated {len(times)} time ranges for {config.interval} intervals")
        
        # Display first few time ranges
        for i, time_range in enumerate(times[:3]):
            logger.info(f"  Range {i+1}: {time_range}")
            
        return times
        
    except ValueError as e:
        logger.error(f"Invalid time range parameters: {e}")
        raise


def demonstrate_path_management(config: DownloadConfig, times: List) -> None:
    """Demonstrate path management with validation."""
    logger.info("Demonstrating path management...")
    
    base_path = Path(config.ds_path)
    wavelengths = config.mission_configs.get("sdo_aia", {}).get("wavelengths", ["171"])
    
    for wavelength in wavelengths[:2]:  # Test with first two wavelengths
        wav_path = base_path / "sdo_aia" / wavelength
        
        for time_range in times[:2]:  # Test with first two time ranges
            try:
                result_path = get_respath(wav_path, time_range, config.interval)
                logger.info(f"  Path for {wavelength}Å, {time_range}: {result_path}")
                
                # Demonstrate path validation
                if result_path.exists():
                    logger.info(f"    Directory exists with {len(list(result_path.glob('*')))} files")
                else:
                    logger.info(f"    Directory created: {result_path}")
                    
            except ValueError as e:
                logger.error(f"Path creation error: {e}")


def demonstrate_file_validation(data_dir: Path) -> None:
    """Demonstrate file validation and checksum features."""
    logger.info("Demonstrating file validation...")
    
    # Find FITS files to validate
    fits_files = list(data_dir.glob("**/*.fits"))
    
    if not fits_files:
        logger.warning("No FITS files found for validation demo")
        logger.info("Creating a dummy file for demonstration...")
        
        # Create a test directory and dummy file
        test_dir = data_dir / "test"
        test_dir.mkdir(exist_ok=True, parents=True)
        dummy_file = test_dir / "dummy.fits"
        dummy_file.write_text("DUMMY FITS FILE CONTENT")
        fits_files = [dummy_file]
    
    for fits_file in fits_files[:3]:  # Validate first 3 files
        logger.info(f"  Validating: {fits_file.name}")
        
        # Validate file (this will fail for dummy files, but demonstrates the feature)
        is_valid = validate_fits_file(fits_file)
        logger.info(f"    Valid FITS: {is_valid}")
        
        # Calculate checksum
        try:
            checksum = get_file_checksum(fits_file)
            logger.info(f"    SHA256: {checksum[:16]}...")  # Show first 16 chars
        except Exception as e:
            logger.error(f"    Checksum error: {e}")


def demonstrate_error_handling() -> None:
    """Demonstrate the custom exception hierarchy."""
    logger.info("Demonstrating error handling...")
    
    # Test configuration error
    try:
        get_times(2025, 2020, "year")  # Invalid: start > end
    except ValueError as e:
        logger.warning(f"Caught expected ValueError: {e}")
    
    # Test invalid interval
    try:
        get_times(2020, 2021, "invalid_interval")  # Invalid interval
    except ValueError as e:
        logger.warning(f"Caught expected ValueError: {e}")
    
    # Demonstrate custom exception usage
    try:
        raise ConfigurationError("This is a configuration error example")
    except HelioDataError as e:
        logger.warning(f"Caught HelioDataError: {e}")


def demonstrate_logging_features(config: DownloadConfig) -> None:
    """Demonstrate advanced logging features."""
    logger.info("Demonstrating logging features...")
    
    # Setup structured logging
    data_path = Path(config.ds_path)
    data_path.mkdir(exist_ok=True, parents=True)
    
    setup_logging(config, data_path / "example.log")
    
    # Log at different levels
    logger.debug("This is a debug message")
    logger.info("This is an info message") 
    logger.warning("This is a warning message")
    logger.success("This demonstrates success logging")
    
    logger.info(f"Logs are written to: {data_path / 'example.log'}")


def main() -> int:
    """Main function demonstrating HelioData features."""
    try:
        # Print package info
        print(f"HelioData Example Script")
        print(f"Package Version: {heliodata.__version__}")
        print(f"Author: {heliodata.__author__}")
        print("-" * 50)
        
        # Create configuration
        config_path = Path("example_config.toml")
        config = create_sample_config(config_path)
        
        # Setup logging first
        demonstrate_logging_features(config)
        
        # Demonstrate time utilities
        times = demonstrate_time_utilities(config)
        
        # Demonstrate path management
        demonstrate_path_management(config, times)
        
        # Demonstrate file operations
        data_dir = Path(config.ds_path)
        demonstrate_file_validation(data_dir)
        
        # Demonstrate error handling
        demonstrate_error_handling()
        
        logger.success("All demonstrations completed successfully!")
        
        # Cleanup suggestions
        print("\nNext steps:")
        print("1. Edit the configuration file with your actual email and data paths")
        print("2. Use the CLI: heliodata download sdo-aia --config example_config.toml")
        print("3. Or use the Python modules directly with the new configuration system")
        
        return 0
        
    except HelioDataError as e:
        logger.error(f"HelioData error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())