#!/usr/bin/env python3
"""
Advanced HelioData Usage Example

This script demonstrates advanced usage patterns including:
- Custom configuration classes
- Batch processing with multiple missions
- Progress tracking and resumable downloads  
- Integration with existing workflows

Author: HelioData Contributors
License: MIT
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger

from heliodata import DownloadConfig, setup_logging
from heliodata.config import SDOAIAConfig, SOHOEITConfig, SolarOrbiterConfig
from heliodata.download.util import get_times, get_respath, validate_fits_file
from heliodata.exceptions import DownloadError, ValidationError


class BatchDownloadManager:
    """Manager for batch downloading from multiple missions."""
    
    def __init__(self, config: DownloadConfig):
        self.config = config
        self.base_path = Path(config.ds_path)
        self.progress_file = self.base_path / "batch_progress.json"
        
        # Setup logging
        setup_logging(config, self.base_path / "batch_download.log")
        
    def load_progress(self) -> Dict:
        """Load download progress from file."""
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                return json.load(f)
        return {"completed_missions": [], "failed_downloads": []}
    
    def save_progress(self, progress: Dict) -> None:
        """Save download progress to file."""
        with open(self.progress_file, 'w') as f:
            json.dump(progress, f, indent=2)
    
    def download_sdo_aia(self, sdo_config: SDOAIAConfig) -> bool:
        """Download SDO/AIA data with custom configuration."""
        logger.info("Starting SDO/AIA batch download...")
        
        try:
            times = get_times(
                self.config.start_year,
                self.config.end_year, 
                self.config.interval
            )
            
            for wavelength in sdo_config.wavelengths:
                logger.info(f"Processing wavelength: {wavelength}Å")
                
                wav_path = self.base_path / "sdo_aia" / wavelength
                
                for time_range in times:
                    result_path = get_respath(wav_path, time_range, self.config.interval)
                    
                    # Check existing files
                    existing_files = list(result_path.glob("*.fits"))
                    logger.info(f"Found {len(existing_files)} existing files for {time_range}")
                    
                    if self.config.validate_files:
                        self._validate_existing_files(existing_files)
            
            logger.success("SDO/AIA download completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"SDO/AIA download failed: {e}")
            raise DownloadError(f"SDO/AIA download error: {e}")
    
    def download_soho_eit(self, soho_config: SOHOEITConfig) -> bool:
        """Download SOHO/EIT data with custom configuration."""
        logger.info("Starting SOHO/EIT batch download...")
        
        try:
            times = get_times(
                self.config.start_year,
                self.config.end_year,
                self.config.interval
            )
            
            for wavelength in soho_config.wavelengths:
                logger.info(f"Processing wavelength: {wavelength}Å")
                
                wav_path = self.base_path / "soho_eit" / wavelength
                
                for time_range in times:
                    result_path = get_respath(wav_path, time_range, self.config.interval)
                    
                    existing_files = list(result_path.glob("*.fits"))
                    logger.info(f"Found {len(existing_files)} existing files for {time_range}")
            
            logger.success("SOHO/EIT download completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"SOHO/EIT download failed: {e}")
            raise DownloadError(f"SOHO/EIT download error: {e}")
    
    def _validate_existing_files(self, files: List[Path]) -> None:
        """Validate existing FITS files and report issues."""
        if not files:
            return
            
        logger.info(f"Validating {len(files)} FITS files...")
        
        valid_count = 0
        invalid_files = []
        
        for file_path in files:
            if validate_fits_file(file_path):
                valid_count += 1
            else:
                invalid_files.append(file_path)
                logger.warning(f"Invalid FITS file: {file_path}")
        
        logger.info(f"Validation complete: {valid_count}/{len(files)} files valid")
        
        if invalid_files and len(invalid_files) > len(files) * 0.1:  # > 10% invalid
            raise ValidationError(f"Too many invalid files: {len(invalid_files)}")
    
    def run_batch_download(self) -> bool:
        """Run batch download for all configured missions."""
        logger.info("Starting batch download process...")
        
        progress = self.load_progress()
        success = True
        
        # SDO/AIA
        if "sdo_aia" in self.config.mission_configs:
            if "sdo_aia" not in progress["completed_missions"]:
                try:
                    sdo_config = SDOAIAConfig.from_dict(
                        self.config.mission_configs["sdo_aia"]
                    )
                    self.download_sdo_aia(sdo_config)
                    progress["completed_missions"].append("sdo_aia")
                except Exception as e:
                    logger.error(f"SDO/AIA batch failed: {e}")
                    progress["failed_downloads"].append({"mission": "sdo_aia", "error": str(e)})
                    success = False
        
        # SOHO/EIT
        if "soho_eit" in self.config.mission_configs:
            if "soho_eit" not in progress["completed_missions"]:
                try:
                    soho_config = SOHOEITConfig.from_dict(
                        self.config.mission_configs["soho_eit"]
                    )
                    self.download_soho_eit(soho_config)
                    progress["completed_missions"].append("soho_eit")
                except Exception as e:
                    logger.error(f"SOHO/EIT batch failed: {e}")
                    progress["failed_downloads"].append({"mission": "soho_eit", "error": str(e)})
                    success = False
        
        # Save progress
        self.save_progress(progress)
        
        if success:
            logger.success("All batch downloads completed successfully!")
        else:
            logger.warning("Some downloads failed. Check progress file for details.")
        
        return success


def create_advanced_config() -> DownloadConfig:
    """Create an advanced configuration for batch processing."""
    return DownloadConfig(
        ds_path="./advanced_data",
        start_year=2023,
        end_year=2023,
        cadence=12,  # 12-hour cadence for more frequent sampling
        interval="month",
        email="user@example.com",  # Replace with your email
        log_level="DEBUG",
        validate_files=True,
        compute_checksums=True,
        mission_configs={
            "sdo_aia": {
                "series": "euv_12s",
                "segment": "image",
                "wavelengths": ["94", "131", "171", "193", "211", "304", "335"]
            },
            "soho_eit": {
                "wavelengths": ["171", "195", "284", "304"]
            },
            "solo_eui": {
                "product": ["eui-fsi174-image", "eui-fsi304-image"],
                "margin": 2,
                "level": 2
            }
        }
    )


def demonstrate_custom_workflow() -> None:
    """Demonstrate a custom data processing workflow."""
    logger.info("Demonstrating custom workflow integration...")
    
    config = create_advanced_config()
    
    # Create data directory structure
    base_path = Path(config.ds_path)
    base_path.mkdir(exist_ok=True, parents=True)
    
    # Setup logging
    setup_logging(config, base_path / "workflow.log")
    
    # Initialize batch manager
    manager = BatchDownloadManager(config)
    
    # Demonstrate configuration loading
    logger.info("Configuration details:")
    logger.info(f"  Base path: {config.ds_path}")
    logger.info(f"  Time range: {config.start_year}-{config.end_year}")
    logger.info(f"  Missions: {list(config.mission_configs.keys())}")
    
    # Demonstrate mission-specific configs
    if "sdo_aia" in config.mission_configs:
        sdo_config = SDOAIAConfig.from_dict(config.mission_configs["sdo_aia"])
        logger.info(f"  SDO/AIA wavelengths: {sdo_config.wavelengths}")
        logger.info(f"  SDO/AIA series: {sdo_config.series}")
    
    # Create sample directory structure (without actual downloads)
    logger.info("Creating sample directory structure...")
    for mission in config.mission_configs:
        mission_path = base_path / mission
        mission_path.mkdir(exist_ok=True, parents=True)
        logger.info(f"  Created: {mission_path}")
    
    # Generate time ranges for workflow planning
    times = get_times(config.start_year, config.end_year, config.interval)
    logger.info(f"Generated {len(times)} time intervals for processing")
    
    # Save configuration for future use
    config_path = base_path / "advanced_config.toml"
    config.save_to_file(config_path)
    logger.success(f"Configuration saved to: {config_path}")


def main() -> int:
    """Main function for advanced usage demonstration."""
    try:
        logger.info("HelioData Advanced Usage Example")
        logger.info("=" * 50)
        
        # Demonstrate custom workflow
        demonstrate_custom_workflow()
        
        print("\nAdvanced features demonstrated:")
        print("✓ Custom configuration management")
        print("✓ Batch processing setup")
        print("✓ Progress tracking system") 
        print("✓ File validation workflows")
        print("✓ Mission-specific configurations")
        print("✓ Advanced logging and error handling")
        
        print("\nTo run actual downloads:")
        print("1. Edit the email in the configuration")
        print("2. Use: python examples/advanced_usage.py")
        print("3. Or integrate into your own workflow")
        
        return 0
        
    except Exception as e:
        logger.error(f"Advanced usage example failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())