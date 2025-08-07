"""Configuration management for HelioData downloads."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import toml
from loguru import logger


@dataclass
class DownloadConfig:
    """Configuration for data downloads."""
    
    # Common settings
    ds_path: str
    start_year: int = 2010
    end_year: int = 2024
    cadence: int = 24
    interval: str = 'year'
    ignore_info: bool = False
    
    # Email for JSOC requests
    email: Optional[str] = None
    
    # Logging settings
    log_level: str = 'INFO'
    log_file: Optional[str] = None
    
    # Data validation
    validate_files: bool = True
    compute_checksums: bool = False
    
    # Mission-specific settings
    mission_configs: Dict[str, Dict] = field(default_factory=dict)
    
    @classmethod
    def from_file(cls, config_path: Path) -> DownloadConfig:
        """Load configuration from TOML file."""
        try:
            with open(config_path, 'r') as f:
                config_data = toml.load(f)
            
            # Extract main config
            main_config = config_data.get('download', {})
            mission_configs = {k: v for k, v in config_data.items() if k != 'download'}
            
            return cls(mission_configs=mission_configs, **main_config)
        
        except FileNotFoundError:
            logger.warning(f"Config file {config_path} not found, using defaults")
            return cls(ds_path=os.getcwd())
        except Exception as e:
            logger.error(f"Error loading config from {config_path}: {e}")
            raise
    
    @classmethod
    def from_env(cls) -> DownloadConfig:
        """Load configuration from environment variables."""
        return cls(
            ds_path=os.getenv('HELIODATA_PATH', os.getcwd()),
            email=os.getenv('HELIODATA_EMAIL'),
            log_level=os.getenv('HELIODATA_LOG_LEVEL', 'INFO'),
            start_year=int(os.getenv('HELIODATA_START_YEAR', '2010')),
            end_year=int(os.getenv('HELIODATA_END_YEAR', '2024')),
            cadence=int(os.getenv('HELIODATA_CADENCE', '24')),
        )
    
    def save_to_file(self, config_path: Path) -> None:
        """Save configuration to TOML file."""
        config_data = {
            'download': {
                'ds_path': self.ds_path,
                'start_year': self.start_year,
                'end_year': self.end_year,
                'cadence': self.cadence,
                'interval': self.interval,
                'ignore_info': self.ignore_info,
                'email': self.email,
                'log_level': self.log_level,
                'log_file': self.log_file,
                'validate_files': self.validate_files,
                'compute_checksums': self.compute_checksums,
            }
        }
        config_data.update(self.mission_configs)
        
        with open(config_path, 'w') as f:
            toml.dump(config_data, f)


@dataclass 
class SDOAIAConfig:
    """Configuration specific to SDO/AIA downloads."""
    
    series: str = 'euv_12s'
    segment: str = 'image'
    wavelengths: List[str] = field(default_factory=lambda: ['094', '131', '171', '193', '211', '304', '335'])
    
    @classmethod
    def from_dict(cls, data: Dict) -> SDOAIAConfig:
        """Create config from dictionary."""
        return cls(**data)


@dataclass
class SOHOEITConfig:
    """Configuration specific to SOHO/EIT downloads."""
    
    wavelengths: List[str] = field(default_factory=lambda: ['171', '195', '284', '304'])
    
    @classmethod
    def from_dict(cls, data: Dict) -> SOHOEITConfig:
        """Create config from dictionary.""" 
        return cls(**data)


@dataclass
class SolarOrbiterConfig:
    """Configuration specific to Solar Orbiter downloads."""
    
    product: List[str] = field(default_factory=lambda: ['eui-fsi174-image', 'eui-fsi304-image'])
    margin: int = 1
    level: int = 2
    
    @classmethod
    def from_dict(cls, data: Dict) -> SolarOrbiterConfig:
        """Create config from dictionary."""
        return cls(**data)


def setup_logging(config: DownloadConfig, log_path: Optional[Path] = None) -> None:
    """Setup logging configuration."""
    logger.remove()  # Remove default handler
    
    # Console logging
    logger.add(
        lambda msg: print(msg, end=''),
        level=config.log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
    )
    
    # File logging
    if log_path or config.log_file:
        log_file = log_path or Path(config.log_file)
        logger.add(
            log_file,
            level=config.log_level,
            rotation="10 MB",
            retention="7 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}"
        )