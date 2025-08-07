"""Command Line Interface for HelioData."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

from loguru import logger

from heliodata.config import DownloadConfig, setup_logging
from heliodata.exceptions import HelioDataError


def create_parser() -> argparse.ArgumentParser:
    """Create the main CLI parser."""
    parser = argparse.ArgumentParser(
        prog="heliodata",
        description="HelioData: Download heliosphere data from various solar missions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--version", 
        action="version", 
        version="%(prog)s 0.1.1"
    )
    
    parser.add_argument(
        "-c", "--config",
        type=Path,
        help="Configuration file path (TOML format)",
        default="heliodata.toml"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (use -v, -vv, or -vvv)"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Config command
    config_parser = subparsers.add_parser(
        "config",
        help="Configuration management"
    )
    config_subparsers = config_parser.add_subparsers(dest="config_action")
    
    config_subparsers.add_parser(
        "init",
        help="Create default configuration file"
    )
    
    config_subparsers.add_parser(
        "show", 
        help="Show current configuration"
    )
    
    # Download commands
    download_parser = subparsers.add_parser(
        "download",
        help="Download data from various missions"
    )
    download_subparsers = download_parser.add_subparsers(dest="mission")
    
    # SDO/AIA
    sdo_parser = download_subparsers.add_parser("sdo-aia", help="Download SDO/AIA data")
    sdo_parser.add_argument("--ds-path", required=True, help="Download directory path")
    sdo_parser.add_argument("--email", required=True, help="Email for JSOC requests")
    sdo_parser.add_argument("--start-year", type=int, default=2010, help="Start year")
    sdo_parser.add_argument("--end-year", type=int, default=2024, help="End year")
    sdo_parser.add_argument("--series", default="euv_12s", choices=["euv_12s", "uv_24s", "vis_1h"])
    sdo_parser.add_argument("--wavelengths", default="094,131,171,193,211,304,335")
    
    # SOHO/EIT
    soho_parser = download_subparsers.add_parser("soho-eit", help="Download SOHO/EIT data")
    soho_parser.add_argument("--ds-path", required=True, help="Download directory path")
    soho_parser.add_argument("--start-year", type=int, default=2010, help="Start year")
    soho_parser.add_argument("--end-year", type=int, default=2024, help="End year")
    soho_parser.add_argument("--wavelengths", default="171,195,284,304")
    
    return parser


def handle_config_command(args: argparse.Namespace) -> int:
    """Handle configuration commands."""
    if args.config_action == "init":
        config_path = Path("heliodata.toml")
        if config_path.exists():
            print(f"Configuration file {config_path} already exists")
            return 1
        
        # Create default config
        config = DownloadConfig.from_env()
        config.save_to_file(config_path)
        print(f"Created configuration file: {config_path}")
        return 0
    
    elif args.config_action == "show":
        try:
            config = DownloadConfig.from_file(args.config)
            print(f"Configuration from {args.config}:")
            print(f"  Data path: {config.ds_path}")
            print(f"  Time range: {config.start_year}-{config.end_year}")
            print(f"  Email: {config.email}")
            print(f"  Log level: {config.log_level}")
            return 0
        except Exception as e:
            logger.error(f"Error reading configuration: {e}")
            return 1
    
    return 0


def handle_download_command(args: argparse.Namespace) -> int:
    """Handle download commands."""
    try:
        # Load configuration
        if Path(args.config).exists():
            config = DownloadConfig.from_file(args.config)
        else:
            config = DownloadConfig.from_env()
        
        # Override with command line arguments
        if hasattr(args, 'ds_path') and args.ds_path:
            config.ds_path = args.ds_path
        if hasattr(args, 'email') and args.email:
            config.email = args.email
        if hasattr(args, 'start_year'):
            config.start_year = args.start_year
        if hasattr(args, 'end_year'):
            config.end_year = args.end_year
            
        # Setup logging
        setup_logging(config, Path(config.ds_path) / "heliodata.log")
        
        if args.mission == "sdo-aia":
            from heliodata.download import sdo_aia
            return sdo_aia.main_cli(args, config)
        elif args.mission == "soho-eit":
            from heliodata.download import soho_eit  
            return soho_eit.main_cli(args, config)
        else:
            logger.error(f"Unknown mission: {args.mission}")
            return 1
            
    except HelioDataError as e:
        logger.error(f"HelioData error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def main(argv: Optional[list[str]] = None) -> int:
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args(argv)
    
    # Setup verbosity
    log_levels = ["WARNING", "INFO", "DEBUG"]
    verbosity = min(args.verbose, len(log_levels) - 1)
    
    if not args.command:
        parser.print_help()
        return 0
    
    if args.command == "config":
        return handle_config_command(args)
    elif args.command == "download":
        return handle_download_command(args) 
    
    return 0


if __name__ == "__main__":
    sys.exit(main())