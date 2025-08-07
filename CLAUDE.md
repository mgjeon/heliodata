# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

HelioData is a Python package for downloading heliosphere-related data from various solar and space weather missions. The project focuses on data acquisition from instruments like SDO/AIA, SOHO/EIT, Solar Orbiter/EUI, and STEREO/SECCHI.

## Architecture

### Core Structure
- `src/heliodata/download/`: Contains mission-specific download modules
- `scripts/download/`: Shell scripts providing convenient wrappers for common download tasks
- `scripts/download_alt/`: Alternative download methods using different APIs

### Download Modules
Each mission has its own download module following a consistent pattern:
- **SDO/AIA** (`sdo_aia.py`): Downloads from JSOC with configurable series (euv_12s, uv_24s, vis_1h)
- **SOHO/EIT** (`soho_eit.py`): Downloads from SDAC via VSO
- **Solar Orbiter/EUI** (`solo_eui.py`): Downloads from SOAR using sunpy-soar
- **SDO/HMI** (`sdo_hmi.py`): Magnetogram data from JSOC
- **Other missions**: stereo_secchi_euvi.py, soho_mdi.py, solo_phi.py

### Common Utilities
- `util.py`: Shared functions for time range generation and result path management
  - `get_times()`: Generates time ranges by year or month intervals
  - `get_respath()`: Creates organized directory structure for downloaded data

## Development Commands

### Installation and Environment
```bash
# Install in development mode
pip install -e .

# Install with development dependencies
pip install -e . --dependency-groups dev

# Install with test dependencies
pip install -e . --dependency-groups test

# Install with documentation dependencies  
pip install -e . --dependency-groups docs
```

### Code Quality Tools
```bash
# Format code with black
black src/

# Lint with ruff
ruff check src/

# Type checking with mypy
mypy src/

# Run tests with pytest
pytest

# Run tests with coverage
pytest --cov=heliodata --cov-report=html

# Install pre-commit hooks
pre-commit install
```

### Using the CLI
The modern way to use HelioData is through the CLI:
```bash
# Create default configuration
heliodata config init

# Show current configuration
heliodata config show

# Download SDO/AIA data
heliodata download sdo-aia --ds-path /path/to/data --email your@email.com

# Download SOHO/EIT data  
heliodata download soho-eit --ds-path /path/to/data
```

### Configuration Management
Use `heliodata.toml` for centralized configuration:
```toml
[download]
ds_path = "/path/to/data"
email = "your@email.com"
start_year = 2020
end_year = 2024
log_level = "INFO"
validate_files = true

[sdo_aia]
series = "euv_12s"
wavelengths = ["094", "131", "171", "193", "211", "304", "335"]
```

### Running Download Scripts (Legacy)
Each download module can still be run as a Python module:
```bash
python -m heliodata.download.sdo_aia --ds_path /path/to/data --email your@email.com --series euv_12s --wavelengths "094,131,171,193,211,304,335"
python -m heliodata.download.soho_eit --ds_path /path/to/data --wavelengths "171,195,284,304"
python -m heliodata.download.solo_eui --ds_path /path/to/data --product "eui-fsi174-image,eui-fsi304-image"
```

### Using Convenience Scripts  
The `scripts/download/` directory contains pre-configured shell scripts:
```bash
./scripts/download/sdo_aia_euv.sh
./scripts/download/soho_eit.sh
./scripts/download/solo_eui_fsi.sh
```

## Key Dependencies

- **SunPy**: Primary library for solar data access and manipulation
- **sunpy-soar**: Extension for Solar Orbiter Archive data access
- **Astropy**: Astronomical data structures and units
- **Loguru**: Logging framework
- **Pandas**: Used in Solar Orbiter data processing for time matching

## Example Scripts

The `examples/` directory contains comprehensive example scripts:

### Basic Usage (`examples/download_example.py`)
Demonstrates core features:
- Type-safe configuration management
- Time range generation with validation
- Path management utilities
- File validation and checksum calculation
- Custom exception handling
- Advanced logging features

```bash
cd examples/
python download_example.py
```

### Advanced Usage (`examples/advanced_usage.py`)
Shows production patterns:
- Batch processing multiple missions
- Progress tracking and resumable operations
- Custom configuration classes
- File validation workflows
- Integration with existing data pipelines

```bash
cd examples/
python advanced_usage.py
```

### Example Configuration Files
Both scripts generate sample configurations you can customize:
- `example_config.toml` - Basic configuration
- `advanced_data/advanced_config.toml` - Multi-mission batch setup

## Data Organization

Downloads are organized hierarchically:
- By wavelength or product type
- By time interval (year/month configurable)
- With progress tracking via `info.json` files
- Automatic directory creation and FITS file detection

## Common Patterns

### Progress Tracking
All modules use JSON-based progress tracking to resume interrupted downloads and avoid duplicate fetching.

### Time Range Handling
Consistent time range processing supports both yearly and monthly intervals with configurable cadence.

### Error Handling
Modules include robust error handling for missing data periods and network issues.

### Data Providers
- **JSOC**: Joint Science Operations Center (SDO data)
- **SDAC**: Solar Data Analysis Center (SOHO data)
- **SOAR**: Solar Orbiter Archive (Solar Orbiter data)
- **VSO**: Virtual Solar Observatory (general interface)