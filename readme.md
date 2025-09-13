# Insurance Policy System

## Environment Setup

This project uses a conda environment with Python 3.12. Before running, testing, or building the code, you must activate the conda environment.

### Activate the Environment

```bash
conda activate retention
```

### Deactivate the Environment

```bash
conda deactivate
```

### Environment Details

- **Environment Name**: retention
- **Python Version**: 3.12.11
- **Location**: `/home/cooneycw/miniconda3/envs/retention`

## Usage Instructions

**Important**: Always activate the conda environment before executing any Python commands:

```bash
# Activate environment
conda activate retention

# Now you can run Python commands
python main.py
python -m pytest  # for testing
python setup.py build  # for building
```

## Project Overview

This project simulates an insurance policy system with 1200 policies over 7 years (84 months) of data. The system generates realistic policy, vehicle, and driver data with proper lifecycle management including renewals, vehicle replacements, and driver changes.

## Project Structure

```
Retention/
├── main.py                           # Main entry point
├── readme.md                        # Project documentation
├── .cursor/
│   └── rules/
│       ├── python.mdc               # Python coding standards
│       └── project.mdc              # Project-specific rules
└── src/                             # Source code package
    ├── __init__.py                  # Package initialization
    └── create/                      # Create module (Stage 1)
        ├── __init__.py              # Module initialization
        ├── data/                    # Data files directory
        ├── build_initial_state.py   # Creates initial 1200 policies
        ├── apply_changes.py         # Simulates monthly changes
        └── generate_inforce.py      # Builds final inforce view
```

### Module Overview

- **main.py**: Entry point for the insurance policy system
- **src/create/**: Core module for policy data generation
- **src/create/data/**: Directory for internal data files
- **src/create/build_initial_state.py**: Creates original policy data
- **src/create/apply_changes.py**: Simulates policy lifecycle changes
- **src/create/generate_inforce.py**: Generates final monthly inforce view

## System Output

The system generates a final CSV file `inforce_monthly_view.csv` with 90 months of data (Jan 2018 - Jun 2025) containing:
- Policy information with expiry dates and client tenure tracking
- Vehicle data with VINs, types, and model years (2001-2018 range)
- Driver data with license numbers and driver types (primary/spouse/secondary)
- Sophisticated premium calculations based on:
  * Driver age and type (young drivers pay more, secondary drivers pay less)
  * Vehicle type (sports cars cost more, sedans are base rate)
  * Multi-vehicle discounts (up to 20% for 4+ vehicles)
- Premium amounts with annual inflation
- Monthly inforce status tracking (last day of each month)
- **Sorted by**: policy, policy_expiry_date, vehicle_no, driver_no, inforce_yy, inforce_mm

## Data Features

### Family Composition
- Single person, single vehicle: 25%
- Single driver, multi-vehicle: 5%
- Family with 2+ vehicles: 50% (weighted: 2-car 50%, 3-car 35%, 4-car 15%)
- Family with single vehicle: 20%

### Vehicle Management
- 10 vehicle types with realistic distribution
- Model year tracking for aging (17-year maximum age)
- VIN generation with proper format

### Driver Management
- Driver type classification (primary, spouse, secondary)
- Age-appropriate driver assignment
- Birthday tracking for aging calculations

## Dependencies

### Required Python Packages
- **PyYAML**: For configuration file management
- **Standard Library**: csv, random, datetime, pathlib, typing

### Installation
```bash
# Activate conda environment
conda activate retention

# Install required packages
pip install PyYAML
```

## Project Structure

```
Retention/
├── config/
│   └── policy_system_config.yaml    # Core assumptions and parameters
├── data/
│   ├── policy_system/               # Master data files
│   │   ├── policies_master.csv
│   │   ├── vehicles_master.csv
│   │   ├── drivers_master.csv
│   │   └── policy_changes_log.csv
│   └── inforce/                     # Monthly inforce data
│       └── inforce_monthly_view.csv
├── output/                          # Reports and analysis (fixed naming)
├── src/create/                      # Source code modules
└── main.py                          # Main orchestrator

## Output File Management

- **Fixed naming convention**: Use consistent, descriptive names for all output files
- **No temporary files**: All generated files should have permanent, meaningful names
- **Reusable reports**: Use the same file names for iterative analysis to avoid clutter
- **Clean output directory**: Remove temporary or test files after validation

### Standard Report Names
- `retention_analysis.csv` - Main retention analysis
- `inforce_summary.csv` - Inforce counts by month
- `cohort_analysis.csv` - Cohort-based retention analysis
```
