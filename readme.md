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

The system generates a final CSV file `inforce_monthly_view.csv` with 84 months of data containing:
- Policy information with expiry dates
- Vehicle data with VINs
- Driver data with license numbers
- Premium amounts with annual inflation
- Monthly inforce status tracking

## Dependencies

*[Add your project dependencies here as they are installed]*
