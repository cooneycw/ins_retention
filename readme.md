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

This project simulates an insurance policy system using a **family-centric approach** with **driver-vehicle assignments**. The system creates ~1,200 inforce policies by end of 2018 and simulates 7+ years of data with realistic family compositions and insurance industry-standard driver assignment rules.

### Current Status (Latest Updates)
- ✅ **Policy numbering is sequential by creation date** - earliest policies (January 2018) have lowest policy numbers
- ✅ **Inforce represents end-of-month status** - policies appear from their start date onwards
- ✅ **Historical assignment tracking** - proper handling of vehicle substitutions and coverage gaps
- ✅ **Family-centric data architecture** - families drive policy composition and driver assignments
- ✅ **Configurable family types** - easy switching between single person, couples, and families with teens
- ✅ **Realistic vehicle change rates** - scaled back additions to maintain realistic multi-vehicle percentages

## Key Features

### Family-Centric Architecture
- **Family composition drives everything** - determine family structure first
- **Driver-vehicle assignments create rated records** - each combination gets its own rating
- **Configurable family types** - single person, couples, families with teens
- **Realistic insurance industry approach** - matches real-world rating practices

### Driver Assignment Rules
- **Every vehicle must have exactly 1 primary driver** (required)
- **Each vehicle can have up to 1 secondary adult + 1 teen** (optional)
- **Maximum: 3 drivers per vehicle** (1 primary + 1 secondary adult + 1 teen)
- **All drivers must be assigned to at least 1 vehicle**
- **No unassigned drivers or vehicles**

### Exposure Allocation
- **Primary solo**: 1.0 exposure
- **Primary shared**: 0.6 exposure (when secondary present)
- **Secondary adult**: 0.5 exposure
- **Teen secondary**: 0.35 exposure
- **Teen primary**: 1.0 exposure
- **Unassigned driver**: 0.0 exposure

## Project Structure

```
Retention/
├── main.py                           # Main entry point
├── readme.md                        # Project documentation
├── .cursor/
│   └── rules/
│       ├── python.mdc               # Python coding standards
│       └── project.mdc              # Project-specific rules
├── config/
│   ├── policy_system_config.yaml    # Main system configuration
│   └── family_type_presets.yaml     # Family type preset configurations
├── src/                             # Source code package
│   ├── __init__.py                  # Package initialization
│   ├── create/                      # Create module (Stage 1)
│   │   ├── __init__.py              # Module initialization
│   │   ├── build_initial_state.py   # Creates families with assignments
│   │   ├── apply_changes.py         # Simulates monthly changes
│   │   └── generate_inforce.py      # Builds final inforce view
│   ├── maintain/                    # Maintenance utilities
│   │   ├── __init__.py              # Module initialization
│   │   └── family_type_manager.py   # Family type configuration management
│   └── report/                      # Reporting and analysis
│       ├── __init__.py              # Module initialization
│       └── distribution_analysis.py # Distribution analysis and reporting
├── data/
│   ├── policy_system/               # Master data files
│   │   ├── families_master.csv
│   │   ├── policies_master.csv
│   │   ├── vehicles_master.csv
│   │   ├── drivers_master.csv
│   │   ├── driver_vehicle_assignments.csv
│   │   └── policy_changes_log.csv
│   └── inforce/                     # Monthly inforce data
│       └── inforce_monthly_view.csv
└── output/                          # Reports and analysis
    └── monthly_distribution_summary.csv
```

### Module Overview

- **main.py**: Entry point for the insurance policy system
- **src/create/**: Core module for policy data generation
- **src/maintain/**: Configuration management utilities
- **src/report/**: Distribution analysis and reporting
- **config/**: YAML configuration files

## Family Type Configuration

### Available Family Types
- **Single Person**: 1 adult, 0 teens, 1+ vehicles
- **Couple**: 2 adults, 0 teens, 1-2 vehicles  
- **Family with Teens**: 2 adults, 1-2 teens, 1-3 vehicles

### Configuration Management
The system uses YAML-based configuration to control family types and their evolution rules:

```yaml
family_types:
  single_person:
    percentage: 1.0  # Set to 1.0 for single person only, 0.0 to disable
    can_add_adults: false      # Cannot marry/couple
    can_add_teens: false       # Cannot have children
    can_add_vehicles: true     # Can buy additional vehicles
    max_vehicles: 3            # Maximum vehicles for this family type
```

### Switching Family Types
Use the maintenance utilities to switch between different family type configurations:

```bash
# Show current configuration
python -m src.maintain.family_type_manager

# Switch to single person only
python -m src.maintain.family_type_manager single_person_only

# Switch to mixed families
python -m src.maintain.family_type_manager mixed_families
```

### Available Presets
- **single_person_only**: All families are single person (simple mode)
- **mixed_families**: Realistic mix of single person, couples, families with teens
- **couples_only**: All families are couples
- **families_with_teens_only**: All families have teens

## System Output

The system generates comprehensive data including:

### Master Data Files
- **families_master.csv**: Family composition and rules data
- **policies_master.csv**: Policy master data (references families)
- **vehicles_master.csv**: Vehicle master data with VINs and effective dates
- **drivers_master.csv**: Driver master data with license numbers and birthdays
- **driver_vehicle_assignments.csv**: Driver-vehicle assignments with exposure factors

### Final Output
- **inforce_monthly_view.csv**: Monthly snapshot with driver-vehicle assignment records
- **monthly_distribution_summary.csv**: Monthly statistics and distributions

### Data Features
- Policy information with expiry dates and client tenure tracking
- Vehicle data with VINs, types, and model years (2001-2018 range)
- Driver data with license numbers, names, and assignment types
- Sophisticated premium calculations based on driver-vehicle assignments
- Monthly inforce status tracking (end-of-month status as of last day of each month)
- **Sorted by**: policy, policy_expiry_date, vehicle_no, driver_no, inforce_yy, inforce_mm
- **Policy numbering**: Sequential by creation date (Policy 1 = earliest created policy)
- **Historical tracking**: Proper handling of vehicle substitutions, removals, and coverage gaps

## Data Features

### Family Composition (Configurable)
- **Single person families**: 1 adult, 0 teens, 1+ vehicles
- **Couple families**: 2 adults, 0 teens, 1-2 vehicles
- **Families with teens**: 2 adults, 1-2 teens, 1-3 vehicles

### Vehicle Management
- 10 vehicle types with realistic distribution
- Model year tracking for aging (17-year maximum age)
- VIN generation with proper format
- Vehicle changes: additions, removals, substitutions (configurable per family type)

### Driver Management
- Driver type classification (adult, teen)
- Age-appropriate driver assignment
- Birthday tracking for aging calculations
- Driver-vehicle assignments following insurance industry standards

### Premium Calculation
- Base premium calculations with exposure factors
- Driver age and type considerations
- Vehicle type factors
- Multi-vehicle discounts
- Annual inflation adjustments

## Dependencies

### Required Python Packages
- **PyYAML**: For configuration file management
- **pandas**: For data analysis and reporting
- **Standard Library**: csv, random, datetime, pathlib, typing

### Installation
```bash
# Activate conda environment
conda activate retention

# Install required packages
conda install pandas
pip install PyYAML
```

## Usage Examples

### Running the Complete System
```bash
# Activate environment
conda activate retention

# Run complete system
python main.py
```

### Running Individual Steps
```bash
# Create initial state
python -c "from src.create.build_initial_state import create_initial_state; create_initial_state()"

# Apply monthly changes
python -c "from src.create.apply_changes import simulate_monthly_changes; simulate_monthly_changes()"

# Generate inforce view
python -c "from src.create.generate_inforce import create_inforce_view; create_inforce_view()"

# Run distribution analysis
python -c "from src.report.distribution_analysis import analyze_inforce_distributions; analyze_inforce_distributions()"
```

### Configuration Management
```bash
# Show current family type configuration
python -m src.maintain.family_type_manager

# Switch to single person families only
python -m src.maintain.family_type_manager single_person_only

# Switch to mixed families
python -m src.maintain.family_type_manager mixed_families
```

## Output File Management

- **Fixed naming convention**: Use consistent, descriptive names for all output files
- **No temporary files**: All generated files should have permanent, meaningful names
- **Reusable reports**: Use the same file names for iterative analysis to avoid clutter
- **Clean output directory**: Remove temporary or test files after validation

### Standard Report Names
- `monthly_distribution_summary.csv` - Monthly inforce statistics and distributions
- `inforce_monthly_view.csv` - Complete monthly inforce data with driver-vehicle assignments