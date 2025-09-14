# Policy Viewer - Visual Debugger

The Policy Viewer is a tkinter-based GUI application for exploring the inforce_monthly_view.csv data with major change indicators.

## Features

- **Policy Navigation**: Cycle through policies using Previous/Next buttons or enter policy numbers directly
- **Time Navigation**: Cycle through months using Previous/Next buttons or enter dates in YYYY-MM format
- **Three Information Tabs**:
  - **Policy Info**: Basic policy information and summary
  - **Driver Assignments**: Detailed driver-vehicle assignments in tabular format
  - **Major Changes**: Major change detection status and details

## Usage

**IMPORTANT**: Always activate the conda environment first:
```bash
conda activate retention
```

### From main.py
```bash
# Activate environment and launch the Policy Viewer GUI
conda activate retention
python main.py --testview

# Specify a different CSV file
conda activate retention
python main.py --testview --csv path/to/your/inforce_data.csv
```

### Standalone
```bash
# Activate environment and run the Policy Viewer directly
conda activate retention
python src/testview/policy_viewer.py

# With custom CSV file
conda activate retention
python src/testview/policy_viewer.py --csv data/inforce/inforce_monthly_view_sorted
```

## Navigation

### Policy Navigation
- **Previous Policy (← Prev)**: Go to the previous policy in the list
- **Next Policy (Next →)**: Go to the next policy in the list
- **Direct Entry**: Type a policy number and press Enter

### Time Navigation
- **Previous Month (← Prev Month)**: Go to the previous month
- **Next Month (Next Month →)**: Go to the next month
- **Direct Entry**: Type a date in YYYY-MM format and press Enter

## Status Indicators

- **✅ No Major Changes**: Green indicator when no major changes are detected
- **⚠️ MAJOR CHANGE DETECTED**: Red indicator when major changes are found
- **Record Count**: Shows the number of records for the current policy/time combination

## Requirements

- **Conda Environment**: `retention` conda environment (required)
- Python 3.7+
- tkinter (usually included with Python)
- pandas (installed in retention environment)
- PySpark (installed in retention environment for generating the CSV with major change data)
- Java 17 (required for PySpark 4.0.1 compatibility)

## Data Format

The Policy Viewer expects a CSV file with the following columns:
- `policy`: Policy identifier
- `inforce_yy`: Year
- `inforce_mm`: Month
- `vehicle_no`: Vehicle number
- `driver_no`: Driver number
- `driver_name`: Driver name
- `driver_age`: Driver age
- `assignment_type`: Driver assignment type
- `exposure_factor`: Exposure factor
- `major_change`: Boolean indicating major changes
- Additional policy information columns

## Troubleshooting

- **"CSV file not found"**: Ensure the CSV file exists at the specified path
- **"Policy not found"**: The entered policy number doesn't exist in the data
- **"Time not found"**: The entered date doesn't exist in the data
- **Import errors**: Ensure all required dependencies are installed
