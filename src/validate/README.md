# Validation Module

This module provides comprehensive validation tools to verify that the insurance policy system is working correctly.

## Validation Scripts

### Core Validation
- **`validate_assignment_premiums.py`** - Comprehensive reconciliation between assignment storage and inforce calculations
- **`validate_premium_calculations.py`** - Validates premium calculation logic and consistency

### Testing Scripts
- **`test_assignment_premiums.py`** - Tests the assignment premium storage functionality
- **`test_policy2_fix.py`** - Tests vehicle substitution fixes and premium calculations
- **`test_premium_trace.py`** - Traces detailed premium calculation steps

### Analysis Scripts
- **`analyze_exposure_factor.py`** - Analyzes exposure factor calculations
- **`demo_pyspark.py`** - Demonstrates PySpark functionality

## Usage

### Run All Validations
```bash
# Activate environment
conda activate retention

# Run comprehensive validation
python -m src.validate.validate_assignment_premiums

# Run premium calculation validation
python -m src.validate.validate_premium_calculations
```

### Run Individual Tests
```bash
# Test assignment premiums
python -m src.validate.test_assignment_premiums

# Test specific policy fixes
python -m src.validate.test_policy2_fix

# Trace premium calculations
python -m src.validate.test_premium_trace
```

## Validation Standards

### Perfect Reconciliation Target
- **0.0% difference** between assignment storage and inforce calculations
- **100% of policies** must achieve perfect reconciliation
- **All months** must show consistent results

### Validation Output
- Validation results are saved to `output/` directory
- Reports include detailed breakdowns by policy and month
- Any failures are clearly flagged with specific error details

## Integration with Pipeline

The validation module is designed to be integrated as a critical step in the data processing pipeline:

1. **Build Initial State** → 2. **Apply Changes** → 3. **Generate Inforce** → 4. **Validate Results**

This ensures that any processing errors are caught immediately and the system maintains perfect reconciliation.
