# PySpark Data Processing Module

This module provides PySpark-based data processing capabilities for the retention analysis system, optimized for large-scale insurance data processing.

## Overview

The PySpark module includes:

- **Data Loader** (`data_loader.py`): Efficient loading and validation of CSV data
- **Base Analysis** (`base_analysis.py`): Foundational analysis functions and utilities
- **Distribution Analysis** (`distribution_analysis.py`): Comprehensive distribution analysis using PySpark

## Features

### Data Loading
- Schema-defined CSV loading with type safety
- Data validation and quality reporting
- Optimized Spark configuration for performance
- Automatic date parsing and type conversion

### Analysis Capabilities
- Policy distribution analysis
- Vehicle and driver statistics
- Temporal analysis (year/month trends)
- Premium analysis and statistics
- Retention metrics calculation
- Cohort analysis
- Data quality assessment

### Performance Optimizations
- Adaptive query execution
- Partition coalescing
- Kryo serialization
- DataFrame caching
- Efficient aggregations

## Usage

### Basic Usage

```python
from src.pyspark.distribution_analysis import DistributionAnalysis

# Initialize analysis
analysis = DistributionAnalysis()

# Perform comprehensive analysis
analysis.analyze_inforce_distributions()

# Generate reports
analysis.generate_distribution_report()

# Close session
analysis.close()
```

### Data Loading Only

```python
from src.pyspark.data_loader import InforceDataLoader

# Initialize loader
loader = InforceDataLoader()

# Load data
df = loader.load_inforce_data()

# Validate data
validation_results = loader.validate_data(df)

# Close session
loader.close()
```

### Base Analysis Functions

```python
from src.pyspark.base_analysis import BaseAnalysis

# Initialize base analysis
base_analysis = BaseAnalysis(spark_session)

# Get data quality report
quality_report = base_analysis.get_data_quality_report(df)

# Calculate retention metrics
retention_metrics = base_analysis.calculate_retention_metrics(df)

# Calculate premium analysis
premium_analysis = base_analysis.calculate_premium_analysis(df)
```

## Demo Script

Run the demo script to test PySpark functionality:

```bash
# Full demo
python demo_pyspark.py

# Data loader only
python demo_pyspark.py --loader-only
```

## Dependencies

Install required dependencies:

```bash
pip install -r requirements.txt
```

Key dependencies:
- `pyspark>=3.4.0`
- `py4j>=0.10.9`
- `findspark>=2.0.0` (optional, for better performance)

## Configuration

The module uses the existing `config/policy_system_config.yaml` for file paths and settings. Make sure this file is properly configured before running PySpark analysis.

## Output

PySpark analysis generates:
- `monthly_distribution_summary_pyspark.csv`: Monthly distribution statistics
- Console output with detailed analysis results
- Data quality reports and validation summaries

## Performance Notes

- PySpark is optimized for large datasets (millions of records)
- For smaller datasets, pandas may be faster due to overhead
- Spark session configuration is optimized for single-machine processing
- Consider cluster deployment for very large datasets

## Error Handling

The module includes comprehensive error handling:
- Import error detection for missing PySpark
- Data validation with detailed error reporting
- Graceful Spark session cleanup
- Detailed exception information for debugging
