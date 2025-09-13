#!/usr/bin/env python3
"""
PySpark Demo Script

Demonstrates the PySpark-based data processing capabilities for the retention analysis system.
This script can be run independently to test PySpark functionality.
"""

import sys
import os
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def demo_pyspark_analysis():
    """Demonstrate PySpark analysis capabilities."""
    print("PySpark Retention Analysis Demo")
    print("=" * 40)
    
    try:
        from src.pyspark.distribution_analysis import DistributionAnalysis
        from src.pyspark.data_loader import InforceDataLoader
        
        print("✅ PySpark modules imported successfully")
        
        # Initialize PySpark analysis
        print("\n1. Initializing PySpark session...")
        analysis = DistributionAnalysis()
        print("✅ PySpark session created")
        
        # Load and validate data
        print("\n2. Loading inforce data...")
        data_loader = InforceDataLoader(analysis.spark)
        df = data_loader.load_inforce_data()
        
        # Validate data
        print("\n3. Validating data...")
        validation_results = data_loader.validate_data(df)
        
        # Perform distribution analysis
        print("\n4. Performing distribution analysis...")
        analysis.analyze_inforce_distributions(df)
        
        # Generate reports
        print("\n5. Generating distribution reports...")
        analysis.generate_distribution_report(df)
        
        # Perform data profiling
        print("\n6. Performing data profiling...")
        from src.pyspark.profiling_analysis import ProfilingAnalysis
        profiling = ProfilingAnalysis(analysis.spark)
        profiling.generate_profiling_report(df)
        
        # Demonstrate some base analysis functions
        print("\n7. Demonstrating base analysis functions...")
        from src.pyspark.base_analysis import BaseAnalysis
        base_analysis = BaseAnalysis(analysis.spark)
        
        # Get data quality report
        quality_report = base_analysis.get_data_quality_report(df)
        print(f"   - Data quality report generated")
        print(f"   - Total records: {quality_report['total_records']:,}")
        print(f"   - Duplicate records: {quality_report['duplicate_records']:,}")
        
        # Calculate temporal analysis
        temporal_analysis = base_analysis.calculate_temporal_analysis(df)
        print(f"   - Temporal analysis completed")
        
        # Calculate premium analysis
        premium_analysis = base_analysis.calculate_premium_analysis(df)
        print(f"   - Premium analysis completed")
        
        # Close Spark session
        print("\n8. Closing PySpark session...")
        analysis.close()
        print("✅ PySpark session closed")
        
        print("\n" + "=" * 40)
        print("🎉 PySpark demo completed successfully!")
        print("Check the output directory for generated reports.")
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure PySpark is installed: pip install pyspark")
        return False
        
    except Exception as e:
        print(f"❌ Error during PySpark demo: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

def demo_data_loader_only():
    """Demo just the data loader functionality."""
    print("PySpark Data Loader Demo")
    print("=" * 30)
    
    try:
        from src.pyspark.data_loader import InforceDataLoader
        
        # Initialize data loader
        loader = InforceDataLoader()
        
        # Load data
        print("Loading inforce data...")
        df = loader.load_inforce_data()
        
        # Validate data
        print("Validating data...")
        validation_results = loader.validate_data(df)
        
        # Show some basic operations
        print("\nBasic DataFrame operations:")
        print(f"  - Schema: {len(df.columns)} columns")
        print(f"  - Columns: {', '.join(df.columns[:5])}...")
        
        # Show sample data
        print("\nSample data (first 3 rows):")
        sample = df.limit(3).collect()
        for i, row in enumerate(sample):
            print(f"  Row {i+1}: Policy {row['policy']}, Premium ${row['premium_paid']:.2f}")
        
        # Close session
        loader.close()
        print("\n✅ Data loader demo completed")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="PySpark Demo Script")
    parser.add_argument("--loader-only", action="store_true", 
                       help="Run only the data loader demo")
    
    args = parser.parse_args()
    
    if args.loader_only:
        success = demo_data_loader_only()
    else:
        success = demo_pyspark_analysis()
    
    sys.exit(0 if success else 1)
