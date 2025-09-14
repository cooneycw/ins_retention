#!/usr/bin/env python3
"""
Insurance Policy System - Main Entry Point

This is the main entry point for the insurance policy system.
Make sure to activate the conda environment before running:
    conda activate retention
"""

import sys
import os
from pathlib import Path
from typing import Optional

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def main() -> None:
    """Main function - orchestrates the insurance policy system generation."""
    import argparse
    
    # Check conda environment
    conda_env = os.environ.get('CONDA_DEFAULT_ENV', '')
    if conda_env != 'retention':
        print("⚠️  WARNING: Not running in 'retention' conda environment!")
        print("   Please run: conda activate retention")
        print(f"   Current environment: {conda_env if conda_env else 'none'}")
        print()
    
    parser = argparse.ArgumentParser(description="Insurance Policy System")
    parser.add_argument("--testview", action="store_true", 
                       help="Launch Policy Viewer GUI instead of running pipeline")
    parser.add_argument("--csv", default="data/inforce/inforce_monthly_view_sorted",
                       help="Path to CSV file or directory for testview (default: data/inforce/inforce_monthly_view_sorted)")
    
    args = parser.parse_args()
    
    # If testview is requested, launch the GUI
    if args.testview:
        try:
            from src.testview import PolicyViewer
            print("Launching Policy Viewer...")
            viewer = PolicyViewer(args.csv)
            viewer.run()
            return
        except Exception as e:
            print(f"Error launching Policy Viewer: {e}")
            return
    
    # Normal pipeline execution
    print("Insurance Policy System - Main Entry Point")
    print("=" * 50)
    
    # Ensure data directory exists
    data_dir = Path("src/create/data")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Step-by-step process execution
    steps = [
        ("Build Initial State", build_initial_state),
        ("Apply Monthly Changes", apply_monthly_changes),
        ("Generate Inforce View", generate_inforce_view),
        ("PySpark Distribution Analysis", distribution_analysis),
    ]
    
    for step_name, step_function in steps:
        print(f"\n{'='*20} {step_name} {'='*20}")
        try:
            step_function()
            print(f"✅ {step_name} completed successfully")
        except Exception as e:
            print(f"❌ {step_name} failed: {e}")
            print("Stopping execution. Fix the error and re-run.")
            return
    
    # Clean up PySpark session if it was created
    import sys
    data_loader = getattr(sys.modules['__main__'], 'pyspark_data_loader', None)
    if data_loader is not None:
        print("\nCleaning up PySpark session...")
        data_loader.close()
        print("✅ PySpark session closed")
    
    print(f"\n{'='*50}")
    print("🎉 Insurance Policy System generation completed!")
    print("Check 'inforce_monthly_view.csv' for the final output.")
    print("Check 'output/' directory for PySpark-generated reports.")
    print("\nTo launch the Policy Viewer GUI, run:")
    print("  python main.py --testview")

def build_initial_state() -> None:
    """Step 1: Create initial families with driver-vehicle assignments."""
    from src.create.build_initial_state import create_initial_state
    create_initial_state()

def apply_monthly_changes() -> None:
    """Step 2: Apply monthly changes over 90 months with actual vehicle changes."""
    from src.create.apply_changes_proper import simulate_monthly_changes
    simulate_monthly_changes()

def generate_inforce_view() -> None:
    """Step 3: Generate the final inforce monthly view and create PySpark DataFrame."""
    from src.create.generate_inforce_minimal import create_inforce_view
    
    # Generate the CSV file first
    create_inforce_view()
    
    # Create PySpark DataFrame from the generated CSV
    try:
        from src.spark_analysis.data_loader import InforceDataLoader
        print("  - Creating PySpark DataFrame from inforce data...")
        
        # Initialize data loader
        data_loader = InforceDataLoader()
        
        # Load data into PySpark DataFrame
        inforce_df = data_loader.load_inforce_data()
        
        # Validate the loaded data
        validation_results = data_loader.validate_data(inforce_df)
        
        # Apply major change detection to the PySpark DataFrame
        from src.spark_analysis.major_change_analysis import MajorChangeAnalysis
        print("  - Applying major change detection to PySpark DataFrame...")
        
        major_change_analysis = MajorChangeAnalysis(data_loader.spark)
        inforce_df_with_changes = major_change_analysis.detect_major_changes(inforce_df)
        
        # Generate summary of major changes
        change_summary = major_change_analysis.generate_change_summary(inforce_df_with_changes)
        
        # Save sorted DataFrame for viewing with editor
        print("  - Saving sorted PySpark DataFrame for viewing...")
        sorted_df = inforce_df_with_changes.orderBy("policy", "inforce_yy", "inforce_mm")
        sorted_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("data/inforce/inforce_monthly_view_sorted")
        
        # Cache the DataFrame for use in reporting
        inforce_df_with_changes.cache()
        
        # Store the DataFrame globally for use in reporting step
        import sys
        sys.modules['__main__'].inforce_pyspark_df = inforce_df_with_changes
        sys.modules['__main__'].pyspark_data_loader = data_loader
        
        print("  - PySpark DataFrame with major change flags created and cached successfully")
        
    except ImportError as e:
        print(f"  - Warning: PySpark not available: {e}")
        print("  - Install PySpark with: pip install pyspark")
        # Set None so reporting step knows PySpark is not available
        import sys
        sys.modules['__main__'].inforce_pyspark_df = None
        sys.modules['__main__'].pyspark_data_loader = None
    except Exception as e:
        print(f"  - Error creating PySpark DataFrame: {e}")
        # Set None so reporting step knows PySpark failed
        import sys
        sys.modules['__main__'].inforce_pyspark_df = None
        sys.modules['__main__'].pyspark_data_loader = None

def distribution_analysis() -> None:
    """Step 4: Perform PySpark-based distribution analysis and generate reports."""
    # Check if PySpark DataFrame is available from previous step
    import sys
    inforce_df = getattr(sys.modules['__main__'], 'inforce_pyspark_df', None)
    data_loader = getattr(sys.modules['__main__'], 'pyspark_data_loader', None)
    
    if inforce_df is None or data_loader is None:
        print("  - PySpark DataFrame not available from previous step")
        print("  - Loading inforce data directly from CSV file...")
        try:
            from src.spark_analysis.data_loader import InforceDataLoader
            from src.spark_analysis.major_change_analysis import MajorChangeAnalysis
            
            # Create new data loader and load the existing CSV
            data_loader = InforceDataLoader()
            inforce_df = data_loader.load_inforce_data()
            
            # Apply major change detection
            print("  - Applying major change detection...")
            major_change_analysis = MajorChangeAnalysis(data_loader.spark)
            inforce_df = major_change_analysis.detect_major_changes(inforce_df)
            
            # Cache the DataFrame
            inforce_df.cache()
            
            print("  - Successfully loaded and processed inforce data")
            
        except Exception as e:
            print(f"  - Error loading inforce data: {e}")
            print("  - Install PySpark with: pip install pyspark")
            return
    
    try:
        from src.spark_analysis.distribution_analysis import DistributionAnalysis
        from src.spark_analysis.profiling_analysis import ProfilingAnalysis
        print("  - Starting PySpark-based analysis...")
        
        # Initialize PySpark analysis with existing DataFrame
        distribution_analysis = DistributionAnalysis(data_loader.spark)
        profiling_analysis = ProfilingAnalysis(data_loader.spark)
        
        # Perform comprehensive distribution analysis
        print("  - Running distribution analysis...")
        distribution_analysis.analyze_inforce_distributions(inforce_df)
        distribution_analysis.generate_distribution_report(inforce_df)
        
        # Perform data profiling
        print("  - Running data profiling...")
        profiling_analysis.generate_profiling_report(inforce_df)
        
        print("  - PySpark analysis completed successfully")
        
    except Exception as e:
        print(f"  - Error in PySpark analysis: {e}")
        print("  - Distribution analysis failed. Check PySpark installation and configuration.")


if __name__ == "__main__":
    main()
