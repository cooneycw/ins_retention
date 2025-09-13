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
        ("Distribution Analysis", distribution_analysis),
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
    
    print(f"\n{'='*50}")
    print("🎉 Insurance Policy System generation completed!")
    print("Check 'inforce_monthly_view.csv' for the final output.")

def build_initial_state() -> None:
    """Step 1: Create initial families with driver-vehicle assignments."""
    from src.create.build_initial_state import create_initial_state
    create_initial_state()

def apply_monthly_changes() -> None:
    """Step 2: Apply monthly changes over 90 months with actual vehicle changes."""
    from src.create.apply_changes_proper import simulate_monthly_changes
    simulate_monthly_changes()

def generate_inforce_view() -> None:
    """Step 3: Generate the final inforce monthly view (minimal version)."""
    from src.create.generate_inforce_minimal import create_inforce_view
    create_inforce_view()

def distribution_analysis() -> None:
    """Step 4: Perform distribution analysis and generate reports."""
    from src.report.distribution_analysis import analyze_inforce_distributions, generate_distribution_report
    analyze_inforce_distributions()
    generate_distribution_report()

if __name__ == "__main__":
    main()
