#!/usr/bin/env python3
"""
Test script to investigate premium trace issues in inforce data.
"""

import pandas as pd
from pathlib import Path

def test_premium_trace():
    """Test premium trace in inforce data."""
    
    # Load inforce data
    inforce_path = Path("data/inforce/inforce_monthly_view_sorted")
    part_files = list(inforce_path.glob("part-*.csv"))
    
    if not part_files:
        print("No inforce data found!")
        return
    
    # Load the largest part file
    largest_file = max(part_files, key=lambda f: f.stat().st_size)
    print(f"Loading inforce data from: {largest_file}")
    
    df = pd.read_csv(largest_file)
    
    # Test Policy 1 premium trace
    policy_01 = df[df['policy'] == 1].copy()
    
    if policy_01.empty:
        print("Policy 1 not found in inforce data!")
        return
    
    print(f"\nPolicy 1 premium trace:")
    print("=" * 50)
    
    # Group by year-month and show premium
    monthly_premiums = policy_01.groupby(['inforce_yy', 'inforce_mm'])['premium_paid'].first()
    
    for (year, month), premium in monthly_premiums.items():
        print(f"{year}-{month:02d}: ${premium}")
    
    # Check if premium changes over time
    unique_premiums = policy_01['premium_paid'].unique()
    print(f"\nUnique premiums for Policy 1: {unique_premiums}")
    
    if len(unique_premiums) == 1:
        print("❌ ISSUE: Premium is static - no changes over time!")
    else:
        print("✅ Premium changes over time")
    
    # Test a few more policies
    print(f"\nTesting premium trace for multiple policies:")
    print("=" * 50)
    
    sample_policies = df['policy'].unique()[:5]
    
    for policy_no in sample_policies:
        policy_data = df[df['policy'] == policy_no]
        unique_premiums = policy_data['premium_paid'].unique()
        
        if len(unique_premiums) == 1:
            print(f"Policy {policy_no}: Static premium ${unique_premiums[0]}")
        else:
            print(f"Policy {policy_no}: {len(unique_premiums)} different premiums")

if __name__ == "__main__":
    test_premium_trace()
