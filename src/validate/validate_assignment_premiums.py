#!/usr/bin/env python3
"""
Updated Premium Reconciliation Script
Compares assignment premiums (stored at assignment time) with inforce premiums
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

def load_data():
    """Load all required data files"""
    print("Loading data files...")
    
    # Get the project root directory (two levels up from this file)
    project_root = Path(__file__).parent.parent.parent
    
    # Load inforce data
    inforce_df = pd.read_csv(project_root / 'data/inforce/inforce_monthly_view.csv')
    
    # Load assignments data with premiums
    assignments_df = pd.read_csv(project_root / 'data/policy_system/driver_vehicle_assignments.csv')
    
    # Load policies data
    policies_df = pd.read_csv(project_root / 'data/policy_system/policies_master.csv')
    
    print(f"Loaded {len(inforce_df)} inforce records")
    print(f"Loaded {len(assignments_df)} assignment records")
    print(f"Loaded {len(policies_df)} policy records")
    
    return inforce_df, assignments_df, policies_df

def calculate_inforce_annual_premium(inforce_df, policy_no, year):
    """Calculate annual earned premium from inforce data for a specific policy and year"""
    policy_inforce = inforce_df[
        (inforce_df['policy'] == policy_no) & 
        (inforce_df['inforce_yy'] == year)
    ]
    
    if len(policy_inforce) == 0:
        return 0.0
    
    # Sum monthly earned premiums (premium_paid is annual, so divide by 12 for monthly earned)
    annual_earned = (policy_inforce['premium_paid'] / 12).sum()
    return round(annual_earned, 2)

def calculate_assignment_annual_premium(assignments_df, policies_df, policy_no, year):
    """Calculate annual premium from assignment data for a specific policy and year"""
    # Get family ID for the policy
    policy = policies_df[policies_df['policy_no'] == policy_no]
    if len(policy) == 0:
        return 0.0
    
    family_id = policy.iloc[0]['family_id']
    
    # Get assignments for this family that were active during the year
    year_start = f"{year}-01-01"
    year_end = f"{year}-12-31"
    
    family_assignments = assignments_df[
        (assignments_df['family_id'] == family_id) &
        (assignments_df['effective_date'] <= year_end) &
        (assignments_df['status'] == 'active')
    ]
    
    if len(family_assignments) == 0:
        return 0.0
    
    # Sum the annual premiums paid for active assignments
    annual_premium = family_assignments['annual_premium_paid'].sum()
    return round(annual_premium, 2)

def reconcile_premiums(inforce_df, assignments_df, policies_df, target_year=2018):
    """Reconcile premiums between assignment data and inforce data"""
    
    print(f"\\nRECONCILIATION FOR {target_year}")
    print("=" * 50)
    
    # Get all policies that have data for the target year
    policies_with_data = inforce_df[
        inforce_df['inforce_yy'] == target_year
    ]['policy'].unique()
    
    print(f"Found {len(policies_with_data)} policies with {target_year} data")
    
    reconciliation_results = []
    
    for policy_no in sorted(policies_with_data):
        # Calculate inforce annual premium
        inforce_premium = calculate_inforce_annual_premium(inforce_df, policy_no, target_year)
        
        # Calculate assignment annual premium
        assignment_premium = calculate_assignment_annual_premium(assignments_df, policies_df, policy_no, target_year)
        
        # Calculate difference
        if assignment_premium > 0:
            difference = inforce_premium - assignment_premium
            pct_diff = (difference / assignment_premium) * 100
        else:
            difference = 0
            pct_diff = 0
        
        reconciliation_results.append({
            'policy_no': policy_no,
            'inforce_premium': inforce_premium,
            'assignment_premium': assignment_premium,
            'difference': difference,
            'pct_difference': pct_diff
        })
    
    # Convert to DataFrame for analysis
    results_df = pd.DataFrame(reconciliation_results)
    
    # Filter out policies with zero premiums (no data)
    results_df = results_df[results_df['assignment_premium'] > 0]
    
    print(f"\\nRECONCILIATION SUMMARY")
    print("-" * 30)
    print(f"Total policies analyzed: {len(results_df)}")
    print(f"Mean difference: ${results_df['difference'].mean():.2f}")
    print(f"Mean % difference: {results_df['pct_difference'].mean():.2f}%")
    print(f"Std deviation: {results_df['pct_difference'].std():.2f}%")
    
    # Count policies within tolerance
    within_1pct = len(results_df[abs(results_df['pct_difference']) <= 1.0])
    within_5pct = len(results_df[abs(results_df['pct_difference']) <= 5.0])
    within_10pct = len(results_df[abs(results_df['pct_difference']) <= 10.0])
    
    print(f"\\nTOLERANCE ANALYSIS")
    print("-" * 20)
    print(f"Within 1%: {within_1pct} ({within_1pct/len(results_df)*100:.1f}%)")
    print(f"Within 5%: {within_5pct} ({within_5pct/len(results_df)*100:.1f}%)")
    print(f"Within 10%: {within_10pct} ({within_10pct/len(results_df)*100:.1f}%)")
    
    # Show largest discrepancies
    print(f"\\nLARGEST DISCREPANCIES")
    print("-" * 25)
    largest_discrepancies = results_df.nlargest(10, 'pct_difference')
    print(largest_discrepancies[['policy_no', 'inforce_premium', 'assignment_premium', 'pct_difference']].to_string(index=False))
    
    return results_df

def analyze_specific_policies(inforce_df, assignments_df, policies_df, policy_numbers, year=2018):
    """Analyze specific policies in detail"""
    
    print(f"\\nDETAILED ANALYSIS FOR POLICIES {policy_numbers} - {year}")
    print("=" * 60)
    
    for policy_no in policy_numbers:
        print(f"\\nPOLICY {policy_no}:")
        print("-" * 15)
        
        # Get policy info
        policy = policies_df[policies_df['policy_no'] == policy_no]
        if len(policy) == 0:
            print("Policy not found")
            continue
            
        family_id = policy.iloc[0]['family_id']
        print(f"Family ID: {family_id}")
        print(f"Policy Status: {policy.iloc[0]['status']}")
        
        # Calculate premiums
        inforce_premium = calculate_inforce_annual_premium(inforce_df, policy_no, year)
        assignment_premium = calculate_assignment_annual_premium(assignments_df, policies_df, policy_no, year)
        
        print(f"Inforce Premium: ${inforce_premium:,.2f}")
        print(f"Assignment Premium: ${assignment_premium:,.2f}")
        
        if assignment_premium > 0:
            difference = inforce_premium - assignment_premium
            pct_diff = (difference / assignment_premium) * 100
            print(f"Difference: ${difference:,.2f} ({pct_diff:+.2f}%)")
        
        # Show assignment details
        year_start = f"{year}-01-01"
        year_end = f"{year}-12-31"
        
        family_assignments = assignments_df[
            (assignments_df['family_id'] == family_id) &
            (assignments_df['effective_date'] <= year_end) &
            (assignments_df['status'] == 'active')
        ]
        
        print(f"\\nActive Assignments for {year}:")
        if len(family_assignments) > 0:
            display_cols = ['assignment_id', 'vehicle_no', 'driver_no', 'exposure_factor', 
                          'annual_premium_rate', 'annual_premium_paid', 'effective_date', 'status']
            print(family_assignments[display_cols].to_string(index=False))
        else:
            print("No active assignments found")
        
        # Show inforce details
        policy_inforce = inforce_df[
            (inforce_df['policy'] == policy_no) & 
            (inforce_df['inforce_yy'] == year)
        ]
        
        print(f"\\nInforce Records for {year}:")
        if len(policy_inforce) > 0:
            display_cols = ['vehicle_no', 'driver_no', 'premium_rate', 'premium_paid', 'inforce_mm']
            print(policy_inforce[display_cols].head(12).to_string(index=False))
        else:
            print("No inforce records found")

def main():
    """Main reconciliation function"""
    print("ASSIGNMENT PREMIUM RECONCILIATION")
    print("=" * 40)
    
    # Load data
    inforce_df, assignments_df, policies_df = load_data()
    
    # Run reconciliation for 2018
    results_df = reconcile_premiums(inforce_df, assignments_df, policies_df, 2018)
    
    # Analyze specific policies
    analyze_specific_policies(inforce_df, assignments_df, policies_df, [1, 2], 2018)
    
    # Save results
    results_df.to_csv('assignment_premium_reconciliation_2018.csv', index=False)
    print(f"\\nResults saved to: assignment_premium_reconciliation_2018.csv")

if __name__ == "__main__":
    main()
