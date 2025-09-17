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
    """Reconcile premiums between assignment data and inforce data - AGGREGATE FOCUS"""
    
    print(f"\\nAGGREGATE RECONCILIATION FOR {target_year}")
    print("=" * 50)
    
    # Get all policies that have data for the target year
    policies_with_data = inforce_df[
        inforce_df['inforce_yy'] == target_year
    ]['policy'].unique()
    
    print(f"Found {len(policies_with_data)} policies with {target_year} data")
    
    # Calculate aggregate totals
    total_inforce_premium = 0.0
    total_assignment_premium = 0.0
    perfect_matches = 0
    within_1pct = 0
    within_5pct = 0
    
    for policy_no in sorted(policies_with_data):
        # Calculate inforce annual premium
        inforce_premium = calculate_inforce_annual_premium(inforce_df, policy_no, target_year)
        
        # Calculate assignment annual premium
        assignment_premium = calculate_assignment_annual_premium(assignments_df, policies_df, policy_no, target_year)
        
        # Add to totals
        total_inforce_premium += inforce_premium
        total_assignment_premium += assignment_premium
        
        # Calculate difference for match rate analysis
        if assignment_premium > 0:
            pct_difference = abs((inforce_premium - assignment_premium) / assignment_premium * 100)
            
            if pct_difference == 0.0:
                perfect_matches += 1
            if pct_difference <= 1.0:
                within_1pct += 1
            if pct_difference <= 5.0:
                within_5pct += 1
    
    # Calculate aggregate differences
    total_difference = total_assignment_premium - total_inforce_premium
    aggregate_pct_difference = (total_difference / total_inforce_premium * 100) if total_inforce_premium > 0 else 0
    
    # Summary report
    print(f"\\nAGGREGATE RECONCILIATION SUMMARY")
    print("-" * 40)
    print(f"Total Policies: {len(policies_with_data)}")
    print(f"Total Inforce Premium: ${total_inforce_premium:,.2f}")
    print(f"Total Assignment Premium: ${total_assignment_premium:,.2f}")
    print(f"Total Difference: ${total_difference:,.2f}")
    print(f"Aggregate % Difference: {aggregate_pct_difference:.4f}%")
    
    print(f"\\nMATCH RATE ANALYSIS")
    print("-" * 25)
    print(f"Perfect Matches (0.0%): {perfect_matches} ({perfect_matches/len(policies_with_data)*100:.1f}%)")
    print(f"Within 1%: {within_1pct} ({within_1pct/len(policies_with_data)*100:.1f}%)")
    print(f"Within 5%: {within_5pct} ({within_5pct/len(policies_with_data)*100:.1f}%)")
    
    # Success criteria
    print(f"\\nVALIDATION RESULT")
    print("-" * 20)
    if abs(aggregate_pct_difference) <= 0.01:  # Within 0.01%
        print("✅ PERFECT RECONCILIATION ACHIEVED!")
        print("   Aggregate difference within 0.01% tolerance")
    elif abs(aggregate_pct_difference) <= 1.0:  # Within 1%
        print("✅ RECONCILIATION WITHIN TOLERANCE")
        print("   Aggregate difference within 1% tolerance")
    else:
        print("❌ RECONCILIATION OUTSIDE TOLERANCE")
        print("   Aggregate difference exceeds 1% tolerance")
    
    return {
        'total_policies': len(policies_with_data),
        'total_inforce_premium': total_inforce_premium,
        'total_assignment_premium': total_assignment_premium,
        'total_difference': total_difference,
        'aggregate_pct_difference': aggregate_pct_difference,
        'perfect_matches': perfect_matches,
        'within_1pct': within_1pct,
        'within_5pct': within_5pct,
        'match_rate_1pct': within_1pct/len(policies_with_data)*100,
        'success': abs(aggregate_pct_difference) <= 1.0
    }

# Individual policy analysis removed - focusing on aggregate reconciliation only

def main():
    """Main reconciliation function - AGGREGATE FOCUS"""
    print("ASSIGNMENT PREMIUM RECONCILIATION - AGGREGATE FOCUS")
    print("=" * 50)
    
    # Load data
    inforce_df, assignments_df, policies_df = load_data()
    
    # Run aggregate reconciliation for 2018
    results = reconcile_premiums(inforce_df, assignments_df, policies_df, 2018)
    
    # Return success status for pipeline integration
    return results['success']

if __name__ == "__main__":
    main()
