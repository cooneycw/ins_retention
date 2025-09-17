#!/usr/bin/env python3
"""
Test script to verify assignment premium storage functionality
"""

import pandas as pd

def test_assignment_premiums():
    """Test the new assignment premium storage functionality"""
    
    print("TESTING ASSIGNMENT PREMIUM STORAGE")
    print("=" * 50)
    
    # Load assignments data
    assignments_df = pd.read_csv('data/policy_system/driver_vehicle_assignments.csv')
    
    print("ASSIGNMENTS DATA STRUCTURE:")
    print("-" * 30)
    print("Columns:", assignments_df.columns.tolist())
    print()
    
    # Check if new columns exist
    has_premium_rate = 'annual_premium_rate' in assignments_df.columns
    has_premium_paid = 'annual_premium_paid' in assignments_df.columns
    
    print("NEW COLUMNS CHECK:")
    print("-" * 20)
    print(f"annual_premium_rate: {'✅' if has_premium_rate else '❌'}")
    print(f"annual_premium_paid: {'✅' if has_premium_paid else '❌'}")
    
    if has_premium_rate and has_premium_paid:
        print()
        print("SAMPLE ASSIGNMENT RECORDS WITH PREMIUMS:")
        print("-" * 45)
        
        # Show sample records
        sample_cols = ['assignment_id', 'family_id', 'vehicle_no', 'driver_no', 
                      'exposure_factor', 'annual_premium_rate', 'annual_premium_paid', 
                      'effective_date', 'status']
        
        print(assignments_df[sample_cols].head(10).to_string(index=False))
        
        print()
        print("PREMIUM STATISTICS:")
        print("-" * 20)
        print(f"Total assignments: {len(assignments_df)}")
        print(f"Active assignments: {len(assignments_df[assignments_df['status'] == 'active'])}")
        print(f"Removed assignments: {len(assignments_df[assignments_df['status'] == 'removed'])}")
        
        if len(assignments_df) > 0:
            print(f"Premium rate range: ${assignments_df['annual_premium_rate'].min():.2f} - ${assignments_df['annual_premium_rate'].max():.2f}")
            print(f"Premium paid range: ${assignments_df['annual_premium_paid'].min():.2f} - ${assignments_df['annual_premium_paid'].max():.2f}")
            print(f"Mean premium rate: ${assignments_df['annual_premium_rate'].mean():.2f}")
            print(f"Mean premium paid: ${assignments_df['annual_premium_paid'].mean():.2f}")
        
        print()
        print("POLICY 1 AND 2 ASSIGNMENTS:")
        print("-" * 30)
        
        # Load policies to get family IDs
        policies_df = pd.read_csv('data/policy_system/policies_master.csv')
        
        for policy_no in [1, 2]:
            policy = policies_df[policies_df['policy_no'] == policy_no]
            if len(policy) > 0:
                family_id = policy.iloc[0]['family_id']
                policy_assignments = assignments_df[assignments_df['family_id'] == family_id]
                
                print(f"\\nPolicy {policy_no} (Family {family_id}):")
                if len(policy_assignments) > 0:
                    print(policy_assignments[['assignment_id', 'vehicle_no', 'driver_no', 
                                           'exposure_factor', 'annual_premium_rate', 'annual_premium_paid', 
                                           'effective_date', 'status']].to_string(index=False))
                else:
                    print("  No assignments found")
    else:
        print()
        print("❌ NEW COLUMNS NOT FOUND - Data regeneration needed")
        print("Run: python main.py")

if __name__ == "__main__":
    test_assignment_premiums()
