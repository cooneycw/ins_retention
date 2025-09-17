#!/usr/bin/env python3
"""
Test Policy 2 reconciliation after vehicle substitution fix
"""

import pandas as pd
from datetime import datetime

def test_policy2_reconciliation():
    """Test Policy 2 reconciliation to verify vehicle substitution fix"""
    
    print("POLICY 2 RECONCILIATION TEST - AFTER VEHICLE SUBSTITUTION FIX")
    print("=" * 70)
    
    # Load inforce data
    try:
        inforce_df = pd.read_csv('data/inforce/inforce_monthly_view_sorted/part-00000-*.csv')
    except:
        # Try alternative path
        inforce_df = pd.read_csv('data/inforce/inforce_monthly_view.csv')
    
    # Load policy system data
    policies_df = pd.read_csv('data/policy_system/policies_master.csv')
    
    print("1. CHECKING VEHICLE TYPE CHANGES IN INFORCE DATA")
    print("-" * 50)
    
    # Get Policy 2 data
    policy_2 = inforce_df[inforce_df['policy'] == 2]
    
    # Check 2019 data around vehicle substitution (2019-05-01)
    policy_2_2019 = policy_2[policy_2['inforce_yy'] == 2019]
    
    print("2019 Monthly Vehicle Details:")
    print("Month | Driver | Vehicle | Vehicle Type | Premium Rate")
    print("-" * 65)
    
    vehicle_types_before = []
    vehicle_types_after = []
    premium_rates_before = []
    premium_rates_after = []
    
    for month in range(1, 13):
        month_data = policy_2_2019[policy_2_2019['inforce_mm'] == month]
        
        if len(month_data) == 0:
            print(f'{month:5} | No data')
            continue
        
        for _, row in month_data.iterrows():
            print(f'{month:5} | {row["driver_name"]:15} | {row["vehicle_no"]:7} | {row["vehicle_type"]:12} | ${row["premium_rate"]:8.2f}')
            
            # Collect data for analysis
            if month <= 4:  # Before substitution
                vehicle_types_before.append(row["vehicle_type"])
                premium_rates_before.append(row["premium_rate"])
            elif month >= 5:  # After substitution
                vehicle_types_after.append(row["vehicle_type"])
                premium_rates_after.append(row["premium_rate"])
    
    print("\n2. ANALYSIS OF CHANGES")
    print("-" * 30)
    
    # Check if vehicle type changed
    unique_types_before = list(set(vehicle_types_before))
    unique_types_after = list(set(vehicle_types_after))
    
    print(f"Vehicle types before May 2019: {unique_types_before}")
    print(f"Vehicle types after May 2019: {unique_types_after}")
    
    if unique_types_before != unique_types_after:
        print("✅ VEHICLE TYPE CHANGED - Fix appears to be working!")
    else:
        print("❌ VEHICLE TYPE UNCHANGED - Fix may not be working")
    
    # Check if premium rates changed
    unique_rates_before = list(set(premium_rates_before))
    unique_rates_after = list(set(premium_rates_after))
    
    print(f"Premium rates before May 2019: {unique_rates_before}")
    print(f"Premium rates after May 2019: {unique_rates_after}")
    
    if unique_rates_before != unique_rates_after:
        print("✅ PREMIUM RATES CHANGED - Fix appears to be working!")
    else:
        print("❌ PREMIUM RATES UNCHANGED - Fix may not be working")
    
    print("\n3. RECONCILIATION TEST")
    print("-" * 25)
    
    # Calculate inforce annual premium for 2018
    policy_2_2018 = policy_2[policy_2['inforce_yy'] == 2018]
    if len(policy_2_2018) > 0:
        # Sum monthly earned premiums (premium_paid is annual, so divide by 12 for monthly earned)
        monthly_earned = policy_2_2018['premium_paid'] / 12
        inforce_annual_2018 = monthly_earned.sum()
        
        # Get policy system premium
        policy_2_system = policies_df[policies_df['policy_no'] == 2]
        if len(policy_2_system) > 0:
            system_premium_2018 = float(policy_2_system.iloc[0]['premium_paid'])
            
            # Calculate difference
            difference = system_premium_2018 - inforce_annual_2018
            percent_diff = (difference / inforce_annual_2018) * 100
            
            print(f"Policy System Premium (2018): ${system_premium_2018:,.2f}")
            print(f"Inforce Annual Premium (2018): ${inforce_annual_2018:,.2f}")
            print(f"Difference: ${difference:,.2f} ({percent_diff:+.1f}%)")
            
            if abs(percent_diff) <= 1.0:
                print("✅ RECONCILIATION WITHIN 1% - Fix successful!")
            else:
                print("❌ RECONCILIATION OUTSIDE 1% - May need further investigation")
        else:
            print("❌ Policy 2 not found in policy system data")
    else:
        print("❌ No 2018 data found for Policy 2")
    
    print("\n4. SUMMARY")
    print("-" * 15)
    print("Expected after fix:")
    print("- Vehicle type should change after 2019-05-01")
    print("- Premium rates should change after 2019-05-01")
    print("- 2018 reconciliation should be within 1%")
    print("- 2019 reconciliation should be within 1%")

if __name__ == "__main__":
    test_policy2_reconciliation()
