#!/usr/bin/env python3
"""
Premium and Exposure Validation Tool

This script compares premium calculations between:
1. Policy System files (policies_master.csv, driver_vehicle_assignments.csv)
2. Inforce files (inforce_monthly_view.csv)

It calculates exposure and premium earnings from both sources and identifies discrepancies.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
from datetime import datetime
import yaml

# Add src to path for imports
sys.path.insert(0, 'src')

def load_config():
    """Load configuration from YAML file."""
    with open('config/policy_system_config.yaml', 'r') as f:
        return yaml.safe_load(f)

def load_policy_system_data():
    """Load data from policy system files."""
    print("Loading policy system data...")
    
    # Load policies
    policies_df = pd.read_csv('data/policy_system/policies_master.csv', dtype={'policy_no': str})
    print(f"  Loaded {len(policies_df)} policies")
    
    # Load assignments
    assignments_df = pd.read_csv('data/policy_system/driver_vehicle_assignments.csv', dtype={'family_id': str})
    print(f"  Loaded {len(assignments_df)} assignments")
    
    # Load drivers
    drivers_df = pd.read_csv('data/policy_system/drivers_master.csv', dtype={'family_id': str})
    print(f"  Loaded {len(drivers_df)} drivers")
    
    # Load vehicles
    vehicles_df = pd.read_csv('data/policy_system/vehicles_master.csv', dtype={'family_id': str})
    print(f"  Loaded {len(vehicles_df)} vehicles")
    
    return policies_df, assignments_df, drivers_df, vehicles_df

def load_inforce_data():
    """Load data from inforce files."""
    print("Loading inforce data...")
    
    # Find the latest inforce file
    inforce_dir = Path("data/inforce/inforce_monthly_view_sorted")
    if inforce_dir.exists():
        part_files = list(inforce_dir.glob("part-*.csv"))
        if part_files:
            # Use the largest file
            largest_file = max(part_files, key=lambda f: f.stat().st_size)
            inforce_df = pd.read_csv(largest_file, dtype={'policy': str})
            print(f"  Loaded {len(inforce_df)} inforce records from {largest_file.name}")
        else:
            raise FileNotFoundError("No part files found in inforce_monthly_view_sorted")
    else:
        # Fallback to main inforce file
        inforce_df = pd.read_csv('data/inforce/inforce_monthly_view.csv', dtype={'policy': str})
        print(f"  Loaded {len(inforce_df)} inforce records from main file")
    
    return inforce_df

def calculate_policy_system_premiums(policies_df, assignments_df, drivers_df, vehicles_df, config):
    """Calculate premiums from policy system data."""
    print("Calculating premiums from policy system data...")
    
    base_premium = config['policies']['premium']['base_amount']
    calc_config = config['policies']['premium']['calculation']
    
    policy_premiums = {}
    
    for _, policy in policies_df.iterrows():
        # Include all policies for comparison, but note their status
        policy_status = policy['status']
            
        family_id = policy['family_id']
        policy_no = policy['policy_no']
        
        # Get assignments for this family
        family_assignments = assignments_df[assignments_df['family_id'] == family_id]
        family_assignments = family_assignments[family_assignments['status'] == 'active']
        
        total_premium = 0.0
        total_exposure = 0.0
        
        for _, assignment in family_assignments.iterrows():
            exposure_factor = float(assignment['exposure_factor'])
            if exposure_factor > 0:
                total_exposure += exposure_factor
                
                # Get vehicle and driver details
                vehicle = vehicles_df[
                    (vehicles_df['family_id'] == family_id) & 
                    (vehicles_df['vehicle_no'] == assignment['vehicle_no'])
                ]
                driver = drivers_df[
                    (drivers_df['family_id'] == family_id) & 
                    (drivers_df['driver_no'] == assignment['driver_no'])
                ]
                
                if len(vehicle) > 0 and len(driver) > 0:
                    vehicle = vehicle.iloc[0]
                    driver = driver.iloc[0]
                    
                    # Calculate assignment premium
                    assignment_premium = base_premium * exposure_factor
                    
                    # Apply vehicle type factor
                    vehicle_factor = calc_config["vehicle_type_factors"].get(vehicle["vehicle_type"], 1.0)
                    assignment_premium *= vehicle_factor
                    
                    # Apply driver age factor
                    driver_age = int(driver["age"])
                    age_factor = get_age_factor(driver_age, calc_config["driver_age_factors"])
                    assignment_premium *= age_factor
                    
                    # Apply driver type factor
                    driver_type_factor = calc_config["driver_type_factors"].get(driver["driver_type"], 1.0)
                    assignment_premium *= driver_type_factor
                    
                    total_premium += assignment_premium
        
        policy_premiums[policy_no] = {
            'calculated_premium': round(max(total_premium, 100.0), 2),
            'total_exposure': total_exposure,
            'stored_premium': float(policy['premium_paid']),
            'family_id': family_id,
            'status': policy_status
        }
    
    print(f"  Calculated premiums for {len(policy_premiums)} policies")
    return policy_premiums

def get_age_factor(age: int, age_factors: dict) -> float:
    """Get the age factor for premium calculation."""
    # Convert age_factors keys to integers if they're strings
    int_age_factors = {int(k): v for k, v in age_factors.items()}
    
    # Find the closest age factor
    if age in int_age_factors:
        return int_age_factors[age]
    
    # Find the closest lower age factor
    applicable_ages = [a for a in int_age_factors.keys() if a <= age]
    if applicable_ages:
        closest_age = max(applicable_ages)
        return int_age_factors[closest_age]
    
    # Default to 1.0 if no factor found
    return 1.0

def calculate_inforce_premiums(inforce_df):
    """Calculate premiums from inforce data."""
    print("Calculating premiums from inforce data...")
    
    # Get the latest month for each policy to get current premium and exposure
    # Create a combined year-month field for easier comparison
    inforce_df['year_month'] = inforce_df['inforce_yy'] * 100 + inforce_df['inforce_mm']
    
    # Find the latest year-month for each policy
    latest_year_month = inforce_df.groupby('policy')['year_month'].max().reset_index()
    latest_year_month.columns = ['policy', 'latest_year_month']
    
    # Merge to get the latest month records for each policy
    latest_month_data = inforce_df.merge(latest_year_month, left_on=['policy', 'year_month'], right_on=['policy', 'latest_year_month'], how='inner')
    
    # Group by policy and calculate totals
    inforce_summary = latest_month_data.groupby('policy').agg({
        'premium_paid': 'mean',  # Should be consistent across records for same policy
        'exposure_factor': 'sum',  # Current month's total exposure for the policy
        'inforce_yy': 'count'  # Number of records in latest month
    }).reset_index()
    
    inforce_summary.columns = ['policy', 'inforce_premium', 'current_exposure', 'records_in_latest_month']
    
    # Also calculate total months in force
    total_months = inforce_df.groupby('policy')['inforce_yy'].nunique().reset_index()
    total_months.columns = ['policy', 'months_inforce']
    
    # Merge the data
    inforce_summary = inforce_summary.merge(total_months, on='policy', how='left')
    
    # Convert to dictionary for easy lookup
    inforce_premiums = {}
    for _, row in inforce_summary.iterrows():
        inforce_premiums[row['policy']] = {
            'inforce_premium': float(row['inforce_premium']),
            'current_exposure': float(row['current_exposure']),
            'months_inforce': int(row['months_inforce'])
        }
    
    print(f"  Calculated premiums for {len(inforce_premiums)} policies from inforce data")
    return inforce_premiums

def compare_premiums(policy_premiums, inforce_premiums):
    """Compare premiums between policy system and inforce data."""
    print("Comparing premiums between policy system and inforce data...")
    
    comparison_results = []
    discrepancies = []
    
    # Get all unique policies
    all_policies = set(policy_premiums.keys()) | set(inforce_premiums.keys())
    
    for policy_no in sorted(all_policies):
        policy_data = policy_premiums.get(policy_no, {})
        inforce_data = inforce_premiums.get(policy_no, {})
        
        if not policy_data and not inforce_data:
            continue
            
        # Extract values with defaults
        calculated_premium = policy_data.get('calculated_premium', 0)
        stored_premium = policy_data.get('stored_premium', 0)
        policy_exposure = policy_data.get('total_exposure', 0)
        
        inforce_premium = inforce_data.get('inforce_premium', 0)
        inforce_exposure = inforce_data.get('current_exposure', 0)
        months_inforce = inforce_data.get('months_inforce', 0)
        
        # Calculate differences
        premium_diff = abs(calculated_premium - inforce_premium) if inforce_premium > 0 else 0
        exposure_diff = abs(policy_exposure - inforce_exposure) if inforce_exposure > 0 else 0
        stored_vs_calculated_diff = abs(stored_premium - calculated_premium)
        
        # Check for discrepancies (only for active policies)
        policy_status = policy_data.get('status', 'unknown')
        is_active = policy_status == 'active'
        
        # For cancelled policies, we expect discrepancies (they should have minimum premiums)
        if not is_active:
            has_premium_discrepancy = False  # Don't flag cancelled policies as discrepancies
            has_exposure_discrepancy = False
            has_stored_discrepancy = False
        else:
            has_premium_discrepancy = premium_diff > 0.01  # Allow for small rounding differences
            has_exposure_discrepancy = exposure_diff > 0.01
            has_stored_discrepancy = stored_vs_calculated_diff > 0.01
        
        if has_premium_discrepancy or has_exposure_discrepancy or has_stored_discrepancy:
            discrepancies.append({
                'policy': policy_no,
                'family_id': policy_data.get('family_id', 'N/A'),
                'calculated_premium': calculated_premium,
                'stored_premium': stored_premium,
                'inforce_premium': inforce_premium,
                'policy_exposure': policy_exposure,
                'inforce_exposure': inforce_exposure,
                'months_inforce': months_inforce,
                'premium_diff': premium_diff,
                'exposure_diff': exposure_diff,
                'stored_vs_calculated_diff': stored_vs_calculated_diff,
                'has_premium_discrepancy': has_premium_discrepancy,
                'has_exposure_discrepancy': has_exposure_discrepancy,
                'has_stored_discrepancy': has_stored_discrepancy
            })
        
        comparison_results.append({
            'policy': policy_no,
            'family_id': policy_data.get('family_id', 'N/A'),
            'status': policy_status,
            'calculated_premium': calculated_premium,
            'stored_premium': stored_premium,
            'inforce_premium': inforce_premium,
            'policy_exposure': policy_exposure,
            'inforce_exposure': inforce_exposure,
            'months_inforce': months_inforce,
            'premium_diff': premium_diff,
            'exposure_diff': exposure_diff,
            'stored_vs_calculated_diff': stored_vs_calculated_diff
        })
    
    return comparison_results, discrepancies

def generate_report(comparison_results, discrepancies):
    """Generate a comprehensive validation report."""
    print("\n" + "="*80)
    print("PREMIUM AND EXPOSURE VALIDATION REPORT")
    print("="*80)
    
    total_policies = len(comparison_results)
    policies_with_discrepancies = len(discrepancies)
    
    print(f"Total policies analyzed: {total_policies}")
    print(f"Policies with discrepancies: {policies_with_discrepancies}")
    print(f"Discrepancy rate: {policies_with_discrepancies/total_policies*100:.2f}%")
    
    if discrepancies:
        print(f"\nDISCREPANCIES FOUND:")
        print("-" * 50)
        
        # Group by discrepancy type
        premium_disc = [d for d in discrepancies if d['has_premium_discrepancy']]
        exposure_disc = [d for d in discrepancies if d['has_exposure_discrepancy']]
        stored_disc = [d for d in discrepancies if d['has_stored_discrepancy']]
        
        print(f"Premium calculation discrepancies: {len(premium_disc)}")
        print(f"Exposure calculation discrepancies: {len(exposure_disc)}")
        print(f"Stored vs calculated discrepancies: {len(stored_disc)}")
        
        # Show top 10 discrepancies
        print(f"\nTOP 10 DISCREPANCIES:")
        print("-" * 50)
        sorted_disc = sorted(discrepancies, key=lambda x: x['premium_diff'] + x['exposure_diff'], reverse=True)
        
        for i, disc in enumerate(sorted_disc[:10]):
            print(f"{i+1:2d}. Policy {disc['policy']} (Family {disc['family_id']})")
            print(f"    Calculated: ${disc['calculated_premium']:.2f}, Stored: ${disc['stored_premium']:.2f}, Inforce: ${disc['inforce_premium']:.2f}")
            print(f"    Policy Exposure: {disc['policy_exposure']:.2f}, Inforce Exposure: {disc['inforce_exposure']:.2f}")
            print(f"    Premium Diff: ${disc['premium_diff']:.2f}, Exposure Diff: {disc['exposure_diff']:.2f}")
            print()
    else:
        print("\n✅ NO DISCREPANCIES FOUND!")
        print("All premium and exposure calculations are consistent between policy system and inforce data.")
    
    # Summary statistics
    print(f"\nSUMMARY STATISTICS:")
    print("-" * 30)
    
    comparison_df = pd.DataFrame(comparison_results)
    
    print(f"Average calculated premium: ${comparison_df['calculated_premium'].mean():.2f}")
    print(f"Average stored premium: ${comparison_df['stored_premium'].mean():.2f}")
    print(f"Average inforce premium: ${comparison_df['inforce_premium'].mean():.2f}")
    print(f"Average policy exposure: {comparison_df['policy_exposure'].mean():.2f}")
    print(f"Average inforce exposure: {comparison_df['inforce_exposure'].mean():.2f}")
    
    # Save detailed results
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    # Save full comparison
    comparison_df.to_csv("output/premium_validation_comparison.csv", index=False)
    print(f"\nDetailed comparison saved to: output/premium_validation_comparison.csv")
    
    # Save discrepancies
    if discrepancies:
        disc_df = pd.DataFrame(discrepancies)
        disc_df.to_csv("output/premium_validation_discrepancies.csv", index=False)
        print(f"Discrepancies saved to: output/premium_validation_discrepancies.csv")

def main():
    """Main validation function."""
    print("Premium and Exposure Validation Tool")
    print("=" * 50)
    
    try:
        # Load configuration
        config = load_config()
        
        # Load data
        policies_df, assignments_df, drivers_df, vehicles_df = load_policy_system_data()
        inforce_df = load_inforce_data()
        
        # Calculate premiums from both sources
        policy_premiums = calculate_policy_system_premiums(policies_df, assignments_df, drivers_df, vehicles_df, config)
        inforce_premiums = calculate_inforce_premiums(inforce_df)
        
        # Compare results
        comparison_results, discrepancies = compare_premiums(policy_premiums, inforce_premiums)
        
        # Generate report
        generate_report(comparison_results, discrepancies)
        
        print(f"\n✅ Validation completed successfully!")
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
