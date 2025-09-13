"""
Distribution Analysis Module

Analyzes the distribution of policies, vehicles, and drivers in the inforce data.
"""

import csv
import yaml
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Any
import pandas as pd


def analyze_inforce_distributions() -> None:
    """
    Perform comprehensive distribution analysis on the inforce data.
    """
    print("Analyzing inforce distributions...")
    
    # Load configuration
    config_path = Path("config/policy_system_config.yaml")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Load inforce data
    inforce_path = Path(config['file_paths']['data']['inforce']) / config['data_files']['inforce']['monthly_view']
    
    if not inforce_path.exists():
        print(f"Error: Inforce file not found at {inforce_path}")
        return
    
    # Read the data
    df = pd.read_csv(inforce_path)
    
    print(f"  - Loaded {len(df)} inforce records")
    
    # Perform various distribution analyses
    _analyze_policy_distributions(df)
    _analyze_vehicle_distributions(df)
    _analyze_driver_distributions(df)
    _analyze_temporal_distributions(df)
    _analyze_premium_distributions(df)
    
    print("Distribution analysis completed!")


def _analyze_policy_distributions(df: pd.DataFrame) -> None:
    """Analyze policy-level distributions."""
    print("  - Policy Distributions:")
    
    # Unique policies
    unique_policies = df['policy'].nunique()
    print(f"    Total unique policies: {unique_policies}")
    
    # Policy expiry date distribution
    expiry_counts = df['policy_expiry_date'].value_counts()
    print(f"    Policy expiry dates: {len(expiry_counts)} unique dates")
    print(f"    Most common expiry: {expiry_counts.index[0]} ({expiry_counts.iloc[0]} policies)")
    
    # Client tenure distribution
    tenure_stats = df['client_tenure_days'].describe()
    print(f"    Client tenure - Mean: {tenure_stats['mean']:.0f} days, Median: {tenure_stats['50%']:.0f} days")


def _analyze_vehicle_distributions(df: pd.DataFrame) -> None:
    """Analyze vehicle-level distributions."""
    print("  - Vehicle Distributions:")
    
    # Vehicle types
    vehicle_types = df['vehicle_type'].value_counts()
    print(f"    Vehicle types: {len(vehicle_types)} types")
    for vtype, count in vehicle_types.head(5).items():
        print(f"      {vtype}: {count} records")
    
    # Model years
    model_years = df['model_year'].value_counts().sort_index()
    print(f"    Model years: {model_years.index.min()}-{model_years.index.max()}")
    print(f"    Most common model year: {model_years.idxmax()} ({model_years.max()} records)")
    
    # Vehicles per policy
    vehicles_per_policy = df.groupby('policy')['vehicle_no'].nunique()
    print(f"    Vehicles per policy - Mean: {vehicles_per_policy.mean():.2f}, Max: {vehicles_per_policy.max()}")


def _analyze_driver_distributions(df: pd.DataFrame) -> None:
    """Analyze driver-level distributions."""
    print("  - Driver Distributions:")
    
    # Driver types
    driver_types = df['driver_type'].value_counts()
    print(f"    Driver types: {len(driver_types)} types")
    for dtype, count in driver_types.items():
        print(f"      {dtype}: {count} records")
    
    # Driver ages
    age_stats = df['driver_age'].describe()
    print(f"    Driver ages - Mean: {age_stats['mean']:.1f}, Range: {age_stats['min']:.0f}-{age_stats['max']:.0f}")
    
    # Drivers per policy
    drivers_per_policy = df.groupby('policy')['driver_no'].nunique()
    print(f"    Drivers per policy - Mean: {drivers_per_policy.mean():.2f}, Max: {drivers_per_policy.max()}")
    
    # Unique driver names (if available)
    if 'driver_name' in df.columns:
        unique_drivers = df['driver_name'].nunique()
        print(f"    Unique driver names: {unique_drivers}")


def _analyze_temporal_distributions(df: pd.DataFrame) -> None:
    """Analyze temporal distributions."""
    print("  - Temporal Distributions:")
    
    # Year distribution
    year_counts = df['inforce_yy'].value_counts().sort_index()
    print(f"    Years covered: {year_counts.index.min()}-{year_counts.index.max()}")
    print(f"    Records per year - Min: {year_counts.min()}, Max: {year_counts.max()}")
    
    # Month distribution
    month_counts = df['inforce_mm'].value_counts().sort_index()
    print(f"    Month distribution: {month_counts.min()}-{month_counts.max()} records per month")


def _analyze_premium_distributions(df: pd.DataFrame) -> None:
    """Analyze premium distributions."""
    print("  - Premium Distributions:")
    
    # Premium statistics
    premium_stats = df['premium_paid'].describe()
    print(f"    Premium - Mean: ${premium_stats['mean']:.2f}, Median: ${premium_stats['50%']:.2f}")
    print(f"    Premium range: ${premium_stats['min']:.2f} - ${premium_stats['max']:.2f}")
    
    # Premium by driver type
    if 'driver_type' in df.columns:
        premium_by_type = df.groupby('driver_type')['premium_paid'].mean()
        print(f"    Average premium by driver type:")
        for dtype, avg_premium in premium_by_type.items():
            print(f"      {dtype}: ${avg_premium:.2f}")


def generate_distribution_report() -> None:
    """Generate a comprehensive distribution report and save to file."""
    print("Generating distribution report...")
    
    # Load configuration
    config_path = Path("config/policy_system_config.yaml")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Load inforce data
    inforce_path = Path(config['file_paths']['data']['inforce']) / config['data_files']['inforce']['monthly_view']
    df = pd.read_csv(inforce_path)
    
    # Create output directory
    output_dir = Path(config['file_paths']['output'])
    output_dir.mkdir(exist_ok=True)
    
    # Generate the requested monthly distribution report
    _generate_monthly_distribution_report(df, output_dir)
    
    print(f"Distribution reports saved to {output_dir}")


def _generate_monthly_distribution_report(df: pd.DataFrame, output_dir: Path) -> None:
    """Generate monthly distribution report with requested statistics."""
    
    # Group by year and month - this gives us inforce policies (policies active at month end)
    monthly_stats = df.groupby(['inforce_yy', 'inforce_mm']).agg({
        'policy': 'nunique',  # Count of inforce policies
        'premium_paid': 'mean',  # Average premium (inforce policies only)
        'driver_age': 'mean',  # Average driver age (inforce policies only)
        'vehicle_type': lambda x: x.value_counts().to_dict()  # Vehicle type distribution
    }).reset_index()
    
    # Calculate average driver and vehicle counts per inforce policy
    policy_stats = df.groupby(['inforce_yy', 'inforce_mm', 'policy']).agg({
        'driver_no': 'nunique',
        'vehicle_no': 'nunique'
    }).reset_index()
    
    avg_stats = policy_stats.groupby(['inforce_yy', 'inforce_mm']).agg({
        'driver_no': 'mean',
        'vehicle_no': 'mean'
    }).reset_index()
    
    # Merge the statistics
    monthly_stats = monthly_stats.merge(avg_stats, on=['inforce_yy', 'inforce_mm'])
    
    # Rename columns
    monthly_stats.columns = ['year', 'month', 'inforce_count', 'avg_premium', 'avg_driver_age', 'vehicle_type_dist', 'avg_driver_count', 'avg_vehicle_count']
    
    # Calculate renewed vs new policies
    # A policy is "new" if it appears for the first time in that month AND is active at month end
    # A policy is "renewed" if it appears in a previous month
    policy_first_appearance = df.groupby('policy')[['inforce_yy', 'inforce_mm']].min().reset_index()
    
    # Add renewed/new policy counts
    monthly_stats['new_policies'] = 0
    monthly_stats['renewed_policies'] = 0
    
    for idx, row in monthly_stats.iterrows():
        current_year = row['year']
        current_month = row['month']
        
        # Get policies active at the end of this month
        active_policies = set(df[
            (df['inforce_yy'] == current_year) & 
            (df['inforce_mm'] == current_month)
        ]['policy'].unique())
        
        # Count new policies (first appearance in this month AND active at month end)
        new_policies = policy_first_appearance[
            (policy_first_appearance['inforce_yy'] == current_year) & 
            (policy_first_appearance['inforce_mm'] == current_month) &
            (policy_first_appearance['policy'].isin(active_policies))
        ]['policy'].nunique()
        
        monthly_stats.at[idx, 'new_policies'] = new_policies
        monthly_stats.at[idx, 'renewed_policies'] = row['inforce_count'] - new_policies
    
    # Add cumulative policy count (total policies ever created up to this month)
    policy_first_appearance = df.groupby('policy')[['inforce_yy', 'inforce_mm']].min().reset_index()
    monthly_stats['cumulative_policy_count'] = 0
    
    for idx, row in monthly_stats.iterrows():
        current_year = row['year']
        current_month = row['month']
        
        # Count all policies created up to and including this month
        cumulative_count = policy_first_appearance[
            (policy_first_appearance['inforce_yy'] < current_year) |
            ((policy_first_appearance['inforce_yy'] == current_year) & 
             (policy_first_appearance['inforce_mm'] <= current_month))
        ]['policy'].nunique()
        
        monthly_stats.at[idx, 'cumulative_policy_count'] = cumulative_count
    
    # Flatten vehicle type distribution into separate columns (as percentages)
    vehicle_types = set()
    for dist in monthly_stats['vehicle_type_dist']:
        if isinstance(dist, dict):
            vehicle_types.update(dist.keys())
    
    # Add vehicle type columns
    for vtype in sorted(vehicle_types):
        monthly_stats[f'vehicle_type_{vtype}_pct'] = 0.0
    
    # Populate vehicle type percentages
    for idx, row in monthly_stats.iterrows():
        if isinstance(row['vehicle_type_dist'], dict):
            total_vehicles = sum(row['vehicle_type_dist'].values())
            if total_vehicles > 0:
                for vtype, count in row['vehicle_type_dist'].items():
                    percentage = (count / total_vehicles) * 100
                    monthly_stats.at[idx, f'vehicle_type_{vtype}_pct'] = round(percentage, 1)
    
    # Drop the original vehicle_type_dist column
    monthly_stats = monthly_stats.drop('vehicle_type_dist', axis=1)
    
    # Round numeric columns
    monthly_stats['avg_premium'] = monthly_stats['avg_premium'].round(2)
    monthly_stats['avg_driver_age'] = monthly_stats['avg_driver_age'].round(1)
    monthly_stats['avg_driver_count'] = monthly_stats['avg_driver_count'].round(2)
    monthly_stats['avg_vehicle_count'] = monthly_stats['avg_vehicle_count'].round(2)
    
    # Save to CSV
    output_file = output_dir / "monthly_distribution_summary.csv"
    monthly_stats.to_csv(output_file, index=False)
    print(f"  - Monthly distribution summary saved to {output_file}")
    
    # Print summary statistics
    print(f"  - Generated monthly statistics for {len(monthly_stats)} months")
    print(f"  - Date range: {monthly_stats['year'].min()}-{monthly_stats['month'].min():02d} to {monthly_stats['year'].max()}-{monthly_stats['month'].max():02d}")
    print(f"  - Average inforce policies per month: {monthly_stats['inforce_count'].mean():.0f}")
    print(f"  - Average premium per month: ${monthly_stats['avg_premium'].mean():.2f}")
    print(f"  - Final cumulative policy count: {monthly_stats['cumulative_policy_count'].iloc[-1]}")
