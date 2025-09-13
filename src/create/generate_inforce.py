"""
Generate Inforce View Module

Creates the final monthly in-force view CSV file with 90 months of data.
This is Step 3 of the insurance policy system generation.
"""

from __future__ import annotations

import csv
import yaml
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

# Load configuration
CONFIG_PATH = Path("config/policy_system_config.yaml")
with open(CONFIG_PATH, 'r') as f:
    CONFIG = yaml.safe_load(f)


def create_inforce_view() -> None:
    """
    Generate the final monthly in-force view.
    
    This function creates a flat CSV file with monthly snapshots of:
    - Policy information with expiry dates and client tenure
    - Vehicle data with VINs, types, and model years
    - Driver data with license numbers and driver types
    - Premium amounts with inflation
    - Monthly inforce status tracking
    """
    print("Generating monthly in-force view...")
    print(f"Date range: {CONFIG['date_range']['start_date']} to {CONFIG['date_range']['end_date']}")
    
    # Load master data
    print("  - Loading master data...")
    master_data = _load_master_data()
    
    # Process monthly snapshots
    print("  - Processing monthly snapshots...")
    inforce_data = _process_monthly_snapshots(master_data)
    
    # Save inforce view
    print("  - Saving inforce view...")
    _save_inforce_view(inforce_data)
    
    print("Inforce view generation completed successfully!")


def _load_master_data() -> Dict[str, List[Dict[str, Any]]]:
    """Load all master data files."""
    data_dir = Path(CONFIG['file_paths']['data']['policy_system'])
    master_data = {}
    
    # Load policies
    policies_file = data_dir / CONFIG['data_files']['master']['policies']
    master_data['policies'] = _load_csv_file(policies_file)
    
    # Load vehicles
    vehicles_file = data_dir / CONFIG['data_files']['master']['vehicles']
    master_data['vehicles'] = _load_csv_file(vehicles_file)
    
    # Load drivers
    drivers_file = data_dir / CONFIG['data_files']['master']['drivers']
    master_data['drivers'] = _load_csv_file(drivers_file)
    
    print(f"    Loaded {len(master_data['policies'])} policies")
    print(f"    Loaded {len(master_data['vehicles'])} vehicles")
    print(f"    Loaded {len(master_data['drivers'])} drivers")
    
    return master_data


def _load_csv_file(filepath: Path) -> List[Dict[str, Any]]:
    """Load data from CSV file."""
    if not filepath.exists():
        return []
    
    data = []
    with open(filepath, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Convert numeric fields
            if 'premium_paid' in row and row['premium_paid']:
                row['premium_paid'] = float(row['premium_paid'])
            if 'age' in row and row['age']:
                row['age'] = int(row['age'])
            if 'model_year' in row and row['model_year']:
                row['model_year'] = int(row['model_year'])
            if 'client_tenure_days' in row and row['client_tenure_days']:
                row['client_tenure_days'] = int(row['client_tenure_days'])
            data.append(row)
    
    return data


def _process_monthly_snapshots(master_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Process monthly snapshots for all 90 months."""
    inforce_data = []
    
    # Get date range
    start_date = datetime.strptime(CONFIG['date_range']['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(CONFIG['date_range']['end_date'], '%Y-%m-%d')
    
    current_date = start_date
    month_count = 0
    
    while current_date <= end_date:
        month_count += 1
        year = current_date.year
        month = current_date.month
        
        print(f"    Processing month {month_count}: {year}-{month:02d}")
        
        # Get active policies for this month
        active_policies = _get_active_policies_for_month(master_data, year, month)
        
        # Create inforce records for each active policy
        for policy in active_policies:
            policy_no = policy["policy_no"]
            
            # Get active vehicles and drivers for this policy
            policy_vehicles = _get_active_vehicles_for_policy(master_data, policy_no, year, month)
            policy_drivers = _get_active_drivers_for_policy(master_data, policy_no, year, month)
            
            # Create inforce records for each vehicle-driver combination
            for vehicle in policy_vehicles:
                for driver in policy_drivers:
                    inforce_record = {
                        "inforce_yy": year,
                        "inforce_mm": month,
                        "policy": policy_no,
                        "policy_expiry_date": policy["current_expiry_date"],
                        "client_tenure_days": policy["client_tenure_days"],
                        "vehicle_no": vehicle["vehicle_no"],
                        "vehicle_vin": vehicle["vin"],
                        "vehicle_type": vehicle["vehicle_type"],
                        "model_year": vehicle["model_year"],
                        "driver_no": driver["driver_no"],
                        "driver_license_no": driver["license_no"],
                        "driver_type": driver["driver_type"],
                        "driver_age": driver["age"],
                        "premium_paid": policy["premium_paid"]
                    }
                    inforce_data.append(inforce_record)
        
        # Move to next month
        if month == 12:
            current_date = current_date.replace(year=year + 1, month=1)
        else:
            current_date = current_date.replace(month=month + 1)
    
    print(f"    Generated {len(inforce_data)} inforce records")
    return inforce_data


def _get_active_policies_for_month(master_data: Dict[str, List[Dict[str, Any]]], 
                                  year: int, month: int) -> List[Dict[str, Any]]:
    """Get policies that are active for a specific month (last day of month)."""
    active_policies = []
    
    # Get the last day of the month
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    
    # Last day of current month
    current_date = next_month - timedelta(days=1)
    
    for policy in master_data['policies']:
        # Check if policy is in force for this month (last day)
        start_date = datetime.strptime(policy['start_date'], '%Y-%m-%d')
        expiry_date = datetime.strptime(policy['current_expiry_date'], '%Y-%m-%d')
        
        # Policy must have started by this date
        if start_date > current_date:
            continue
        
        # Policy must not have expired by this date
        if current_date > expiry_date:
            continue
        
        # Check if policy was cancelled before this date
        if 'cancellation_date' in policy and policy['cancellation_date']:
            cancellation_date = datetime.strptime(policy['cancellation_date'], '%Y-%m-%d')
            if current_date >= cancellation_date:
                continue
        
        active_policies.append(policy)
    
    return active_policies


def _get_active_vehicles_for_policy(master_data: Dict[str, List[Dict[str, Any]]], 
                                   policy_no: str, year: int, month: int) -> List[Dict[str, Any]]:
    """Get vehicles that are active for a specific policy and month (last day of month)."""
    active_vehicles = []
    
    # Get the last day of the month
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    
    # Last day of current month
    current_date = next_month - timedelta(days=1)
    
    for vehicle in master_data['vehicles']:
        if vehicle['policy_no'] != policy_no:
            continue
        
        # Check if vehicle is in force for this month (last day)
        effective_date = datetime.strptime(vehicle['effective_date'], '%Y-%m-%d')
        
        # Vehicle must have been effective by this date
        if effective_date > current_date:
            continue
        
        # Check if vehicle was removed before this date
        if 'removal_date' in vehicle and vehicle['removal_date']:
            removal_date = datetime.strptime(vehicle['removal_date'], '%Y-%m-%d')
            if current_date >= removal_date:
                continue
        
        # Check if vehicle was replaced before this date
        if 'replacement_date' in vehicle and vehicle['replacement_date']:
            replacement_date = datetime.strptime(vehicle['replacement_date'], '%Y-%m-%d')
            if current_date >= replacement_date:
                continue
        
        active_vehicles.append(vehicle)
    
    return active_vehicles


def _get_active_drivers_for_policy(master_data: Dict[str, List[Dict[str, Any]]], 
                                  policy_no: str, year: int, month: int) -> List[Dict[str, Any]]:
    """Get drivers that are active for a specific policy and month (last day of month)."""
    active_drivers = []
    
    # Get the last day of the month
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    
    # Last day of current month
    current_date = next_month - timedelta(days=1)
    
    for driver in master_data['drivers']:
        if driver['policy_no'] != policy_no:
            continue
        
        # Check if driver is in force for this month (last day)
        effective_date = datetime.strptime(driver['effective_date'], '%Y-%m-%d')
        
        # Driver must have been effective by this date
        if effective_date > current_date:
            continue
        
        # Check if driver was removed before this date
        if 'removal_date' in driver and driver['removal_date']:
            removal_date = datetime.strptime(driver['removal_date'], '%Y-%m-%d')
            if current_date >= removal_date:
                continue
        
        # Age the driver based on current date (last day of month)
        aged_driver = driver.copy()
        birth_date = datetime.strptime(driver['birthday'], '%Y-%m-%d')
        age = (current_date - birth_date).days // 365
        aged_driver['age'] = age
        
        active_drivers.append(aged_driver)
    
    return active_drivers


def _save_inforce_view(data: List[Dict[str, Any]]) -> None:
    """Save the inforce view to CSV file and sort it."""
    filepath = Path(CONFIG['file_paths']['data']['inforce']) / CONFIG['data_files']['inforce']['monthly_view']
    
    # Ensure directory exists
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    # Write mode - create new file each time
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        if data:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
    
    print(f"  Saved {len(data)} records to {filepath}")
    
    # Sort the file after writing
    print("  - Sorting inforce file...")
    _sort_inforce_file(filepath)


def _sort_inforce_file(filepath: Path) -> None:
    """Sort the inforce CSV file by: policy, policy_expiry_date, vehicle_no, driver_no, inforce_yy, inforce_mm."""
    # Read all data from the file
    data = []
    with open(filepath, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    
    # Sort the data by: policy, policy_expiry_date, vehicle_no, driver_no, inforce_yy, inforce_mm
    # This groups each policy's complete lifecycle together
    data.sort(key=lambda x: (
        x['policy'],
        x['policy_expiry_date'],
        x['vehicle_no'],
        x['driver_no'],
        int(x['inforce_yy']),
        int(x['inforce_mm'])
    ))
    
    # Write the sorted data back to the file
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        if data:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
    
    print(f"    File sorted by: policy, policy_expiry_date, vehicle_no, driver_no, inforce_yy, inforce_mm")