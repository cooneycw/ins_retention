"""
Minimal Generate Inforce View Module

A simplified version that works with the new family-centric data structure.
This generates the inforce view from driver-vehicle assignments.
"""

import csv
import yaml
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

# Load configuration
CONFIG_PATH = Path("config/policy_system_config.yaml")
with open(CONFIG_PATH, 'r') as f:
    CONFIG = yaml.safe_load(f)

# Extract constants from config
START_DATE = datetime.strptime(CONFIG['date_range']['start_date'], '%Y-%m-%d')
END_DATE = datetime.strptime(CONFIG['date_range']['end_date'], '%Y-%m-%d')


def create_inforce_view() -> None:
    """Generate monthly in-force view from driver-vehicle assignments."""
    print("Generating monthly in-force view...")
    print(f"Date range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
    
    # Load master data
    print("  - Loading master data...")
    master_data = _load_master_data()
    
    # Generate monthly snapshots
    print("  - Processing monthly snapshots...")
    inforce_records = []
    
    current_date = START_DATE
    month_count = 0
    
    while current_date <= END_DATE:
        month_count += 1
        year = current_date.year
        month = current_date.month
        
        print(f"    Processing month {month_count}: {year}-{month:02d}")
        
        # Generate inforce records for this month
        month_records = _generate_monthly_inforce_records(master_data, year, month)
        inforce_records.extend(month_records)
        
        # Move to next month
        if month == 12:
            current_date = current_date.replace(year=year + 1, month=1)
        else:
            current_date = current_date.replace(month=month + 1)
    
    # Save inforce view
    print("  - Saving inforce view...")
    _save_inforce_view(inforce_records)
    
    # Sort the file
    print("  - Sorting inforce file...")
    _sort_inforce_file()
    
    print("Inforce view generation completed successfully!")


def _load_master_data() -> Dict[str, List[Dict[str, Any]]]:
    """Load all master data files."""
    data_dir = Path(CONFIG['file_paths']['data']['policy_system'])
    
    master_data = {}
    
    # Load policies
    policies_file = data_dir / CONFIG['data_files']['master']['policies']
    master_data['policies'] = _load_csv_file(policies_file)
    print(f"    Loaded {len(master_data['policies'])} policies")
    
    # Load vehicles
    vehicles_file = data_dir / CONFIG['data_files']['master']['vehicles']
    master_data['vehicles'] = _load_csv_file(vehicles_file)
    print(f"    Loaded {len(master_data['vehicles'])} vehicles")
    
    # Load drivers
    drivers_file = data_dir / CONFIG['data_files']['master']['drivers']
    master_data['drivers'] = _load_csv_file(drivers_file)
    print(f"    Loaded {len(master_data['drivers'])} drivers")
    
    # Load assignments
    assignments_file = data_dir / CONFIG['data_files']['master']['driver_vehicle_assignments']
    master_data['assignments'] = _load_csv_file(assignments_file)
    print(f"    Loaded {len(master_data['assignments'])} assignments")
    
    return master_data


def _load_csv_file(filepath: Path) -> List[Dict[str, Any]]:
    """Load a CSV file and return as list of dictionaries."""
    if not filepath.exists():
        return []
    
    data = []
    with open(filepath, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    
    return data


def _generate_monthly_inforce_records(master_data: Dict[str, List[Dict[str, Any]]], 
                                    year: int, month: int) -> List[Dict[str, Any]]:
    """Generate inforce records for a specific month."""
    records = []
    
    # Create a date for the end of this month (inforce is end-of-month status)
    from datetime import datetime
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    current_date = datetime(year, month, last_day)
    
    # Get all policies that were active during this month
    active_policies = []
    for policy in master_data['policies']:
        if policy['status'] == 'cancelled':
            # Check if policy was cancelled before this month
            cancelled_date = _get_policy_cancellation_date(master_data, policy['family_id'])
            if cancelled_date and cancelled_date < current_date:
                continue  # Policy was cancelled before this month
        active_policies.append(policy)
    
    for policy in active_policies:
        family_id = policy['family_id']
        
        # Get all assignments for this family that were active during this month
        active_assignments = _get_active_assignments_for_month(master_data, family_id, current_date)
        
        for assignment in active_assignments:
            # Get vehicle for this assignment
            vehicle = next((v for v in master_data['vehicles'] 
                           if v['family_id'] == family_id and v['vehicle_no'] == assignment['vehicle_no']), None)
            if not vehicle:
                continue
            
            # Get driver for this assignment
            driver = next((d for d in master_data['drivers'] 
                          if d['family_id'] == family_id and d['driver_no'] == assignment['driver_no']), None)
            if not driver:
                continue
            
            # Calculate actual client tenure days
            policy_start_date = datetime.strptime(policy['start_date'], '%Y-%m-%d')
            tenure_days = (current_date - policy_start_date).days
            
            # Create inforce record
            inforce_record = {
                "inforce_yy": year,
                "inforce_mm": month,
                "policy": policy['policy_no'],
                "policy_expiry_date": policy['current_expiry_date'],
                "client_tenure_days": tenure_days,
                "vehicle_no": vehicle['vehicle_no'],
                "vehicle_vin": vehicle['vin'],
                "vehicle_type": vehicle['vehicle_type'],
                "model_year": vehicle['model_year'],
                "driver_no": driver['driver_no'],
                "driver_license_no": driver['license_no'],
                "driver_name": driver['driver_name'],
                "driver_type": driver['driver_type'],
                "assignment_type": assignment['assignment_type'],
                "exposure_factor": assignment['exposure_factor'],
                "driver_age": driver['age'],
                "premium_paid": policy['premium_paid']
            }
            
            records.append(inforce_record)
    
    return records


def _get_policy_cancellation_date(master_data: Dict[str, List[Dict[str, Any]]], family_id: str) -> datetime:
    """Get the cancellation date for a policy if it was cancelled."""
    from datetime import datetime
    
    # Check if policy is cancelled
    policy = next((p for p in master_data['policies'] if p['family_id'] == family_id), None)
    if not policy or policy['status'] != 'cancelled':
        return None
    
    # Look for policy cancellation in changes log
    changes_df = pd.read_csv('data/policy_system/policy_changes_log.csv')
    cancellation_changes = changes_df[
        (changes_df['family_id'] == family_id) & 
        (changes_df['change_type'] == 'policy_cancellation')
    ]
    
    if len(cancellation_changes) > 0:
        return datetime.strptime(cancellation_changes.iloc[0]['date'], '%Y-%m-%d')
    
    return None


def _get_active_assignments_for_month(master_data: Dict[str, List[Dict[str, Any]]], 
                                    family_id: str, current_date: datetime) -> List[Dict[str, Any]]:
    """Get all assignments that were active during a specific month."""
    from datetime import datetime
    
    active_assignments = []
    
    for assignment in master_data['assignments']:
        if assignment['family_id'] != family_id:
            continue
        
        # Check if assignment was active during this month
        effective_date = datetime.strptime(assignment['effective_date'], '%Y-%m-%d')
        
        # Assignment is active if:
        # 1. It was effective before or during this month
        # 2. It wasn't removed before this month
        if effective_date <= current_date:
            # Check if this assignment was removed before this month
            was_removed_before = False
            if assignment['status'] == 'removed':
                # Find when this assignment was removed
                changes_df = pd.read_csv('data/policy_system/policy_changes_log.csv')
                removal_changes = changes_df[
                    (changes_df['family_id'] == family_id) & 
                    (changes_df['vehicle_no'] == assignment['vehicle_no']) &
                    (changes_df['change_type'].isin(['vehicle_removal', 'vehicle_substitution']))
                ]
                
                if len(removal_changes) > 0:
                    removal_date = datetime.strptime(removal_changes.iloc[0]['date'], '%Y-%m-%d')
                    if removal_date < current_date:
                        was_removed_before = True
            
            if not was_removed_before:
                active_assignments.append(assignment)
    
    return active_assignments


def _save_inforce_view(records: List[Dict[str, Any]]) -> None:
    """Save inforce view to CSV file."""
    filepath = Path(CONFIG['file_paths']['data']['inforce']) / CONFIG['data_files']['inforce']['monthly_view']
    
    # Ensure directory exists
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    if not records:
        print(f"  Warning: No inforce records to save")
        return
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = records[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    
    print(f"  Saved {len(records)} inforce records to {filepath}")


def _sort_inforce_file() -> None:
    """Sort the inforce file by the required fields."""
    filepath = Path(CONFIG['file_paths']['data']['inforce']) / CONFIG['data_files']['inforce']['monthly_view']
    
    if not filepath.exists():
        return
    
    # Load data
    records = _load_csv_file(filepath)
    
    # Sort by required fields
    records.sort(key=lambda x: (
        x['policy'],
        x['policy_expiry_date'],
        x['vehicle_no'],
        x['driver_no'],
        int(x['inforce_yy']),
        int(x['inforce_mm'])
    ))
    
    # Save sorted data
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = records[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    
    print(f"    File sorted by: policy, policy_expiry_date, vehicle_no, driver_no, inforce_yy, inforce_mm")


if __name__ == "__main__":
    create_inforce_view()
