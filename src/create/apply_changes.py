"""
Apply Changes Module

Simulates monthly changes over 90 months including:
- Vehicle replacements and additions/removals
- Driver additions and removals
- Policy renewals and cancellations
- New policy additions

This is Step 2 of the insurance policy system generation.
"""

from __future__ import annotations

import csv
import random
import yaml
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

# Load configuration
CONFIG_PATH = Path("config/policy_system_config.yaml")
with open(CONFIG_PATH, 'r') as f:
    CONFIG = yaml.safe_load(f)

# Change probabilities (per month)
CHANGE_PROBABILITIES = {
    "vehicle_replacement": 0.02,      # 2% of policies per month
    "vehicle_addition": 0.01,         # 1% of policies per month
    "vehicle_removal": 0.005,         # 0.5% of policies per month
    "driver_addition": 0.015,         # 1.5% of policies per month
    "driver_removal": 0.01,           # 1% of policies per month
    "mid_year_cancellation": 0.002,   # 0.2% of policies per month (mid-year)
    "new_policy": 0.015,              # 1.5% new policies per month (18% annually)
}

# Retention system parameters
RETENTION_CONFIG = {
    "base_retention_rate": 0.85,      # 85% average retention
    "renewal_month_cancellation": 0.10,  # 10% cancel at renewal (most cancellations)
    "residual_effect_months": 3,      # 3 months of residual effect
    "residual_cancellation_rates": [0.03, 0.015, 0.005],  # 3%, 1.5%, 0.5% in months after renewal
}


def simulate_monthly_changes() -> None:
    """
    Apply monthly changes over 90 months (Jan 2018 - Jun 2025).
    
    This function processes:
    - Monthly vehicle changes (replacements, additions, removals)
    - Monthly driver changes (additions, removals)
    - Policy renewals and cancellations
    - New policy additions
    """
    print("Simulating monthly changes over 90 months...")
    print(f"Date range: {CONFIG['date_range']['start_date']} to {CONFIG['date_range']['end_date']}")
    
    # Load initial master data
    print("  - Loading initial master data...")
    master_data = _load_master_data()
    
    # Initialize change tracking
    all_changes = []
    current_month = 1
    current_year = 2018
    
    # Process each month
    for month_num in range(1, CONFIG['date_range']['total_months'] + 1):
        print(f"  - Processing month {month_num}: {current_year}-{current_month:02d}")
        
        # Apply changes for this month
        month_changes = _process_monthly_changes(current_month, current_year, master_data)
        all_changes.extend(month_changes)
        
        # Update month/year
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
    
    # Save all changes
    print("  - Saving changes log...")
    _save_changes_log(all_changes)
    
    # Save updated master data
    print("  - Saving updated master data...")
    _save_updated_master_data(master_data)
    
    # Validate system coherence
    print("  - Validating system coherence...")
    _validate_system_coherence(master_data)
    
    print("Monthly changes simulation completed successfully!")


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
            if 'premium_paid' in row:
                row['premium_paid'] = float(row['premium_paid'])
            if 'age' in row:
                row['age'] = int(row['age'])
            if 'model_year' in row:
                row['model_year'] = int(row['model_year'])
            if 'client_tenure_days' in row:
                row['client_tenure_days'] = int(row['client_tenure_days'])
            data.append(row)
    
    return data


def _process_monthly_changes(month: int, year: int, master_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Process all changes for a specific month."""
    month_changes = []
    current_date = f"{year}-{month:02d}-01"
    
    # Get active policies
    active_policies = [p for p in master_data['policies'] if p['status'] == 'active']
    
    # Apply vehicle changes
    vehicle_changes = _apply_vehicle_changes(month, year, master_data, active_policies)
    month_changes.extend(vehicle_changes)
    
    # Apply driver changes
    driver_changes = _apply_driver_changes(month, year, master_data, active_policies)
    month_changes.extend(driver_changes)
    
    # Apply policy renewals
    renewal_changes = _apply_policy_renewals(month, year, master_data, active_policies)
    month_changes.extend(renewal_changes)
    
    # Apply policy cancellations
    cancellation_changes = _apply_policy_cancellations(month, year, master_data, active_policies)
    month_changes.extend(cancellation_changes)
    
    # Add new policies
    new_policy_changes = _add_new_policies(month, year, master_data)
    month_changes.extend(new_policy_changes)
    
    return month_changes


def _apply_vehicle_changes(month: int, year: int, master_data: Dict[str, List[Dict[str, Any]]], 
                          active_policies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Apply vehicle changes for a specific month."""
    changes = []
    current_date = f"{year}-{month:02d}-01"
    
    # Vehicle replacements
    replacement_count = int(len(active_policies) * CHANGE_PROBABILITIES['vehicle_replacement'])
    for _ in range(replacement_count):
        if active_policies:
            policy = random.choice(active_policies)
            change = _replace_vehicle(policy, master_data, current_date)
            if change:
                changes.append(change)
    
    # Vehicle additions
    addition_count = int(len(active_policies) * CHANGE_PROBABILITIES['vehicle_addition'])
    for _ in range(addition_count):
        if active_policies:
            policy = random.choice(active_policies)
            change = _add_vehicle(policy, master_data, current_date)
            if change:
                changes.append(change)
    
    # Vehicle removals
    removal_count = int(len(active_policies) * CHANGE_PROBABILITIES['vehicle_removal'])
    for _ in range(removal_count):
        if active_policies:
            policy = random.choice(active_policies)
            change = _remove_vehicle(policy, master_data, current_date)
            if change:
                changes.append(change)
    
    return changes


def _apply_driver_changes(month: int, year: int, master_data: Dict[str, List[Dict[str, Any]]], 
                         active_policies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Apply driver changes for a specific month."""
    changes = []
    current_date = f"{year}-{month:02d}-01"
    
    # Driver additions (kids turning 16, new spouses)
    addition_count = int(len(active_policies) * CHANGE_PROBABILITIES['driver_addition'])
    for _ in range(addition_count):
        if active_policies:
            policy = random.choice(active_policies)
            change = _add_driver(policy, master_data, current_date)
            if change:
                changes.append(change)
    
    # Driver removals (kids turning 25, divorces)
    removal_count = int(len(active_policies) * CHANGE_PROBABILITIES['driver_removal'])
    for _ in range(removal_count):
        if active_policies:
            policy = random.choice(active_policies)
            change = _remove_driver(policy, master_data, current_date)
            if change:
                changes.append(change)
    
    return changes


def _apply_policy_renewals(month: int, year: int, master_data: Dict[str, List[Dict[str, Any]]], 
                          active_policies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Apply policy renewals for a specific month."""
    changes = []
    current_date = f"{year}-{month:02d}-01"
    
    # Find policies expiring this month
    expiring_policies = []
    for policy in active_policies:
        expiry_date = datetime.strptime(policy['current_expiry_date'], '%Y-%m-%d')
        if expiry_date.month == month and expiry_date.year == year:
            expiring_policies.append(policy)
    
    # Process renewals with retention logic
    for policy in expiring_policies:
        # Apply retention decision
        retention_decision = _make_retention_decision(policy, current_date)
        
        if retention_decision == "renew":
            change = _renew_policy(policy, master_data, current_date)
            if change:
                changes.append(change)
        else:  # cancel
            change = _cancel_policy(policy, master_data, current_date)
            if change:
                changes.append(change)
    
    return changes


def _apply_policy_cancellations(month: int, year: int, master_data: Dict[str, List[Dict[str, Any]]], 
                               active_policies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Apply policy cancellations for a specific month."""
    changes = []
    current_date = f"{year}-{month:02d}-01"
    
    # Mid-year cancellations (random churn)
    mid_year_count = int(len(active_policies) * CHANGE_PROBABILITIES['mid_year_cancellation'])
    for _ in range(mid_year_count):
        if active_policies:
            policy = random.choice(active_policies)
            change = _cancel_policy(policy, master_data, current_date)
            if change:
                changes.append(change)
    
    # Residual effect cancellations (months after renewal)
    residual_changes = _apply_residual_cancellations(month, year, master_data, active_policies)
    changes.extend(residual_changes)
    
    return changes


def _add_new_policies(month: int, year: int, master_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Add new policies for a specific month."""
    changes = []
    current_date = f"{year}-{month:02d}-01"
    
    # Get current active policy count
    active_policies = [p for p in master_data['policies'] if p['status'] == 'active']
    current_policy_count = len(active_policies)
    
    # Add new policies based on current policy count (not target)
    new_policy_count = int(current_policy_count * CHANGE_PROBABILITIES['new_policy'])
    for _ in range(new_policy_count):
        change = _create_new_policy(master_data, current_date)
        if change:
            changes.append(change)
    
    return changes


# Vehicle change operations
def _replace_vehicle(policy: Dict[str, Any], master_data: Dict[str, List[Dict[str, Any]]], 
                    current_date: str) -> Dict[str, Any]:
    """Replace a vehicle for a policy."""
    policy_no = policy["policy_no"]
    
    # Get active vehicles for this policy
    policy_vehicles = [v for v in master_data['vehicles'] 
                      if v["policy_no"] == policy_no and v["status"] == "active"]
    
    if not policy_vehicles:
        return None
    
    # Select a vehicle to replace
    old_vehicle = random.choice(policy_vehicles)
    
    # Create new vehicle
    new_vehicle_no = _get_next_vehicle_number_for_policy(master_data, policy_no)
    new_vin = _generate_vin()
    new_vehicle_type = random.choice(CONFIG['vehicles']['types'])
    
    # Model year: newer vehicles (2018-2025 for replacements)
    current_year = int(current_date[:4])
    new_model_year = random.randint(2018, current_year)
    
    new_vehicle = {
        "vehicle_no": new_vehicle_no,
        "policy_no": policy_no,
        "vin": new_vin,
        "vehicle_type": new_vehicle_type,
        "model_year": new_model_year,
        "effective_date": current_date,
        "status": "active"
    }
    
    # Update old vehicle status
    old_vehicle["status"] = "replaced"
    old_vehicle["replacement_date"] = current_date
    
    # Add new vehicle
    master_data['vehicles'].append(new_vehicle)
    
    return {
        "change_type": "vehicle_replacement",
        "policy_no": policy_no,
        "date": current_date,
        "old_vehicle_no": old_vehicle["vehicle_no"],
        "new_vehicle_no": new_vehicle_no,
        "old_vin": old_vehicle["vin"],
        "new_vin": new_vin,
        "old_vehicle_type": old_vehicle["vehicle_type"],
        "new_vehicle_type": new_vehicle_type
    }


def _add_vehicle(policy: Dict[str, Any], master_data: Dict[str, List[Dict[str, Any]]], 
                current_date: str) -> Dict[str, Any]:
    """Add a vehicle to a policy."""
    policy_no = policy["policy_no"]
    
    # Check if policy can have more vehicles (max 4)
    policy_vehicles = [v for v in master_data['vehicles'] 
                      if v["policy_no"] == policy_no and v["status"] == "active"]
    
    if len(policy_vehicles) >= 4:
        return None
    
    # Create new vehicle
    new_vehicle_no = _get_next_vehicle_number_for_policy(master_data, policy_no)
    new_vin = _generate_vin()
    new_vehicle_type = random.choice(CONFIG['vehicles']['types'])
    
    # Model year: newer vehicles (2018-2025 for additions)
    current_year = int(current_date[:4])
    new_model_year = random.randint(2018, current_year)
    
    new_vehicle = {
        "vehicle_no": new_vehicle_no,
        "policy_no": policy_no,
        "vin": new_vin,
        "vehicle_type": new_vehicle_type,
        "model_year": new_model_year,
        "effective_date": current_date,
        "status": "active"
    }
    
    # Add new vehicle
    master_data['vehicles'].append(new_vehicle)
    
    return {
        "change_type": "vehicle_addition",
        "policy_no": policy_no,
        "date": current_date,
        "new_vehicle_no": new_vehicle_no,
        "new_vin": new_vin,
        "new_vehicle_type": new_vehicle_type
    }


def _remove_vehicle(policy: Dict[str, Any], master_data: Dict[str, List[Dict[str, Any]]], 
                   current_date: str) -> Dict[str, Any]:
    """Remove a vehicle from a policy."""
    policy_no = policy["policy_no"]
    
    # Get active vehicles for this policy
    policy_vehicles = [v for v in master_data['vehicles'] 
                      if v["policy_no"] == policy_no and v["status"] == "active"]
    
    # Don't remove if only one vehicle left
    if len(policy_vehicles) <= 1:
        return None
    
    # Select a vehicle to remove
    removed_vehicle = random.choice(policy_vehicles)
    
    # Update vehicle status
    removed_vehicle["status"] = "removed"
    removed_vehicle["removal_date"] = current_date
    
    return {
        "change_type": "vehicle_removal",
        "policy_no": policy_no,
        "date": current_date,
        "removed_vehicle_no": removed_vehicle["vehicle_no"],
        "removed_vin": removed_vehicle["vin"],
        "removed_vehicle_type": removed_vehicle["vehicle_type"]
    }


def _add_driver(policy: Dict[str, Any], master_data: Dict[str, List[Dict[str, Any]]], 
               current_date: str) -> Dict[str, Any]:
    """Add a driver to a policy."""
    policy_no = policy["policy_no"]
    
    # Get active drivers for this policy
    policy_drivers = [d for d in master_data['drivers'] 
                     if d["policy_no"] == policy_no and d["status"] == "active"]
    
    # Check if policy can have more drivers (max 4)
    if len(policy_drivers) >= 4:
        return None
    
    # Determine driver type and age
    if len(policy_drivers) == 0:
        # First driver is primary
        driver_type = "primary"
        age = random.randint(*CONFIG['drivers']['age_ranges']['primary'])
    elif len(policy_drivers) == 1:
        # Second driver is usually spouse
        driver_type = "spouse"
        age = random.randint(*CONFIG['drivers']['age_ranges']['spouse'])
    else:
        # Additional drivers are secondary (kids)
        driver_type = "secondary"
        age = random.randint(*CONFIG['drivers']['age_ranges']['secondary'])
    
    # Create new driver
    new_driver_no = _get_next_driver_number_for_policy(master_data, policy_no)
    new_license = _generate_ontario_license()
    
    # Calculate birthday
    current_year = int(current_date[:4])
    birth_year = current_year - age
    birthday = f"{birth_year}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
    
    new_driver = {
        "driver_no": new_driver_no,
        "policy_no": policy_no,
        "license_no": new_license,
        "driver_type": driver_type,
        "age": age,
        "birthday": birthday,
        "effective_date": current_date,
        "status": "active"
    }
    
    # Add new driver
    master_data['drivers'].append(new_driver)
    
    return {
        "change_type": "driver_addition",
        "policy_no": policy_no,
        "date": current_date,
        "new_driver_no": new_driver_no,
        "new_license_no": new_license,
        "driver_type": driver_type,
        "age": age
    }


def _remove_driver(policy: Dict[str, Any], master_data: Dict[str, List[Dict[str, Any]]], 
                  current_date: str) -> Dict[str, Any]:
    """Remove a driver from a policy."""
    policy_no = policy["policy_no"]
    
    # Get active drivers for this policy
    policy_drivers = [d for d in master_data['drivers'] 
                     if d["policy_no"] == policy_no and d["status"] == "active"]
    
    # Don't remove if only one driver left
    if len(policy_drivers) <= 1:
        return None
    
    # Prefer to remove secondary drivers (kids) over primary/spouse
    secondary_drivers = [d for d in policy_drivers if d["driver_type"] == "secondary"]
    if secondary_drivers:
        removed_driver = random.choice(secondary_drivers)
    else:
        # If no secondary drivers, remove a non-primary driver
        non_primary = [d for d in policy_drivers if d["driver_type"] != "primary"]
        if non_primary:
            removed_driver = random.choice(non_primary)
        else:
            return None  # Don't remove the only primary driver
    
    # Update driver status
    removed_driver["status"] = "removed"
    removed_driver["removal_date"] = current_date
    
    return {
        "change_type": "driver_removal",
        "policy_no": policy_no,
        "date": current_date,
        "removed_driver_no": removed_driver["driver_no"],
        "removed_license_no": removed_driver["license_no"],
        "driver_type": removed_driver["driver_type"],
        "age": removed_driver["age"]
    }


def _renew_policy(policy: Dict[str, Any], master_data: Dict[str, List[Dict[str, Any]]], 
                 current_date: str) -> Dict[str, Any]:
    """Renew a policy."""
    policy_no = policy["policy_no"]
    
    # Calculate new expiry date (1 year later)
    current_expiry = datetime.strptime(policy["current_expiry_date"], '%Y-%m-%d')
    new_expiry = current_expiry + timedelta(days=365)
    
    # Update client tenure
    policy["client_tenure_days"] += 365
    
    # Apply inflation to premium (5% annual)
    inflation_rate = CONFIG['policies']['premium']['annual_inflation_rate']
    policy["premium_paid"] = round(policy["premium_paid"] * (1 + inflation_rate), 2)
    
    # Update expiry date
    old_expiry = policy["current_expiry_date"]
    policy["current_expiry_date"] = new_expiry.strftime('%Y-%m-%d')
    
    # Age all drivers by 1 year
    policy_drivers = [d for d in master_data['drivers'] 
                     if d["policy_no"] == policy_no and d["status"] == "active"]
    
    for driver in policy_drivers:
        driver["age"] += 1
    
    # Resequence vehicle and driver numbers on renewal (01, 02, 03...)
    _resequence_vehicles_and_drivers_on_renewal(policy_no, master_data)
    
    return {
        "change_type": "policy_renewal",
        "policy_no": policy_no,
        "date": current_date,
        "old_expiry_date": old_expiry,
        "new_expiry_date": policy["current_expiry_date"],
        "new_premium": policy["premium_paid"],
        "client_tenure_days": policy["client_tenure_days"]
    }


def _cancel_policy(policy: Dict[str, Any], master_data: Dict[str, List[Dict[str, Any]]], 
                  current_date: str) -> Dict[str, Any]:
    """Cancel a policy."""
    policy_no = policy["policy_no"]
    
    # Update policy status
    policy["status"] = "cancelled"
    policy["cancellation_date"] = current_date
    
    # Update all vehicles and drivers to cancelled
    policy_vehicles = [v for v in master_data['vehicles'] 
                      if v["policy_no"] == policy_no and v["status"] == "active"]
    policy_drivers = [d for d in master_data['drivers'] 
                     if d["policy_no"] == policy_no and d["status"] == "active"]
    
    for vehicle in policy_vehicles:
        vehicle["status"] = "cancelled"
        vehicle["cancellation_date"] = current_date
    
    for driver in policy_drivers:
        driver["status"] = "cancelled"
        driver["cancellation_date"] = current_date
    
    return {
        "change_type": "policy_cancellation",
        "policy_no": policy_no,
        "date": current_date,
        "vehicles_cancelled": len(policy_vehicles),
        "drivers_cancelled": len(policy_drivers)
    }


def _create_new_policy(master_data: Dict[str, List[Dict[str, Any]]], 
                      current_date: str) -> Dict[str, Any]:
    """Create a new policy."""
    # Get next policy number
    existing_policies = master_data['policies']
    if existing_policies:
        max_policy_no = max(int(p["policy_no"]) for p in existing_policies)
        new_policy_no = f"{max_policy_no + 1:08d}"
    else:
        new_policy_no = "00000001"
    
    # Create new policy
    start_date = current_date
    expiry_date = (datetime.strptime(current_date, '%Y-%m-%d') + timedelta(days=365)).strftime('%Y-%m-%d')
    
    # Determine family type
    family_type = _select_family_type()
    
    new_policy = {
        "policy_no": new_policy_no,
        "start_date": start_date,
        "current_expiry_date": expiry_date,
        "premium_paid": 0.0,  # Will be calculated after vehicles/drivers are added
        "client_tenure_days": 0,
        "family_type": family_type,
        "status": "active"
    }
    
    # Create vehicles and drivers for new policy
    vehicles = _create_vehicles_for_policy(new_policy, master_data)
    drivers = _create_drivers_for_policy(new_policy, master_data)
    
    # Calculate premium
    from src.create.build_initial_state import _calculate_single_policy_premium
    new_policy["premium_paid"] = _calculate_single_policy_premium(vehicles, drivers)
    
    # Add to master data
    master_data['policies'].append(new_policy)
    master_data['vehicles'].extend(vehicles)
    master_data['drivers'].extend(drivers)
    
    return {
        "change_type": "new_policy",
        "policy_no": new_policy_no,
        "date": current_date,
        "family_type": family_type,
        "vehicles_created": len(vehicles),
        "drivers_created": len(drivers),
        "premium": new_policy["premium_paid"]
    }


def _save_changes_log(changes: List[Dict[str, Any]]) -> None:
    """Save changes to the policy changes log."""
    filepath = Path(CONFIG['file_paths']['data']['policy_system']) / CONFIG['data_files']['master']['changes_log']
    
    if not changes:
        print(f"  No changes to save to {filepath}")
        return
    
    # Get all unique fieldnames from all changes
    all_fieldnames = set()
    for change in changes:
        all_fieldnames.update(change.keys())
    
    # Sort fieldnames for consistent output
    fieldnames = sorted(list(all_fieldnames))
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Write each change, filling missing fields with empty strings
        for change in changes:
            row = {field: change.get(field, '') for field in fieldnames}
            writer.writerow(row)
    
    print(f"  Saved {len(changes)} changes to {filepath}")


def _save_updated_master_data(master_data: Dict[str, List[Dict[str, Any]]]) -> None:
    """Save updated master data files."""
    data_dir = Path(CONFIG['file_paths']['data']['policy_system'])
    
    # Save policies
    policies_file = data_dir / CONFIG['data_files']['master']['policies']
    _save_csv_file(master_data['policies'], policies_file)
    
    # Save vehicles
    vehicles_file = data_dir / CONFIG['data_files']['master']['vehicles']
    _save_csv_file(master_data['vehicles'], vehicles_file)
    
    # Save drivers
    drivers_file = data_dir / CONFIG['data_files']['master']['drivers']
    _save_csv_file(master_data['drivers'], drivers_file)


# Helper functions
def _get_next_vehicle_number_for_policy(master_data: Dict[str, List[Dict[str, Any]]], policy_no: str) -> str:
    """Get the next available vehicle number for a specific policy."""
    policy_vehicles = [v for v in master_data['vehicles'] 
                      if v["policy_no"] == policy_no and v["status"] == "active"]
    
    if policy_vehicles:
        max_vehicle_no = max(int(v["vehicle_no"]) for v in policy_vehicles)
        return f"{max_vehicle_no + 1:02d}"
    else:
        return "01"


def _get_next_driver_number_for_policy(master_data: Dict[str, List[Dict[str, Any]]], policy_no: str) -> str:
    """Get the next available driver number for a specific policy."""
    policy_drivers = [d for d in master_data['drivers'] 
                     if d["policy_no"] == policy_no and d["status"] == "active"]
    
    if policy_drivers:
        max_driver_no = max(int(d["driver_no"]) for d in policy_drivers)
        return f"{max_driver_no + 1:02d}"
    else:
        return "01"


def _resequence_vehicles_and_drivers_on_renewal(policy_no: str, master_data: Dict[str, List[Dict[str, Any]]]) -> None:
    """Resequence vehicle and driver numbers on policy renewal (01, 02, 03...)."""
    # Resequence vehicles
    policy_vehicles = [v for v in master_data['vehicles'] 
                      if v["policy_no"] == policy_no and v["status"] == "active"]
    
    # Sort vehicles by current vehicle_no to maintain some order
    policy_vehicles.sort(key=lambda x: int(x["vehicle_no"]))
    
    for i, vehicle in enumerate(policy_vehicles):
        vehicle["vehicle_no"] = f"{i + 1:02d}"  # 01, 02, 03...
    
    # Resequence drivers
    policy_drivers = [d for d in master_data['drivers'] 
                     if d["policy_no"] == policy_no and d["status"] == "active"]
    
    # Sort drivers by driver type (primary first, then spouse, then secondary)
    driver_type_order = {"primary": 1, "spouse": 2, "secondary": 3}
    policy_drivers.sort(key=lambda x: (driver_type_order.get(x["driver_type"], 4), int(x["driver_no"])))
    
    for i, driver in enumerate(policy_drivers):
        driver["driver_no"] = f"{i + 1:02d}"  # 01, 02, 03...


def _generate_vin() -> str:
    """Generate a realistic 17-character VIN."""
    chars = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
    vin = ""
    
    for i in range(17):
        if i in [8]:  # Check digit position
            vin += random.choice("0123456789")
        else:
            vin += random.choice(chars)
    
    return vin


def _generate_ontario_license() -> str:
    """Generate Ontario-style driver license number."""
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    numbers = "0123456789"
    
    license_no = ""
    license_no += "".join(random.choices(letters, k=4))
    license_no += "".join(random.choices(numbers, k=3))
    license_no += "".join(random.choices(letters, k=2))
    
    return license_no


def _select_family_type() -> str:
    """Select family type based on distribution."""
    family_types = CONFIG['family_types']
    rand = random.random()
    cumulative = 0
    
    for family_type, details in family_types.items():
        cumulative += details['percentage']
        if rand <= cumulative:
            return family_type
    
    return "family_single_vehicle"  # fallback


def _create_vehicles_for_policy(policy: Dict[str, Any], master_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Create vehicles for a new policy."""
    vehicles = []
    family_type = policy["family_type"]
    policy_no = policy["policy_no"]
    
    # Determine number of vehicles based on family type
    if family_type == "single_person_single_vehicle":
        num_vehicles = 1
    elif family_type == "single_driver_multi_vehicle":
        num_vehicles = random.randint(2, 3)
    elif family_type == "family_multi_vehicle":
        vehicle_weights = CONFIG['family_types']['family_multi_vehicle']['vehicle_weights']
        num_vehicles = random.choices([2, 3, 4], weights=vehicle_weights)[0]
    else:  # family_single_vehicle
        num_vehicles = 1
    
    # Create vehicles
    for v in range(num_vehicles):
        vehicle_no = _get_next_vehicle_number_for_policy(master_data, policy_no)
        vin = _generate_vin()
        vehicle_type = random.choice(CONFIG['vehicles']['types'])
        
        # Model year for new policies (2018-2025)
        current_year = int(policy["start_date"][:4])
        model_year = random.randint(2018, current_year)
        
        vehicle = {
            "vehicle_no": vehicle_no,
            "policy_no": policy_no,
            "vin": vin,
            "vehicle_type": vehicle_type,
            "model_year": model_year,
            "effective_date": policy["start_date"],
            "status": "active"
        }
        
        vehicles.append(vehicle)
    
    return vehicles


def _create_drivers_for_policy(policy: Dict[str, Any], master_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Create drivers for a new policy."""
    drivers = []
    family_type = policy["family_type"]
    policy_no = policy["policy_no"]
    
    # Determine number of drivers based on family type
    if family_type == "single_person_single_vehicle":
        num_drivers = 1
    elif family_type == "single_driver_multi_vehicle":
        num_drivers = 1
    elif family_type == "family_multi_vehicle":
        num_drivers = random.randint(2, 4)
    else:  # family_single_vehicle
        num_drivers = random.randint(1, 2)
    
    # Create drivers
    for d in range(num_drivers):
        driver_no = _get_next_driver_number_for_policy(master_data, policy_no)
        license_no = _generate_ontario_license()
        
        # Determine driver type and age
        if d == 0:  # Primary driver
            driver_type = "primary"
            age = random.randint(*CONFIG['drivers']['age_ranges']['primary'])
        elif family_type in ["family_multi_vehicle", "family_single_vehicle"] and d == 1:
            # Spouse
            driver_type = "spouse"
            age = random.randint(*CONFIG['drivers']['age_ranges']['spouse'])
        else:
            # Secondary drivers (kids)
            driver_type = "secondary"
            age = random.randint(*CONFIG['drivers']['age_ranges']['secondary'])
        
        # Calculate birthday
        current_year = int(policy["start_date"][:4])
        birth_year = current_year - age
        birthday = f"{birth_year}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
        
        driver = {
            "driver_no": driver_no,
            "policy_no": policy_no,
            "license_no": license_no,
            "driver_type": driver_type,
            "age": age,
            "birthday": birthday,
            "effective_date": policy["start_date"],
            "status": "active"
        }
        
        drivers.append(driver)
    
    return drivers


def _make_retention_decision(policy: Dict[str, Any], current_date: str) -> str:
    """Make retention decision for a policy at renewal."""
    # Apply renewal month cancellation rate (12% cancel at renewal)
    renewal_cancellation_rate = RETENTION_CONFIG["renewal_month_cancellation"]
    
    # Random decision
    rand = random.random()
    if rand < renewal_cancellation_rate:
        return "cancel"
    else:
        return "renew"


def _apply_residual_cancellations(month: int, year: int, master_data: Dict[str, List[Dict[str, Any]]], 
                                 active_policies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Apply residual effect cancellations (months after renewal)."""
    changes = []
    current_date = f"{year}-{month:02d}-01"
    
    # Find policies that renewed in the last few months
    residual_rates = RETENTION_CONFIG["residual_cancellation_rates"]
    residual_months = RETENTION_CONFIG["residual_effect_months"]
    
    for months_ago in range(1, residual_months + 1):
        # Calculate the month when these policies renewed
        renewal_month = month - months_ago
        renewal_year = year
        if renewal_month <= 0:
            renewal_month += 12
            renewal_year -= 1
        
        # Find policies that renewed in that month
        renewed_policies = []
        for policy in active_policies:
            # Check if policy renewed in the target month
            if _was_policy_renewed_in_month(policy, renewal_month, renewal_year):
                renewed_policies.append(policy)
        
        # Apply residual cancellation rate
        if months_ago <= len(residual_rates):
            cancellation_rate = residual_rates[months_ago - 1]
            cancellation_count = int(len(renewed_policies) * cancellation_rate)
            
            for _ in range(cancellation_count):
                if renewed_policies:
                    policy = random.choice(renewed_policies)
                    change = _cancel_policy(policy, master_data, current_date)
                    if change:
                        changes.append(change)
    
    return changes


def _was_policy_renewed_in_month(policy: Dict[str, Any], target_month: int, target_year: int) -> bool:
    """Check if a policy was renewed in a specific month."""
    # This is a simplified check - in a real system, we'd track renewal history
    # For now, we'll use the policy's current expiry date to infer renewal timing
    try:
        expiry_date = datetime.strptime(policy['current_expiry_date'], '%Y-%m-%d')
        # If expiry is 1 year after the target month, it was likely renewed then
        expected_renewal = datetime(target_year, target_month, 1) + timedelta(days=365)
        return abs((expiry_date - expected_renewal).days) < 30  # Within 30 days
    except:
        return False


def _validate_system_coherence(master_data: Dict[str, List[Dict[str, Any]]]) -> None:
    """Validate that the system remains coherent after changes."""
    policies = master_data['policies']
    vehicles = master_data['vehicles']
    drivers = master_data['drivers']
    
    print("    Validating system coherence...")
    
    # Check 1: All active policies have at least one active vehicle
    active_policies = [p for p in policies if p['status'] == 'active']
    for policy in active_policies:
        policy_vehicles = [v for v in vehicles 
                          if v['policy_no'] == policy['policy_no'] and v['status'] == 'active']
        if not policy_vehicles:
            print(f"    WARNING: Policy {policy['policy_no']} has no active vehicles")
    
    # Check 2: All active policies have at least one active driver
    for policy in active_policies:
        policy_drivers = [d for d in drivers 
                         if d['policy_no'] == policy['policy_no'] and d['status'] == 'active']
        if not policy_drivers:
            print(f"    WARNING: Policy {policy['policy_no']} has no active drivers")
    
    # Check 3: All active vehicles belong to active policies
    active_vehicles = [v for v in vehicles if v['status'] == 'active']
    for vehicle in active_vehicles:
        policy = next((p for p in policies if p['policy_no'] == vehicle['policy_no']), None)
        if not policy or policy['status'] != 'active':
            print(f"    WARNING: Vehicle {vehicle['vehicle_no']} belongs to inactive policy {vehicle['policy_no']}")
    
    # Check 4: All active drivers belong to active policies
    active_drivers = [d for d in drivers if d['status'] == 'active']
    for driver in active_drivers:
        policy = next((p for p in policies if p['policy_no'] == driver['policy_no']), None)
        if not policy or policy['status'] != 'active':
            print(f"    WARNING: Driver {driver['driver_no']} belongs to inactive policy {driver['policy_no']}")
    
    # Check 5: Policy counts
    active_policy_count = len(active_policies)
    cancelled_policy_count = len([p for p in policies if p['status'] == 'cancelled'])
    
    print(f"    Active policies: {active_policy_count}")
    print(f"    Cancelled policies: {cancelled_policy_count}")
    print(f"    Total policies: {len(policies)}")
    print(f"    Total vehicles: {len(vehicles)}")
    print(f"    Total drivers: {len(drivers)}")
    
    # Check 6: Premium validation
    for policy in active_policies:
        if policy['premium_paid'] <= 0:
            print(f"    WARNING: Policy {policy['policy_no']} has invalid premium: {policy['premium_paid']}")
    
    print("    System coherence validation completed")


def _save_csv_file(data: List[Dict[str, Any]], filepath: Path) -> None:
    """Save data to CSV file."""
    if not data:
        print(f"  Warning: No data to save to {filepath}")
        return
    
    # Get all unique fieldnames from all records
    all_fieldnames = set()
    for record in data:
        all_fieldnames.update(record.keys())
    
    # Sort fieldnames for consistent output
    fieldnames = sorted(list(all_fieldnames))
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Write each record, filling missing fields with empty strings
        for record in data:
            row = {field: record.get(field, '') for field in fieldnames}
            writer.writerow(row)
    
    print(f"  Saved {len(data)} records to {filepath}")
