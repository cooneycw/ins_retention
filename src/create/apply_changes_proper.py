"""
Proper Apply Changes Module

Implements the actual vehicle changes and family evolution according to YAML configuration.
This replaces the minimal version with real change simulation.
"""

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

# Extract constants from config
START_DATE = datetime.strptime(CONFIG['date_range']['start_date'], '%Y-%m-%d')
END_DATE = datetime.strptime(CONFIG['date_range']['end_date'], '%Y-%m-%d')


def simulate_monthly_changes() -> None:
    """Simulate monthly changes over the full period with actual vehicle changes."""
    print("Simulating monthly changes over 90 months...")
    print(f"Date range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
    
    # Load initial master data
    print("  - Loading initial master data...")
    master_data = _load_master_data()
    
    # Process monthly changes
    changes = []
    current_date = START_DATE
    month_count = 0
    
    while current_date <= END_DATE:
        month_count += 1
        year = current_date.year
        month = current_date.month
        
        print(f"  - Processing month {month_count}: {year}-{month:02d}")
        
        # Apply vehicle changes for this month
        month_changes = _apply_vehicle_changes(master_data, current_date)
        changes.extend(month_changes)
        
        # Apply family evolution for this month
        evolution_changes = _apply_family_evolution(master_data, current_date)
        changes.extend(evolution_changes)
        
        # Apply annual premium increases (every January)
        if month == 1:
            _apply_annual_premium_increases(master_data, year)
        
        # Move to next month
        if month == 12:
            current_date = current_date.replace(year=year + 1, month=1)
        else:
            current_date = current_date.replace(month=month + 1)
    
    # Save changes log
    print("  - Saving changes log...")
    _save_changes_log(changes)
    
    # Save updated master data
    print("  - Saving updated master data...")
    _save_master_data(master_data)
    
    print("Monthly changes simulation completed successfully!")


def _load_master_data() -> Dict[str, List[Dict[str, Any]]]:
    """Load all master data files."""
    data_dir = Path(CONFIG['file_paths']['data']['policy_system'])
    
    master_data = {}
    
    # Load families
    families_file = data_dir / CONFIG['data_files']['master']['families']
    master_data['families'] = _load_csv_file(families_file)
    print(f"    Loaded {len(master_data['families'])} families")
    
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


def _apply_vehicle_changes(master_data: Dict[str, List[Dict[str, Any]]], 
                          current_date: datetime) -> List[Dict[str, Any]]:
    """Apply vehicle changes for the current month."""
    changes = []
    
    # Get vehicle change probabilities
    vehicle_changes = CONFIG['changes']['vehicle_changes']
    
    # Process each family
    for family in master_data['families']:
        if family['status'] != 'active':
            continue
        
        family_id = family['family_id']
        family_type = family['family_type']
        family_config = CONFIG['family_types'][family_type]
        
        # Skip if family type doesn't allow vehicle changes
        if not (family_config['can_add_vehicles'] or 
                family_config['can_remove_vehicles'] or 
                family_config['can_substitute_vehicles']):
            continue
        
        # Get current vehicles for this family
        current_vehicles = [v for v in master_data['vehicles'] 
                           if v['family_id'] == family_id and v['status'] == 'active']
        current_vehicle_count = len(current_vehicles)
        max_vehicles = family_config['max_vehicles']
        
        # Vehicle additions
        if (family_config['can_add_vehicles'] and 
            current_vehicle_count < max_vehicles and
            random.random() < vehicle_changes['additions']['probability']):
            
            change = _add_vehicle(master_data, family_id, current_date)
            if change:
                changes.append(change)
        
        # Vehicle removals
        if (family_config['can_remove_vehicles'] and 
            current_vehicle_count > 0 and  # Must have at least one vehicle to remove
            random.random() < vehicle_changes['removals']['probability']):
            
            change = _remove_vehicle(master_data, family_id, current_date)
            if change:
                changes.append(change)
        
        # Vehicle substitutions
        if (family_config['can_substitute_vehicles'] and 
            current_vehicle_count > 0 and
            random.random() < vehicle_changes['substitutions']['probability']):
            
            change = _substitute_vehicle(master_data, family_id, current_date)
            if change:
                changes.append(change)
    
    return changes


def _add_vehicle(master_data: Dict[str, List[Dict[str, Any]]], 
                 family_id: str, current_date: datetime) -> Dict[str, Any]:
    """Add a new vehicle to a family."""
    # Get family info
    family = next(f for f in master_data['families'] if f['family_id'] == family_id)
    
    # Get next vehicle number
    family_vehicles = [v for v in master_data['vehicles'] if v['family_id'] == family_id]
    next_vehicle_no = max([int(v['vehicle_no']) for v in family_vehicles]) + 1
    
    # Generate new vehicle
    vehicle_types = CONFIG['vehicles']['types']
    vehicle_type = random.choice(vehicle_types)
    
    model_year = random.randint(2001, 2018)
    vin = f"VIN{family_id}{next_vehicle_no:03d}{model_year}"
    
    new_vehicle = {
        'vehicle_no': str(next_vehicle_no),
        'family_id': family_id,
        'vin': vin,
        'vehicle_type': vehicle_type,
        'model_year': str(model_year),
        'status': 'active'
    }
    
    master_data['vehicles'].append(new_vehicle)
    
    # Create assignment for the new vehicle (primary driver gets it)
    primary_driver = next(d for d in master_data['drivers'] 
                         if d['family_id'] == family_id and d['driver_type'] == 'adult')
    
    new_assignment = {
        'assignment_id': f"ASS{family_id}{next_vehicle_no:03d}",
        'family_id': family_id,
        'vehicle_no': str(next_vehicle_no),
        'driver_no': primary_driver['driver_no'],
        'assignment_type': 'primary',
        'exposure_factor': '1.0',
        'effective_date': current_date.strftime('%Y-%m-%d'),
        'status': 'active'
    }
    
    master_data['assignments'].append(new_assignment)
    
    return {
        'change_type': 'vehicle_addition',
        'date': current_date.strftime('%Y-%m-%d'),
        'family_id': family_id,
        'vehicle_no': str(next_vehicle_no),
        'vehicle_type': vehicle_type,
        'reason': random.choice(CONFIG['changes']['vehicle_changes']['additions']['reasons'])
    }


def _remove_vehicle(master_data: Dict[str, List[Dict[str, Any]]], 
                   family_id: str, current_date: datetime) -> Dict[str, Any]:
    """Remove a vehicle from a family."""
    # Get vehicles for this family
    family_vehicles = [v for v in master_data['vehicles'] 
                      if v['family_id'] == family_id and v['status'] == 'active']
    
    if len(family_vehicles) == 0:
        return None  # No vehicles to remove
    
    # Select a vehicle to remove (any vehicle can be removed)
    vehicle_to_remove = random.choice(family_vehicles)
    
    vehicle_no = vehicle_to_remove['vehicle_no']
    
    # Mark vehicle as removed
    vehicle_to_remove['status'] = 'removed'
    
    # Mark assignments as removed
    for assignment in master_data['assignments']:
        if (assignment['family_id'] == family_id and 
            assignment['vehicle_no'] == vehicle_no and
            assignment['status'] == 'active'):
            assignment['status'] = 'removed'
    
    # Check if this was the last vehicle - if so, cancel the policy
    remaining_vehicles = [v for v in master_data['vehicles'] 
                         if v['family_id'] == family_id and v['status'] == 'active']
    
    if len(remaining_vehicles) == 0:
        # Cancel the policy
        policy = next(p for p in master_data['policies'] if p['family_id'] == family_id)
        policy['status'] = 'cancelled'
        
        # Mark family as cancelled
        family = next(f for f in master_data['families'] if f['family_id'] == family_id)
        family['status'] = 'cancelled'
        
        return {
            'change_type': 'policy_cancellation',
            'date': current_date.strftime('%Y-%m-%d'),
            'family_id': family_id,
            'vehicle_no': vehicle_no,
            'vehicle_type': vehicle_to_remove['vehicle_type'],
            'reason': 'all_vehicles_removed'
        }
    
    return {
        'change_type': 'vehicle_removal',
        'date': current_date.strftime('%Y-%m-%d'),
        'family_id': family_id,
        'vehicle_no': vehicle_no,
        'vehicle_type': vehicle_to_remove['vehicle_type'],
        'reason': random.choice(CONFIG['changes']['vehicle_changes']['removals']['reasons'])
    }


def _substitute_vehicle(master_data: Dict[str, List[Dict[str, Any]]], 
                       family_id: str, current_date: datetime) -> Dict[str, Any]:
    """Substitute a vehicle in a family."""
    # Get vehicles for this family
    family_vehicles = [v for v in master_data['vehicles'] 
                      if v['family_id'] == family_id and v['status'] == 'active']
    
    if not family_vehicles:
        return None
    
    # Select a vehicle to substitute
    old_vehicle = random.choice(family_vehicles)
    old_vehicle_no = old_vehicle['vehicle_no']
    
    # Check for coverage gap
    coverage_gap_prob = CONFIG['changes']['vehicle_changes']['substitutions']['coverage_gap_probability']
    has_coverage_gap = random.random() < coverage_gap_prob
    
    if has_coverage_gap:
        # Mark old vehicle as removed first
        old_vehicle['status'] = 'removed'
        
        # Mark assignments as removed
        for assignment in master_data['assignments']:
            if (assignment['family_id'] == family_id and 
                assignment['vehicle_no'] == old_vehicle_no and
                assignment['status'] == 'active'):
                assignment['status'] = 'removed'
        
        # Reset policy tenure if configured
        if CONFIG['changes']['vehicle_changes']['substitutions']['tenure_reset_on_gap']:
            policy = next(p for p in master_data['policies'] if p['family_id'] == family_id)
            policy['client_tenure_days'] = '0'
    
    # Generate new vehicle
    vehicle_types = CONFIG['vehicles']['types']
    vehicle_type = random.choice(vehicle_types)
    
    model_year = random.randint(2001, 2018)
    vin = f"VIN{family_id}{old_vehicle_no}{model_year}"
    
    new_vehicle = {
        'vehicle_no': old_vehicle_no,  # Keep same vehicle number
        'family_id': family_id,
        'vin': vin,
        'vehicle_type': vehicle_type,
        'model_year': str(model_year),
        'status': 'active'
    }
    
    # Replace the old vehicle
    old_vehicle.update(new_vehicle)
    
    # Update existing assignment (don't create a new one)
    primary_driver = next(d for d in master_data['drivers'] 
                         if d['family_id'] == family_id and d['driver_type'] == 'adult')
    
    # Find and update the existing assignment (or create new one if coverage gap)
    assignment_found = False
    for assignment in master_data['assignments']:
        if (assignment['family_id'] == family_id and 
            assignment['vehicle_no'] == old_vehicle_no and
            assignment['status'] == 'active'):
            assignment['status'] = 'removed'
            assignment_found = True
            break
    
    # If no active assignment found (due to coverage gap), create a new one
    if not assignment_found:
        new_assignment = {
            'assignment_id': f"ASS{family_id}{old_vehicle_no}",
            'family_id': family_id,
            'vehicle_no': old_vehicle_no,
            'driver_no': primary_driver['driver_no'],
            'assignment_type': 'primary',
            'exposure_factor': '1.0',
            'effective_date': current_date.strftime('%Y-%m-%d'),
            'status': 'active'
        }
        master_data['assignments'].append(new_assignment)
    
    return {
        'change_type': 'vehicle_substitution',
        'date': current_date.strftime('%Y-%m-%d'),
        'family_id': family_id,
        'vehicle_no': old_vehicle_no,
        'old_vehicle_type': old_vehicle['vehicle_type'],
        'new_vehicle_type': vehicle_type,
        'coverage_gap': has_coverage_gap,
        'tenure_reset': has_coverage_gap and CONFIG['changes']['vehicle_changes']['substitutions']['tenure_reset_on_gap'],
        'reason': random.choice(CONFIG['changes']['vehicle_changes']['substitutions']['reasons'])
    }


def _apply_family_evolution(master_data: Dict[str, List[Dict[str, Any]]], 
                           current_date: datetime) -> List[Dict[str, Any]]:
    """Apply family evolution changes for the current month."""
    changes = []
    
    # For now, skip family evolution since we're using single person families only
    # This would implement marriage, divorce, children turning 16, etc.
    
    return changes


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


def _save_master_data(master_data: Dict[str, List[Dict[str, Any]]]) -> None:
    """Save all master data files."""
    data_dir = Path(CONFIG['file_paths']['data']['policy_system'])
    
    # Save families
    families_file = data_dir / CONFIG['data_files']['master']['families']
    _save_csv_file(master_data['families'], families_file)
    
    # Save policies
    policies_file = data_dir / CONFIG['data_files']['master']['policies']
    _save_csv_file(master_data['policies'], policies_file)
    
    # Save vehicles
    vehicles_file = data_dir / CONFIG['data_files']['master']['vehicles']
    _save_csv_file(master_data['vehicles'], vehicles_file)
    
    # Save drivers
    drivers_file = data_dir / CONFIG['data_files']['master']['drivers']
    _save_csv_file(master_data['drivers'], drivers_file)
    
    # Save assignments
    assignments_file = data_dir / CONFIG['data_files']['master']['driver_vehicle_assignments']
    _save_csv_file(master_data['assignments'], assignments_file)


def _save_csv_file(data: List[Dict[str, Any]], filepath: Path) -> None:
    """Save data to CSV file."""
    if not data:
        return
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def _save_changes_log(changes: List[Dict[str, Any]]) -> None:
    """Save changes to the policy changes log."""
    filepath = Path(CONFIG['file_paths']['data']['policy_system']) / CONFIG['data_files']['master']['changes_log']
    
    if not changes:
        print(f"  No changes to save to {filepath}")
        return
    
    # Get all possible field names from all changes
    all_fieldnames = set()
    for change in changes:
        all_fieldnames.update(change.keys())
    
    # Convert to sorted list for consistent ordering
    fieldnames = sorted(list(all_fieldnames))
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Write each change, filling missing fields with empty strings
        for change in changes:
            row = {field: change.get(field, '') for field in fieldnames}
            writer.writerow(row)
    
    print(f"  Saved {len(changes)} changes to {filepath}")


def _apply_annual_premium_increases(master_data: Dict[str, List[Dict[str, Any]]], year: int) -> None:
    """Apply annual premium increases based on inflation rate."""
    if year <= 2018:  # Don't apply increases in the first year
        return
    
    inflation_rate = CONFIG['policies']['premium']['annual_inflation_rate']
    
    # Apply inflation to all active policies
    for policy in master_data['policies']:
        if policy['status'] == 'active':
            current_premium = float(policy['premium_paid'])
            new_premium = current_premium * (1 + inflation_rate)
            policy['premium_paid'] = round(new_premium, 2)
    
    print(f"    Applied {inflation_rate*100:.1f}% premium increase for {year}")


if __name__ == "__main__":
    simulate_monthly_changes()
