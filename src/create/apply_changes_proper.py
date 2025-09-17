"""
Proper Apply Changes Module

Implements the actual vehicle changes and family evolution according to YAML configuration.
This replaces the minimal version with real change simulation.
"""

import csv
import random
import string
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
        
        # Check for policy renewals (when current date matches expiry date)
        renewal_changes = _process_policy_renewals(master_data, current_date)
        changes.extend(renewal_changes)
        
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
        'status': 'active',
        'effective_date': current_date.strftime('%Y-%m-%d')
    }
    
    master_data['vehicles'].append(new_vehicle)
    
    # Apply corrected driver assignment logic
    _reassign_drivers_to_vehicles(master_data, family_id, current_date)
    
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
    vehicle_to_remove['removal_date'] = current_date.strftime('%Y-%m-%d')
    
    # Mark assignments as removed
    for assignment in master_data['assignments']:
        if (assignment['family_id'] == family_id and 
            assignment['vehicle_no'] == vehicle_no and
            assignment['status'] == 'active'):
            assignment['status'] = 'removed'
            assignment['end_date'] = current_date.strftime('%Y-%m-%d')
    
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
    
    # Reassign drivers to remaining vehicles
    _reassign_drivers_to_vehicles(master_data, family_id, current_date)
    
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
    
    # Store original vehicle data before modification
    original_vehicle_type = old_vehicle['vehicle_type']
    original_model_year = old_vehicle['model_year']
    
    # Check for coverage gap
    coverage_gap_prob = CONFIG['changes']['vehicle_changes']['substitutions']['coverage_gap_probability']
    has_coverage_gap = random.random() < coverage_gap_prob
    
    if has_coverage_gap:
        # Mark old vehicle as removed first
        old_vehicle['status'] = 'removed'
        old_vehicle['removal_date'] = current_date.strftime('%Y-%m-%d')
        
        # Mark assignments as removed with end_date
        for assignment in master_data['assignments']:
            if (assignment['family_id'] == family_id and 
                assignment['vehicle_no'] == old_vehicle_no and
                assignment['status'] == 'active'):
                assignment['status'] = 'removed'
                assignment['end_date'] = current_date.strftime('%Y-%m-%d')
        
        # Reset policy tenure if configured
        if CONFIG['changes']['vehicle_changes']['substitutions']['tenure_reset_on_gap']:
            policy = next(p for p in master_data['policies'] if p['family_id'] == family_id)
            policy['client_tenure_days'] = '0'
    
    # Generate new vehicle data - ensure it's actually different
    vehicle_types = CONFIG['vehicles']['types']
    available_types = [vt for vt in vehicle_types if vt != original_vehicle_type]
    
    # If all types are the same (shouldn't happen with 10 types), pick any type
    if not available_types:
        available_types = vehicle_types
    
    vehicle_type = random.choice(available_types)
    model_year = random.randint(2001, 2018)
    
    # Ensure model year is different for meaningful substitution
    max_attempts = 10
    attempts = 0
    while model_year == int(original_model_year) and attempts < max_attempts:
        model_year = random.randint(2001, 2018)
        attempts += 1
    
    vin = f"VIN{family_id}{old_vehicle_no}{model_year}"
    
    # Update the vehicle in place (preserving the reference)
    old_vehicle['vin'] = vin
    old_vehicle['vehicle_type'] = vehicle_type
    old_vehicle['model_year'] = str(model_year)
    old_vehicle['status'] = 'active'
    
    # Only reassign drivers if there was a coverage gap (assignments were removed)
    # For simple substitutions, keep existing driver assignments
    if has_coverage_gap:
        _reassign_drivers_to_vehicles(master_data, family_id, current_date, 'vehicle_substitution')
    else:
        # If no coverage gap, the vehicle is updated but assignments should remain
        # We need to make sure the vehicle_no still points to the same drivers
        # Since we're updating the vehicle in-place, the assignments should still be valid
        # and do not need to be recreated or changed.
        pass
    
    # Recalculate premiums after vehicle substitution (new vehicle type/year affects rates)
    _recalculate_policy_premiums(master_data, family_id)
    
    return {
        'change_type': 'vehicle_substitution',
        'date': current_date.strftime('%Y-%m-%d'),
        'family_id': family_id,
        'vehicle_no': old_vehicle_no,
        'old_vehicle_type': original_vehicle_type,
        'new_vehicle_type': vehicle_type,
        'coverage_gap': has_coverage_gap,
        'tenure_reset': has_coverage_gap and CONFIG['changes']['vehicle_changes']['substitutions']['tenure_reset_on_gap'],
        'reason': random.choice(CONFIG['changes']['vehicle_changes']['substitutions']['reasons'])
    }


def _reassign_drivers_to_vehicles(master_data: Dict[str, List[Dict[str, Any]]], 
                                 family_id: str, current_date: datetime, 
                                 assignment_type: str = 'driver_reassignment') -> None:
    """Reassign drivers to vehicles using the corrected assignment logic."""
    # Get active vehicles and drivers for this family
    active_vehicles = [v for v in master_data['vehicles'] 
                      if v['family_id'] == family_id and v['status'] == 'active']
    active_drivers = [d for d in master_data['drivers'] 
                     if d['family_id'] == family_id and d['status'] == 'active']
    
    if not active_vehicles or not active_drivers:
        return
    
    # Remove all existing active assignments for this family
    for assignment in master_data['assignments']:
        if (assignment['family_id'] == family_id and 
            assignment['status'] == 'active'):
            assignment['status'] = 'removed'
            assignment['end_date'] = current_date.strftime('%Y-%m-%d')
    
    # Apply corrected assignment logic
    num_vehicles = len(active_vehicles)
    num_drivers = len(active_drivers)
    
    # Sort drivers by age (youngest first for teen detection)
    active_drivers.sort(key=lambda x: int(x['age']))
    
    # Check if there are any teen drivers (age <= 24)
    has_teens = any(int(d['age']) <= 24 for d in active_drivers)
    
    # Assignment logic based on corrected rules
    if num_vehicles == num_drivers:
        # One-to-one assignments
        for i in range(num_vehicles):
            driver = active_drivers[i]
            vehicle = active_vehicles[i]
            new_assignment = {
                'assignment_id': f"ASS{family_id}{vehicle['vehicle_no']}{driver['driver_no']}",
                'family_id': family_id,
                'vehicle_no': vehicle['vehicle_no'],
                'driver_no': driver['driver_no'],
                'assignment_type': 'primary',
                'exposure_factor': '1.0',
                'effective_date': current_date.strftime('%Y-%m-%d'),
                'end_date': '',
                'status': 'active'
            }
            # Calculate and store the annual premium for this assignment
            annual_premium_paid = _calculate_assignment_premium_for_storage(new_assignment, vehicle, driver, current_date)
            new_assignment['annual_premium_rate'] = round(annual_premium_paid / float(new_assignment['exposure_factor']), 2)
            new_assignment['annual_premium_paid'] = annual_premium_paid
            master_data['assignments'].append(new_assignment)
    elif num_vehicles > num_drivers:
        # More vehicles than drivers → some drivers cover multiple vehicles (no secondary needed)
        for i, vehicle in enumerate(active_vehicles):
            primary_driver = active_drivers[i % num_drivers]
            new_assignment = {
                'assignment_id': f"ASS{family_id}{vehicle['vehicle_no']}{primary_driver['driver_no']}",
                'family_id': family_id,
                'vehicle_no': vehicle['vehicle_no'],
                'driver_no': primary_driver['driver_no'],
                'assignment_type': 'primary',
                'exposure_factor': '1.0',
                'effective_date': current_date.strftime('%Y-%m-%d'),
                'end_date': '',
                'status': 'active'
            }
            # Calculate and store the annual premium for this assignment
            annual_premium_paid = _calculate_assignment_premium_for_storage(new_assignment, vehicle, primary_driver, current_date, assignment_type)
            new_assignment['annual_premium_rate'] = round(annual_premium_paid / float(new_assignment['exposure_factor']), 2)
            new_assignment['annual_premium_paid'] = annual_premium_paid
            master_data['assignments'].append(new_assignment)
    else:  # num_drivers > num_vehicles
        # More drivers than vehicles → primary per vehicle, optional secondary (per rules)
        for i, vehicle in enumerate(active_vehicles):
            primary_driver = active_drivers[i] if i < num_drivers else active_drivers[i % num_drivers]
            # Primary
            new_assignment = {
                'assignment_id': f"ASS{family_id}{vehicle['vehicle_no']}{primary_driver['driver_no']}",
                'family_id': family_id,
                'vehicle_no': vehicle['vehicle_no'],
                'driver_no': primary_driver['driver_no'],
                'assignment_type': 'primary',
                'exposure_factor': '1.0',
                'effective_date': current_date.strftime('%Y-%m-%d'),
                'end_date': '',
                'status': 'active'
            }
            # Calculate and store the annual premium for this assignment
            annual_premium_paid = _calculate_assignment_premium_for_storage(new_assignment, vehicle, primary_driver, current_date, assignment_type)
            new_assignment['annual_premium_rate'] = round(annual_premium_paid / float(new_assignment['exposure_factor']), 2)
            new_assignment['annual_premium_paid'] = annual_premium_paid
            master_data['assignments'].append(new_assignment)
            # Secondary only if teens present OR more drivers than vehicles
            if has_teens or num_drivers > num_vehicles:
                # pick a different driver (first one not equal to primary)
                secondary_driver = next((d for d in active_drivers if d['driver_no'] != primary_driver['driver_no']), None)
                if secondary_driver:
                    assignment_type = 'secondary_teen' if int(secondary_driver['age']) <= 24 else 'secondary_adult'
                    exposure_factor = CONFIG['driver_assignment']['exposure_allocations']['teen_secondary'] if assignment_type == 'secondary_teen' else CONFIG['driver_assignment']['exposure_allocations']['secondary_adult']
                    
                    new_secondary_assignment = {
                        'assignment_id': f"ASS{family_id}{vehicle['vehicle_no']}{secondary_driver['driver_no']}",
                        'family_id': family_id,
                        'vehicle_no': vehicle['vehicle_no'],
                        'driver_no': secondary_driver['driver_no'],
                        'assignment_type': assignment_type,
                        'exposure_factor': exposure_factor,
                        'effective_date': current_date.strftime('%Y-%m-%d'),
                        'end_date': '',
                        'status': 'active'
                    }
                    # Calculate and store the annual premium for this assignment
                    annual_premium_paid = _calculate_assignment_premium_for_storage(new_secondary_assignment, vehicle, secondary_driver, current_date, assignment_type)
                    new_secondary_assignment['annual_premium_rate'] = round(annual_premium_paid / float(new_secondary_assignment['exposure_factor']), 2)
                    new_secondary_assignment['annual_premium_paid'] = annual_premium_paid
                    master_data['assignments'].append(new_secondary_assignment)
                    
                    # CRITICAL FIX: Only reduce primary adult's exposure if secondary is also an adult
                    # If secondary is a teen, primary adult keeps 1.0 exposure
                    if assignment_type == 'secondary_adult':
                        new_assignment['exposure_factor'] = CONFIG['driver_assignment']['exposure_allocations']['primary_shared']
                        # Recalculate premium for primary assignment with updated exposure factor
                        annual_premium_paid = _calculate_assignment_premium_for_storage(new_assignment, vehicle, primary_driver, current_date, assignment_type)
                        new_assignment['annual_premium_rate'] = round(annual_premium_paid / float(new_assignment['exposure_factor']), 2)
                        new_assignment['annual_premium_paid'] = annual_premium_paid
    
    # Recalculate premiums after driver reassignments
    _recalculate_policy_premiums(master_data, family_id)


def _apply_family_evolution(master_data: Dict[str, List[Dict[str, Any]]], 
                           current_date: datetime) -> List[Dict[str, Any]]:
    """Apply family evolution changes for the current month."""
    changes = []
    
    # Get family evolution probabilities
    family_evolution = CONFIG['changes']['family_evolution']
    
    # Process each family
    for family in master_data['families']:
        if family['status'] != 'active':
            continue
        
        family_id = family['family_id']
        family_type = family['family_type']
        family_config = CONFIG['family_types'][family_type]
        
        # Adult additions (marriage, partnership)
        if (family_config['can_add_adults'] and
            random.random() < family_evolution['adult_additions']['probability']):
            
            change = _add_adult(master_data, family_id, current_date)
            if change:
                changes.append(change)
        
        # Adult removals (divorce, separation, death)
        if (family_config['can_remove_adults'] and
            random.random() < family_evolution['adult_removals']['probability']):
            
            change = _remove_adult(master_data, family_id, current_date)
            if change:
                changes.append(change)
        
        # Teen additions (children turning 16)
        if (family_config['can_add_teens'] and
            random.random() < family_evolution['teen_additions']['probability']):
            
            change = _add_teen(master_data, family_id, current_date)
            if change:
                changes.append(change)
        
        # Teen removals (aging out, moving out)
        if (family_config['can_remove_teens'] and
            random.random() < family_evolution['teen_removals']['probability']):
            
            change = _remove_teen(master_data, family_id, current_date)
            if change:
                changes.append(change)
    
    return changes


def _add_adult(master_data: Dict[str, List[Dict[str, Any]]], 
               family_id: str, current_date: datetime) -> Dict[str, Any]:
    """Add an adult driver to a family."""
    # Get next driver number for this family
    family_drivers = [d for d in master_data['drivers'] if d['family_id'] == family_id]
    next_driver_no = max([int(d['driver_no']) for d in family_drivers]) + 1
    
    # Generate driver name
    first_names = CONFIG['drivers']['name_generation']['first_names']
    last_names = CONFIG['drivers']['name_generation']['last_names']
    driver_name = f"{random.choice(first_names)} {random.choice(last_names)}"
    
    # Generate adult age (25-65)
    driver_age = random.randint(25, 65)
    
    # Generate driver license
    license_no = f"{''.join(random.choices(string.ascii_uppercase, k=4))}{''.join(random.choices(string.digits, k=3))}{''.join(random.choices(string.ascii_uppercase, k=2))}"
    
    new_driver = {
        'driver_no': str(next_driver_no),
        'family_id': family_id,
        'driver_license_no': license_no,
        'driver_name': driver_name,
        'driver_type': 'adult',
        'age': str(driver_age),
        'age_at_issuance': str(driver_age),  # For new drivers, age_at_issuance = current age
        'birthday': f"{current_date.year - driver_age}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        'effective_date': current_date.strftime('%Y-%m-%d'),
        'status': 'active'
    }
    
    master_data['drivers'].append(new_driver)
    
    # Reassign drivers to vehicles with new driver (use 'new_driver' assignment type)
    _reassign_drivers_to_vehicles(master_data, family_id, current_date, 'new_driver')
    
    return {
        'change_type': 'adult_addition',
        'date': current_date.strftime('%Y-%m-%d'),
        'family_id': family_id,
        'driver_no': str(next_driver_no),
        'driver_name': driver_name,
        'reason': random.choice(CONFIG['changes']['family_evolution']['adult_additions']['reasons'])
    }


def _remove_adult(master_data: Dict[str, List[Dict[str, Any]]], 
                  family_id: str, current_date: datetime) -> Dict[str, Any]:
    """Remove an adult driver from a family."""
    # Get active adult drivers for this family
    family_adults = [d for d in master_data['drivers'] 
                    if d['family_id'] == family_id and d['status'] == 'active' and d['driver_type'] == 'adult']
    
    if len(family_adults) <= 1:
        return None  # Don't remove the last adult driver
    
    # Select an adult to remove
    adult_to_remove = random.choice(family_adults)
    adult_driver_no = adult_to_remove['driver_no']
    
    # Mark driver as removed
    adult_to_remove['status'] = 'removed'
    
    # Mark all assignments for this driver as removed
    for assignment in master_data['assignments']:
        if (assignment['family_id'] == family_id and 
            assignment['driver_no'] == adult_driver_no and
            assignment['status'] == 'active'):
            assignment['status'] = 'removed'
            assignment['end_date'] = current_date.strftime('%Y-%m-%d')
    
    # Reassign remaining drivers to vehicles
    _reassign_drivers_to_vehicles(master_data, family_id, current_date)
    
    return {
        'change_type': 'adult_removal',
        'date': current_date.strftime('%Y-%m-%d'),
        'family_id': family_id,
        'driver_no': adult_driver_no,
        'driver_name': adult_to_remove['driver_name'],
        'reason': random.choice(CONFIG['changes']['family_evolution']['adult_removals']['reasons'])
    }


def _add_teen(master_data: Dict[str, List[Dict[str, Any]]], 
              family_id: str, current_date: datetime) -> Dict[str, Any]:
    """Add a teen driver to a family."""
    # Get next driver number for this family
    family_drivers = [d for d in master_data['drivers'] if d['family_id'] == family_id]
    next_driver_no = max([int(d['driver_no']) for d in family_drivers]) + 1
    
    # Generate driver name
    first_names = CONFIG['drivers']['name_generation']['first_names']
    last_names = CONFIG['drivers']['name_generation']['last_names']
    driver_name = f"{random.choice(first_names)} {random.choice(last_names)}"
    
    # Generate teen age (16-24)
    driver_age = random.randint(16, 24)
    
    # Generate driver license
    license_no = f"{''.join(random.choices(string.ascii_uppercase, k=4))}{''.join(random.choices(string.digits, k=3))}{''.join(random.choices(string.ascii_uppercase, k=2))}"
    
    new_driver = {
        'driver_no': str(next_driver_no),
        'family_id': family_id,
        'driver_license_no': license_no,
        'driver_name': driver_name,
        'driver_type': 'teen',
        'age': str(driver_age),
        'age_at_issuance': str(driver_age),  # For new drivers, age_at_issuance = current age
        'birthday': f"{current_date.year - driver_age}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        'effective_date': current_date.strftime('%Y-%m-%d'),
        'status': 'active'
    }
    
    master_data['drivers'].append(new_driver)
    
    # Reassign drivers to vehicles with new teen driver
    _reassign_drivers_to_vehicles(master_data, family_id, current_date, 'new_driver')
    
    return {
        'change_type': 'teen_addition',
        'date': current_date.strftime('%Y-%m-%d'),
        'family_id': family_id,
        'driver_no': str(next_driver_no),
        'driver_name': driver_name,
        'reason': random.choice(CONFIG['changes']['family_evolution']['teen_additions']['reasons'])
    }


def _remove_teen(master_data: Dict[str, List[Dict[str, Any]]], 
                 family_id: str, current_date: datetime) -> Dict[str, Any]:
    """Remove a teen driver from a family."""
    # Get active teen drivers for this family
    family_teens = [d for d in master_data['drivers'] 
                   if d['family_id'] == family_id and d['status'] == 'active' and d['driver_type'] == 'teen']
    
    if len(family_teens) == 0:
        return None  # No teens to remove
    
    # Select a teen to remove
    teen_to_remove = random.choice(family_teens)
    teen_driver_no = teen_to_remove['driver_no']
    
    # Mark driver as removed
    teen_to_remove['status'] = 'removed'
    
    # Mark all assignments for this driver as removed
    for assignment in master_data['assignments']:
        if (assignment['family_id'] == family_id and 
            assignment['driver_no'] == teen_driver_no and
            assignment['status'] == 'active'):
            assignment['status'] = 'removed'
            assignment['end_date'] = current_date.strftime('%Y-%m-%d')
    
    # Reassign remaining drivers to vehicles
    _reassign_drivers_to_vehicles(master_data, family_id, current_date)
    
    return {
        'change_type': 'teen_removal',
        'date': current_date.strftime('%Y-%m-%d'),
        'family_id': family_id,
        'driver_no': teen_driver_no,
        'driver_name': teen_to_remove['driver_name'],
        'reason': random.choice(CONFIG['changes']['family_evolution']['teen_removals']['reasons'])
    }


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
    
    # Collect all possible fieldnames from all records
    all_fields = set()
    for record in data:
        all_fields.update(record.keys())
    
    # Sort fieldnames for consistent output
    fieldnames = sorted(all_fields)
    
    # Ensure all records have all fields (fill missing fields with empty string)
    normalized_data = []
    for record in data:
        normalized_record = {field: record.get(field, '') for field in fieldnames}
        normalized_data.append(normalized_record)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(normalized_data)


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


def _recalculate_policy_premiums(master_data: Dict[str, List[Dict[str, Any]]], family_id: str) -> None:
    """Recalculate premiums for a specific family after assignment changes."""
    # Get the policy for this family
    policy = next((p for p in master_data['policies'] if p['family_id'] == family_id), None)
    if not policy or policy['status'] != 'active':
        return
    
    # Get assignments for this family
    family_assignments = [a for a in master_data['assignments'] 
                         if a['family_id'] == family_id and a['status'] == 'active']
    
    # Calculate new premium based on current assignments
    new_premium = _calculate_single_policy_premium(family_assignments, master_data['vehicles'], master_data['drivers'])
    
    # Update the policy premium
    policy['premium_paid'] = new_premium


def _calculate_single_policy_premium(assignments: List[Dict[str, Any]], 
                                   vehicles: List[Dict[str, Any]], 
                                   drivers: List[Dict[str, Any]]) -> float:
    """Calculate premium for a single policy based on driver-vehicle assignments."""
    base_premium = CONFIG['policies']['premium']['base_amount']
    calc_config = CONFIG['policies']['premium']['calculation']
    
    total_premium = 0.0
    
    # Calculate premium for each assignment
    for assignment in assignments:
        if float(assignment["exposure_factor"]) > 0:  # Only count assigned drivers
            # Get vehicle and driver details for this assignment
            vehicle = next((v for v in vehicles 
                          if v["family_id"] == assignment["family_id"] and 
                          v["vehicle_no"] == assignment["vehicle_no"]), None)
            driver = next((d for d in drivers 
                         if d["family_id"] == assignment["family_id"] and 
                         d["driver_no"] == assignment["driver_no"]), None)
            
            if not vehicle or not driver:
                continue
            
            # Base premium for this assignment
            assignment_premium = base_premium * float(assignment["exposure_factor"])
            
            # Apply vehicle type factor
            vehicle_type = vehicle["vehicle_type"]
            vehicle_factor = calc_config["vehicle_type_factors"].get(vehicle_type, 1.0)
            assignment_premium *= vehicle_factor
            
            # Apply driver age factor
            driver_age = int(driver["age"])
            age_factor = _get_age_factor(driver_age, calc_config["driver_age_factors"])
            assignment_premium *= age_factor
            
            # Apply driver type factor
            driver_type = driver["driver_type"]
            driver_type_factor = calc_config["driver_type_factors"].get(driver_type, 1.0)
            assignment_premium *= driver_type_factor
            
            # No random variation - deterministic premium calculation
            
            total_premium += assignment_premium
    
    return round(max(total_premium, 100.0), 2)  # Minimum premium of $100


def _get_age_factor(age: int, age_factors: Dict) -> float:
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


def _process_policy_renewals(master_data: Dict[str, List[Dict[str, Any]]], current_date: datetime) -> List[Dict[str, Any]]:
    """Process policy renewals when current date matches expiry date."""
    renewal_changes = []
    
    for policy in master_data['policies']:
        if policy['status'] != 'active':
            continue
            
        expiry_date = datetime.strptime(policy['current_expiry_date'], '%Y-%m-%d')
        
        # Check if policy is due for renewal
        if current_date.date() == expiry_date.date():
            family_id = policy['family_id']
            
            # Update driver ages_at_issuance for renewal
            _update_driver_ages_at_renewal(master_data, family_id, current_date)
            
            # Extend policy expiry date by 1 year
            new_expiry_date = expiry_date + timedelta(days=365)
            policy['current_expiry_date'] = new_expiry_date.strftime('%Y-%m-%d')
            
            # Recalculate premiums with new ages_at_issuance
            _recalculate_policy_premiums(master_data, family_id)
            
            renewal_changes.append({
                'change_type': 'policy_renewal',
                'date': current_date.strftime('%Y-%m-%d'),
                'family_id': family_id,
                'policy_no': policy['policy_no'],
                'old_expiry_date': expiry_date.strftime('%Y-%m-%d'),
                'new_expiry_date': new_expiry_date.strftime('%Y-%m-%d'),
                'reason': 'annual_renewal'
            })
    
    return renewal_changes


def _update_driver_ages_at_renewal(master_data: Dict[str, List[Dict[str, Any]]], 
                                  family_id: str, renewal_date: datetime) -> None:
    """Update driver ages_at_issuance at policy renewal."""
    family_drivers = [d for d in master_data['drivers'] if d['family_id'] == family_id and d['status'] == 'active']
    
    for driver in family_drivers:
        # Calculate current age at renewal
        birthday = datetime.strptime(driver['birthday'], '%Y-%m-%d')
        current_age = renewal_date.year - birthday.year
        if (renewal_date.month, renewal_date.day) < (birthday.month, birthday.day):
            current_age -= 1
        
        # Update age_at_issuance to current age (renewal resets age constancy)
        driver['age_at_issuance'] = str(current_age)
        driver['age'] = str(current_age)  # Also update current age


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


def _get_driver_age_for_premium_calculation(driver: Dict[str, Any], 
                                          assignment_type: str, 
                                          current_date: datetime) -> int:
    """
    Get the appropriate driver age for premium calculation based on age constancy rules.
    
    Rules:
    1. Vehicle substitution: Use age_at_issuance
    2. Driver deletion/reassignment: Use age_at_issuance  
    3. New driver addition: Use current age
    4. New vehicle addition: Use current age
    5. Existing assignment: Use age_at_issuance
    """
    if assignment_type in ['vehicle_substitution', 'driver_reassignment', 'existing_assignment']:
        return int(driver.get('age_at_issuance', driver['age']))
    else:  # new_driver, new_vehicle
        return int(driver['age'])


def _calculate_historical_age_at_issuance(driver: Dict[str, Any], 
                                        policy_start_date: datetime) -> int:
    """Calculate what the driver's age was at policy issuance."""
    birthday = datetime.strptime(driver['birthday'], '%Y-%m-%d')
    issuance_age = policy_start_date.year - birthday.year
    if (policy_start_date.month, policy_start_date.day) < (birthday.month, birthday.day):
        issuance_age -= 1
    return issuance_age


def _calculate_assignment_premium_for_storage(assignment: Dict[str, Any], 
                                            vehicle: Dict[str, Any], 
                                            driver: Dict[str, Any], 
                                            current_date: datetime,
                                            assignment_type: str = 'existing_assignment') -> float:
    """Calculate the annual premium for an assignment to store in assignments data."""
    base_premium = CONFIG['policies']['premium']['base_amount']
    calc_config = CONFIG['policies']['premium']['calculation']
    
    # Calculate the base premium rate for this assignment (before exposure factor)
    assignment_premium_rate = base_premium
    
    # Apply vehicle type factor
    vehicle_type = vehicle["vehicle_type"]
    vehicle_factor = calc_config["vehicle_type_factors"].get(vehicle_type, 1.0)
    assignment_premium_rate *= vehicle_factor
    
    # Apply driver age factor using age constancy rules
    driver_age = _get_driver_age_for_premium_calculation(driver, assignment_type, current_date)
    age_factor = _get_age_factor(driver_age, calc_config["driver_age_factors"])
    assignment_premium_rate *= age_factor
    
    # Apply driver type factor (use assignment_type, not driver_type)
    assignment_type_key = assignment["assignment_type"]
    # Map secondary_adult to secondary for config lookup
    config_key = assignment_type_key if assignment_type_key != 'secondary_adult' else 'secondary'
    driver_type_factor = calc_config["driver_type_factors"].get(config_key, 1.0)
    assignment_premium_rate *= driver_type_factor
    
    # Apply historical inflation adjustments
    inflation_rate = CONFIG['policies']['premium']['annual_inflation_rate']
    years_from_2018 = current_date.year - 2018
    assignment_premium_rate *= ((1 + inflation_rate) ** years_from_2018)
    
    # Calculate annual premium paid (rate * exposure factor)
    annual_premium_paid = assignment_premium_rate * float(assignment["exposure_factor"])
    
    return round(annual_premium_paid, 2)


if __name__ == "__main__":
    simulate_monthly_changes()
