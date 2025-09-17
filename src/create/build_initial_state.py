"""
Build Initial State Module

Creates the initial families, policies, vehicles, drivers, and driver-vehicle assignments.
This is Step 1 of the insurance policy system generation with new family-centric approach.
"""

from __future__ import annotations

import csv
import random
import yaml
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Tuple

# Load configuration
CONFIG_PATH = Path("config/policy_system_config.yaml")
with open(CONFIG_PATH, 'r') as f:
    CONFIG = yaml.safe_load(f)

# Import helper function from apply_changes_proper
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))
from apply_changes_proper import _calculate_assignment_premium_for_storage

# Extract constants from config
START_DATE = datetime.strptime(CONFIG['date_range']['start_date'], '%Y-%m-%d')
END_DATE = datetime.strptime(CONFIG['date_range']['end_date'], '%Y-%m-%d')
TOTAL_POLICIES = CONFIG['policies']['initial_count']

# Family composition distribution
FAMILY_TYPES = {
    family_type: details['percentage'] 
    for family_type, details in CONFIG['family_types'].items()
}

# Vehicle types
VEHICLE_TYPES = CONFIG['vehicles']['types']

# Driver age ranges
PRIMARY_DRIVER_AGE_RANGE = tuple(CONFIG['drivers']['age_ranges']['primary'])
SECONDARY_DRIVER_AGE_RANGE = tuple(CONFIG['drivers']['age_ranges']['secondary'])

# Driver name generation
FIRST_NAMES = CONFIG['drivers']['name_generation']['first_names']
LAST_NAMES = CONFIG['drivers']['name_generation']['last_names']

# Driver assignment rules
EXPOSURE_ALLOCATIONS = CONFIG['driver_assignment']['exposure_allocations']


def create_initial_state() -> None:
    """
    Create initial families, policies, vehicles, drivers, and driver-vehicle assignments.
    
    This function generates:
    - families_master.csv: Family master data
    - policies_master.csv: Policy master data
    - vehicles_master.csv: Vehicle master data
    - drivers_master.csv: Driver master data
    - driver_vehicle_assignments.csv: Driver-vehicle assignment data
    """
    print("Creating initial state with family-centric approach...")
    print(f"Date range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
    
    # Calculate required initial families to achieve target inforce
    target_inforce_2018 = 1200
    required_initial_families = 1103  # Fine-tuned value from previous analysis
    
    print(f"  Creating {required_initial_families} initial families to achieve ~{target_inforce_2018} inforce by end of 2018")
    
    # Generate family data
    print("  - Generating family master data...")
    families = _generate_family_data(required_initial_families)
    _save_csv_file(families, CONFIG['data_files']['master']['families'])
    
    # Generate policy data (sort families by start date first for sequential policy numbering)
    print("  - Generating policy master data...")
    families_sorted = sorted(families, key=lambda f: f['start_date'])
    policies = _generate_policy_data(families_sorted)
    _save_csv_file(policies, CONFIG['data_files']['master']['policies'])
    
    # Generate vehicle data
    print("  - Generating vehicle master data...")
    vehicles = _generate_vehicle_data(families_sorted)
    _save_csv_file(vehicles, CONFIG['data_files']['master']['vehicles'])
    
    # Generate driver data
    print("  - Generating driver master data...")
    drivers = _generate_driver_data(families_sorted)
    _save_csv_file(drivers, CONFIG['data_files']['master']['drivers'])
    
    # Generate driver-vehicle assignments
    print("  - Generating driver-vehicle assignments...")
    assignments = _generate_driver_vehicle_assignments(families_sorted, vehicles, drivers)
    _save_csv_file(assignments, CONFIG['data_files']['master']['driver_vehicle_assignments'])
    
    # Calculate premiums based on assignments
    print("  - Calculating policy premiums...")
    policies_with_premiums = _calculate_policy_premiums(policies, assignments, vehicles, drivers)
    _save_csv_file(policies_with_premiums, CONFIG['data_files']['master']['policies'])
    
    print("Initial state creation completed successfully!")


def _generate_family_data(num_families: int) -> List[Dict[str, Any]]:
    """Generate initial family master data."""
    families = []
    
    for i in range(num_families):
        family_id = f"FAM{i+1:06d}"  # 6-digit family ID starting from FAM000001
        
        # Distribute start dates across 2018 (Jan 1 to Dec 31) instead of sequential
        days_in_2018 = 365
        start_date = START_DATE + timedelta(days=random.randint(0, days_in_2018-1))
        
        # Determine family type
        family_type = _select_family_type()
        
        # Generate family composition based on type
        family_composition = _generate_family_composition(family_type)
        
        family = {
            "family_id": family_id,
            "family_type": family_type,
            "adults": family_composition['adults'],
            "teens": family_composition['teens'],
            "vehicles": family_composition['vehicles'],
            "max_vehicles": family_composition['max_vehicles'],
            "start_date": start_date.strftime("%Y-%m-%d"),
            "status": "active"
        }
        
        families.append(family)
    
    return families


def _generate_policy_data(families: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Generate initial policy master data."""
    policies = []
    
    for i, family in enumerate(families):
        policy_no = f"{i+1:08d}"  # 8-digit policy number starting from 00000001
        
        # Initial expiry date (1 year later)
        start_date = datetime.strptime(family["start_date"], "%Y-%m-%d")
        expiry_date = start_date + timedelta(days=365)
        
        # Premium will be calculated after assignments are generated
        premium = 0.0
        
        # Client tenure starts at 0 (new policy)
        client_tenure_days = 0
        
        policy = {
            "policy_no": policy_no,
            "family_id": family["family_id"],
            "start_date": family["start_date"],
            "current_expiry_date": expiry_date.strftime("%Y-%m-%d"),
            "premium_paid": premium,
            "client_tenure_days": client_tenure_days,
            "family_type": family["family_type"],
            "status": "active"
        }
        
        policies.append(policy)
    
    return policies


def _generate_vehicle_data(families: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Generate initial vehicle master data."""
    vehicles = []
    
    for family in families:
        family_id = family["family_id"]
        num_vehicles = family["vehicles"]
        
        # Generate vehicles for this family with per-family indexing (01, 02, 03...)
        for v in range(num_vehicles):
            vehicle_no = f"{v + 1:02d}"  # Per-family indexing: 01, 02, 03...
            
            # Generate VIN (17 characters)
            vin = _generate_vin()
            
            # Vehicle type
            vehicle_type = random.choice(VEHICLE_TYPES)
            
            # Model year from config
            earliest_year = CONFIG['vehicles']['model_year_range']['earliest']
            latest_year = CONFIG['vehicles']['model_year_range']['latest']
            model_year = random.randint(earliest_year, latest_year)
            
            # Effective date (same as family start)
            effective_date = family["start_date"]
            
            vehicle = {
                "vehicle_no": vehicle_no,
                "family_id": family_id,
                "vin": vin,
                "vehicle_type": vehicle_type,
                "model_year": model_year,
                "effective_date": effective_date,
                "status": "active"
            }
            
            vehicles.append(vehicle)
    
    return vehicles


def _generate_driver_data(families: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Generate initial driver master data."""
    drivers = []
    
    for family in families:
        family_id = family["family_id"]
        num_adults = family["adults"]
        num_teens = family["teens"]
        
        # Generate adult drivers
        for a in range(num_adults):
            driver_no = f"{a + 1:02d}"  # Per-family indexing: 01, 02, 03...
            
            # Generate Ontario-style license number
            license_no = _generate_ontario_license()
            
            # Generate unique driver name
            driver_name = _generate_driver_name()
            
            # Adult drivers are primary age
            age = random.randint(*PRIMARY_DRIVER_AGE_RANGE)
            
            # Calculate birthday (for aging calculations)
            current_year = 2018
            birth_year = current_year - age
            birthday = datetime(birth_year, random.randint(1, 12), random.randint(1, 28))
            
            # Effective date (same as family start)
            effective_date = family["start_date"]
            
            driver = {
                "driver_no": driver_no,
                "family_id": family_id,
                "license_no": license_no,
                "driver_name": driver_name,
                "driver_type": "adult",
                "age": age,
                "age_at_issuance": age,  # For initial drivers, age_at_issuance = current age
                "birthday": birthday.strftime("%Y-%m-%d"),
                "effective_date": effective_date,
                "status": "active"
            }
            
            drivers.append(driver)
        
        # Generate teen drivers
        for t in range(num_teens):
            driver_no = f"{num_adults + t + 1:02d}"  # Continue numbering after adults
            
            # Generate Ontario-style license number
            license_no = _generate_ontario_license()
            
            # Generate unique driver name
            driver_name = _generate_driver_name()
            
            # Teen drivers are secondary age
            age = random.randint(*SECONDARY_DRIVER_AGE_RANGE)
            
            # Calculate birthday (for aging calculations)
            current_year = 2018
            birth_year = current_year - age
            birthday = datetime(birth_year, random.randint(1, 12), random.randint(1, 28))
            
            # Effective date (same as family start)
            effective_date = family["start_date"]
            
            driver = {
                "driver_no": driver_no,
                "family_id": family_id,
                "license_no": license_no,
                "driver_name": driver_name,
                "driver_type": "teen",
                "age": age,
                "age_at_issuance": age,  # For initial drivers, age_at_issuance = current age
                "birthday": birthday.strftime("%Y-%m-%d"),
                "effective_date": effective_date,
                "status": "active"
            }
            
            drivers.append(driver)
    
    return drivers


def _generate_driver_vehicle_assignments(families: List[Dict[str, Any]], 
                                       vehicles: List[Dict[str, Any]], 
                                       drivers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Generate driver-vehicle assignments with proper exposure calculations."""
    assignments = []
    
    for family in families:
        family_id = family["family_id"]
        
        # Get vehicles and drivers for this family
        family_vehicles = [v for v in vehicles if v["family_id"] == family_id]
        family_drivers = [d for d in drivers if d["family_id"] == family_id]
        
        # Separate adults and teens
        adult_drivers = [d for d in family_drivers if d["driver_type"] == "adult"]
        teen_drivers = [d for d in family_drivers if d["driver_type"] == "teen"]
        
        # Create assignments using the new logic
        family_assignments = _assign_drivers_to_vehicles(family_vehicles, adult_drivers, teen_drivers)
        
        # Add assignment records
        for assignment in family_assignments:
            assignment_record = {
                "assignment_id": assignment["assignment_id"],
                "family_id": family_id,
                "vehicle_no": assignment["vehicle_no"],
                "driver_no": assignment["driver_no"],
                "assignment_type": assignment["assignment_type"],
                "exposure_factor": assignment["exposure_factor"],
                "effective_date": family["start_date"],
                "status": "active"
            }
            # Calculate and store the annual premium for this assignment
            from datetime import datetime
            effective_date = datetime.strptime(family["start_date"], '%Y-%m-%d')
            annual_premium_paid = _calculate_assignment_premium_for_storage(assignment_record,
                                                                         next(v for v in vehicles if v["family_id"] == family_id and v["vehicle_no"] == assignment["vehicle_no"]),
                                                                         next(d for d in drivers if d["family_id"] == family_id and d["driver_no"] == assignment["driver_no"]),
                                                                         effective_date,
                                                                         'existing_assignment')
            assignment_record['annual_premium_rate'] = round(annual_premium_paid / float(assignment_record['exposure_factor']), 2)
            assignment_record['annual_premium_paid'] = annual_premium_paid
            assignments.append(assignment_record)
    
    return assignments


def _assign_drivers_to_vehicles(vehicles: List[Dict[str, Any]], 
                               adult_drivers: List[Dict[str, Any]], 
                               teen_drivers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Assign drivers to vehicles following the CORRECTED assignment rules.
    
    CORRECTED RULES:
    1. Each vehicle gets exactly one driver (default - no secondary assignments)
    2. Secondary assignments only when: teen driver present (≤24) OR more drivers than vehicles
    3. Multiple vehicle assignments only when: more vehicles than drivers
    4. Every driver must be assigned to at least 1 vehicle
    """
    assignments = []
    assignment_counter = 1
    
    # Combine all drivers and identify teens (age ≤24)
    all_drivers = adult_drivers + teen_drivers
    teen_drivers_actual = [d for d in all_drivers if d.get('age', 0) <= 24]
    adult_drivers_actual = [d for d in all_drivers if d.get('age', 0) > 24]
    
    num_drivers = len(all_drivers)
    num_vehicles = len(vehicles)
    has_teens = len(teen_drivers_actual) > 0
    
    print(f"    Assignment Logic: {num_drivers} drivers, {num_vehicles} vehicles, {len(teen_drivers_actual)} teens")
    
    # SCENARIO 1: More vehicles than drivers (sum(vehicles) > sum(drivers))
    if num_vehicles > num_drivers:
        print(f"    Scenario: More vehicles than drivers - assigning drivers to multiple vehicles")
        
        # Assign each driver to their own vehicle first
        for i, driver in enumerate(all_drivers):
            if i < num_vehicles:
                vehicle_no = vehicles[i]["vehicle_no"]
                assignments.append({
                    "assignment_id": f"ASS{assignment_counter:08d}",
                    "vehicle_no": vehicle_no,
                    "driver_no": driver["driver_no"],
                    "assignment_type": "primary",
                    "exposure_factor": EXPOSURE_ALLOCATIONS["primary_solo"]
                })
                assignment_counter += 1
        
        # Assign remaining vehicles to drivers (one driver can be primary on multiple vehicles)
        for i in range(num_drivers, num_vehicles):
            vehicle_no = vehicles[i]["vehicle_no"]
            # Assign to first driver (they become primary on multiple vehicles)
            driver = all_drivers[0]
            assignments.append({
                "assignment_id": f"ASS{assignment_counter:08d}",
                "vehicle_no": vehicle_no,
                "driver_no": driver["driver_no"],
                "assignment_type": "primary",
                "exposure_factor": EXPOSURE_ALLOCATIONS["primary_solo"]
            })
            assignment_counter += 1
    
    # SCENARIO 2: More drivers than vehicles (sum(drivers) > sum(vehicles))
    elif num_drivers > num_vehicles:
        print(f"    Scenario: More drivers than vehicles - creating secondary assignments")
        
        # Assign primary drivers to vehicles
        for i, vehicle in enumerate(vehicles):
            vehicle_no = vehicle["vehicle_no"]
            primary_driver = all_drivers[i]
            
            assignments.append({
                "assignment_id": f"ASS{assignment_counter:08d}",
                "vehicle_no": vehicle_no,
                "driver_no": primary_driver["driver_no"],
                "assignment_type": "primary",
                "exposure_factor": EXPOSURE_ALLOCATIONS["primary_solo"]
            })
            assignment_counter += 1
        
        # Assign remaining drivers as secondary (prioritize teens for secondary)
        remaining_drivers = all_drivers[num_vehicles:]
        
        for i, driver in enumerate(remaining_drivers):
            # Assign to first vehicle (or distribute if multiple)
            vehicle_no = vehicles[i % num_vehicles]["vehicle_no"]
            
            # Determine assignment type based on age
            if driver.get('age', 0) <= 24:
                assignment_type = "secondary_teen"
                exposure_factor = EXPOSURE_ALLOCATIONS["teen_secondary"]
            else:
                assignment_type = "secondary_adult"
                exposure_factor = EXPOSURE_ALLOCATIONS["secondary_adult"]
            
            assignments.append({
                "assignment_id": f"ASS{assignment_counter:08d}",
                "vehicle_no": vehicle_no,
                "driver_no": driver["driver_no"],
                "assignment_type": assignment_type,
                "exposure_factor": exposure_factor
            })
            assignment_counter += 1
            
            # Adjust primary driver exposure to shared ONLY if secondary is an adult
            # If secondary is a teen, primary adult keeps 1.0 exposure
            if assignment_type == "secondary_adult":
                for assignment in assignments:
                    if (assignment["vehicle_no"] == vehicle_no and 
                        assignment["assignment_type"] == "primary"):
                        assignment["exposure_factor"] = EXPOSURE_ALLOCATIONS["primary_shared"]
                        break
    
    # SCENARIO 3: Equal drivers and vehicles (sum(drivers) = sum(vehicles))
    else:
        print(f"    Scenario: Equal drivers and vehicles - one driver per vehicle")
        
        # Check if we have teens - if so, create secondary assignments
        if has_teens:
            print(f"    Has teens - creating secondary teen assignments")
            
            # Assign each driver to their own vehicle as primary
            for i, driver in enumerate(all_drivers):
                vehicle_no = vehicles[i]["vehicle_no"]
                assignments.append({
                    "assignment_id": f"ASS{assignment_counter:08d}",
                    "vehicle_no": vehicle_no,
                    "driver_no": driver["driver_no"],
                    "assignment_type": "primary",
                    "exposure_factor": EXPOSURE_ALLOCATIONS["primary_solo"]
                })
                assignment_counter += 1
            
            # Assign teens as secondary to first vehicle
            for teen in teen_drivers_actual:
                vehicle_no = vehicles[0]["vehicle_no"]
                assignments.append({
                    "assignment_id": f"ASS{assignment_counter:08d}",
                    "vehicle_no": vehicle_no,
                    "driver_no": teen["driver_no"],
                    "assignment_type": "secondary_teen",
                    "exposure_factor": EXPOSURE_ALLOCATIONS["teen_secondary"]
                })
                assignment_counter += 1
                
                # Adjust primary driver exposure to shared
                for assignment in assignments:
                    if (assignment["vehicle_no"] == vehicle_no and 
                        assignment["assignment_type"] == "primary"):
                        assignment["exposure_factor"] = EXPOSURE_ALLOCATIONS["primary_shared"]
                        break
        
        else:
            print(f"    No teens - simple one-to-one assignment")
            
            # Simple one-to-one assignment: each driver gets their own vehicle
            for i, driver in enumerate(all_drivers):
                vehicle_no = vehicles[i]["vehicle_no"]
                assignments.append({
                    "assignment_id": f"ASS{assignment_counter:08d}",
                    "vehicle_no": vehicle_no,
                    "driver_no": driver["driver_no"],
                    "assignment_type": "primary",
                    "exposure_factor": EXPOSURE_ALLOCATIONS["primary_solo"]
                })
                assignment_counter += 1
    
    # Validate that all drivers are assigned
    assigned_drivers = set()
    for assignment in assignments:
        assigned_drivers.add(assignment["driver_no"])
    
    # Check for any unassigned drivers (should not happen with corrected logic)
    for driver in all_drivers:
        if driver["driver_no"] not in assigned_drivers:
            print(f"    WARNING: Unassigned driver {driver['driver_no']} - this should not happen!")
            assignments.append({
                "assignment_id": f"ASS{assignment_counter:08d}",
                "vehicle_no": "UNASSIGNED",
                "driver_no": driver["driver_no"],
                "assignment_type": "unassigned",
                "exposure_factor": 0.0
            })
            assignment_counter += 1
    
    print(f"    Created {len(assignments)} assignments")
    return assignments


def _generate_family_composition(family_type: str) -> Dict[str, Any]:
    """Generate family composition based on family type."""
    family_config = CONFIG['family_types'][family_type]
    
    adults = family_config['adults']
    teens = family_config['teens']
    
    # Determine number of vehicles
    if isinstance(family_config['vehicles'], list):
        vehicle_weights = family_config.get('vehicle_weights', None)
        vehicles = random.choices(family_config['vehicles'], weights=vehicle_weights)[0]
    else:
        vehicles = family_config['vehicles']
    
    # Determine number of teens
    if isinstance(teens, list):
        teen_weights = family_config.get('teen_weights', None)
        teens = random.choices(teens, weights=teen_weights)[0]
    
    return {
        'adults': adults,
        'teens': teens,
        'vehicles': vehicles,
        'max_vehicles': family_config.get('max_vehicles', vehicles)
    }


def _select_family_type() -> str:
    """Select family type based on distribution."""
    rand = random.random()
    cumulative = 0
    
    for family_type, probability in FAMILY_TYPES.items():
        cumulative += probability
        if rand <= cumulative:
            return family_type
    
    return "single_person"  # fallback


def _generate_driver_name() -> str:
    """Generate a unique driver name."""
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    return f"{first_name} {last_name}"


def _generate_vin() -> str:
    """Generate a realistic 17-character VIN."""
    # Simplified VIN generation (not following exact VIN standards)
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
    # Ontario format: 4 letters, 3 numbers, 2 letters
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    numbers = "0123456789"
    
    license_no = ""
    license_no += "".join(random.choices(letters, k=4))
    license_no += "".join(random.choices(numbers, k=3))
    license_no += "".join(random.choices(letters, k=2))
    
    return license_no


def _calculate_policy_premiums(policies: List[Dict[str, Any]], 
                              assignments: List[Dict[str, Any]],
                              vehicles: List[Dict[str, Any]],
                              drivers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Calculate premiums for all policies based on driver-vehicle assignments."""
    policies_with_premiums = []
    
    for policy in policies:
        family_id = policy["family_id"]
        
        # Get assignments for this family
        family_assignments = [a for a in assignments if a["family_id"] == family_id]
        
        # Calculate premium based on assignments
        calculated_premium = _calculate_single_policy_premium(family_assignments, vehicles, drivers)
        
        # Update policy with calculated premium
        policy["premium_paid"] = calculated_premium
        policies_with_premiums.append(policy)
    
    return policies_with_premiums


def _calculate_single_policy_premium(assignments: List[Dict[str, Any]], 
                                   vehicles: List[Dict[str, Any]], 
                                   drivers: List[Dict[str, Any]]) -> float:
    """Calculate premium for a single policy based on driver-vehicle assignments."""
    base_premium = CONFIG['policies']['premium']['base_amount']
    calc_config = CONFIG['policies']['premium']['calculation']
    
    total_premium = 0.0
    
    # Calculate premium for each assignment
    for assignment in assignments:
        if assignment["exposure_factor"] > 0:  # Only count assigned drivers
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
            assignment_premium = base_premium * assignment["exposure_factor"]
            
            # Apply vehicle type factor
            vehicle_type = vehicle["vehicle_type"]
            vehicle_factor = calc_config["vehicle_type_factors"].get(vehicle_type, 1.0)
            assignment_premium *= vehicle_factor
            
            # Apply driver age factor
            driver_age = driver["age"]
            age_factor = _get_age_factor(driver_age, calc_config["driver_age_factors"])
            assignment_premium *= age_factor
            
            # Apply driver type factor
            driver_type = driver["driver_type"]
            driver_type_factor = calc_config["driver_type_factors"].get(driver_type, 1.0)
            assignment_premium *= driver_type_factor
            
            # No random variation - deterministic premium calculation
            
            total_premium += assignment_premium
    
    return round(max(total_premium, 100.0), 2)  # Minimum premium of $100


def _get_age_factor(age: int, age_factors: Dict[int, float]) -> float:
    """Get the age factor for premium calculation."""
    # Find the closest age factor
    if age in age_factors:
        return age_factors[age]
    
    # Find the closest lower age factor
    applicable_ages = [a for a in age_factors.keys() if a <= age]
    if applicable_ages:
        closest_age = max(applicable_ages)
        return age_factors[closest_age]
    
    # Default to 1.0 if no factor found
    return 1.0


def _save_csv_file(data: List[Dict[str, Any]], filename: str) -> None:
    """Save data to CSV file."""
    filepath = Path(CONFIG['file_paths']['data']['policy_system']) / filename
    if not data:
        print(f"  Warning: No data to save to {filename}")
        return
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"  Saved {len(data)} records to {filepath}")


if __name__ == "__main__":
    create_initial_state()