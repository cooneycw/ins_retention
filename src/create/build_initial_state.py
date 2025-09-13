"""
Build Initial State Module

Creates the initial 1200 policies with vehicles and drivers.
This is Step 1 of the insurance policy system generation.
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
SPOUSE_DRIVER_AGE_RANGE = tuple(CONFIG['drivers']['age_ranges']['spouse'])


def create_initial_policies() -> None:
    """
    Create initial 1200 policies with associated vehicles and drivers.
    
    This function generates:
    - policies_master.csv: Policy master data
    - vehicles_master.csv: Vehicle master data
    - drivers_master.csv: Driver master data
    """
    print("Creating initial 1200 policies...")
    print(f"Date range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
    
    # Generate policy data
    print("  - Generating policy master data...")
    policies = _generate_policy_data()
    _save_csv_file(policies, CONFIG['data_files']['master']['policies'])
    
    # Generate vehicle data
    print("  - Generating vehicle master data...")
    vehicles = _generate_vehicle_data(policies)
    _save_csv_file(vehicles, CONFIG['data_files']['master']['vehicles'])
    
    # Generate driver data
    print("  - Generating driver master data...")
    drivers = _generate_driver_data(policies)
    _save_csv_file(drivers, CONFIG['data_files']['master']['drivers'])
    
    # Calculate premiums based on vehicles and drivers
    print("  - Calculating policy premiums...")
    policies_with_premiums = _calculate_policy_premiums(policies, vehicles, drivers)
    _save_csv_file(policies_with_premiums, CONFIG['data_files']['master']['policies'])
    
    print("Initial state creation completed successfully!")


def _generate_policy_data() -> List[Dict[str, Any]]:
    """Generate initial policy master data."""
    policies = []
    
    for i in range(TOTAL_POLICIES):
        policy_no = f"{i+1:08d}"  # 8-digit policy number starting from 00000001
        
        # Sequential start date for validation - Policy 00000001 starts Jan 1, 2018
        start_date = START_DATE + timedelta(days=i)
        
        # Initial expiry date (1 year later)
        expiry_date = start_date + timedelta(days=365)
        
        # Premium will be calculated after vehicles and drivers are generated
        # Placeholder for now - will be updated in _calculate_policy_premium
        premium = 0.0
        
        # Client tenure starts at 0 (new policy)
        client_tenure_days = 0
        
        # Determine family type
        family_type = _select_family_type()
        
        policy = {
            "policy_no": policy_no,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "current_expiry_date": expiry_date.strftime("%Y-%m-%d"),
            "premium_paid": premium,
            "client_tenure_days": client_tenure_days,
            "family_type": family_type,
            "status": "active"
        }
        
        policies.append(policy)
    
    return policies


def _generate_vehicle_data(policies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Generate initial vehicle master data with per-policy indexing."""
    vehicles = []
    
    for policy in policies:
        family_type = policy["family_type"]
        policy_no = policy["policy_no"]
        
        # Determine number of vehicles based on family type
        if family_type == "single_person_single_vehicle":
            num_vehicles = 1
        elif family_type == "single_driver_multi_vehicle":
            num_vehicles = random.randint(2, 3)
        elif family_type == "family_multi_vehicle":
            # Weighted distribution from config
            vehicle_weights = CONFIG['family_types']['family_multi_vehicle']['vehicle_weights']
            num_vehicles = random.choices([2, 3, 4], weights=vehicle_weights)[0]
        else:  # family_single_vehicle
            num_vehicles = 1
        
        # Generate vehicles for this policy with per-policy indexing (01, 02, 03...)
        for v in range(num_vehicles):
            vehicle_no = f"{v + 1:02d}"  # Per-policy indexing: 01, 02, 03...
            
            # Generate VIN (17 characters)
            vin = _generate_vin()
            
            # Vehicle type
            vehicle_type = random.choice(VEHICLE_TYPES)
            
            # Model year from config
            earliest_year = CONFIG['vehicles']['model_year_range']['earliest']
            latest_year = CONFIG['vehicles']['model_year_range']['latest']
            model_year = random.randint(earliest_year, latest_year)
            
            # Effective date (same as policy start)
            effective_date = policy["start_date"]
            
            vehicle = {
                "vehicle_no": vehicle_no,
                "policy_no": policy_no,
                "vin": vin,
                "vehicle_type": vehicle_type,
                "model_year": model_year,
                "effective_date": effective_date,
                "status": "active"
            }
            
            vehicles.append(vehicle)
    
    return vehicles


def _generate_driver_data(policies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Generate initial driver master data with per-policy indexing and proper spouse logic."""
    drivers = []
    
    for policy in policies:
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
        
        # Generate drivers for this policy with per-policy indexing (01, 02, 03...)
        for d in range(num_drivers):
            driver_no = f"{d + 1:02d}"  # Per-policy indexing: 01, 02, 03...
            
            # Generate Ontario-style license number
            license_no = _generate_ontario_license()
            
            # Determine driver type and age
            if d == 0:  # Primary driver
                driver_type = "primary"
                age = random.randint(*PRIMARY_DRIVER_AGE_RANGE)
            elif family_type in ["family_multi_vehicle", "family_single_vehicle"] and d == 1:
                # Spouse - only if they are primary driver on a vehicle
                # For now, assume spouse is primary driver on at least one vehicle
                driver_type = "spouse"
                age = random.randint(*SPOUSE_DRIVER_AGE_RANGE)
            else:
                # Secondary drivers (kids)
                driver_type = "secondary"
                age = random.randint(*SECONDARY_DRIVER_AGE_RANGE)
            
            # Calculate birthday (for aging calculations)
            current_year = 2018
            birth_year = current_year - age
            birthday = datetime(birth_year, random.randint(1, 12), random.randint(1, 28))
            
            # Effective date (same as policy start)
            effective_date = policy["start_date"]
            
            driver = {
                "driver_no": driver_no,
                "policy_no": policy_no,
                "license_no": license_no,
                "driver_type": driver_type,
                "age": age,
                "birthday": birthday.strftime("%Y-%m-%d"),
                "effective_date": effective_date,
                "status": "active"
            }
            
            drivers.append(driver)
    
    return drivers


def _select_family_type() -> str:
    """Select family type based on distribution."""
    rand = random.random()
    cumulative = 0
    
    for family_type, probability in FAMILY_TYPES.items():
        cumulative += probability
        if rand <= cumulative:
            return family_type
    
    return "family_single_vehicle"  # fallback


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
                              vehicles: List[Dict[str, Any]], 
                              drivers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Calculate premiums for all policies based on vehicles and drivers."""
    policies_with_premiums = []
    
    for policy in policies:
        policy_no = policy["policy_no"]
        
        # Get vehicles and drivers for this policy
        policy_vehicles = [v for v in vehicles if v["policy_no"] == policy_no]
        policy_drivers = [d for d in drivers if d["policy_no"] == policy_no]
        
        # Calculate premium
        calculated_premium = _calculate_single_policy_premium(policy_vehicles, policy_drivers)
        
        # Update policy with calculated premium
        policy["premium_paid"] = calculated_premium
        policies_with_premiums.append(policy)
    
    return policies_with_premiums


def _calculate_single_policy_premium(vehicles: List[Dict[str, Any]], 
                                   drivers: List[Dict[str, Any]]) -> float:
    """Calculate premium for a single policy based on vehicles and drivers."""
    base_premium = CONFIG['policies']['premium']['base_amount']
    calc_config = CONFIG['policies']['premium']['calculation']
    
    # Start with base premium
    total_premium = base_premium
    
    # Calculate vehicle premium factors
    vehicle_premium = 0.0
    for vehicle in vehicles:
        vehicle_type = vehicle["vehicle_type"]
        vehicle_factor = calc_config['vehicle_type_factors'].get(vehicle_type, 1.0)
        vehicle_premium += base_premium * vehicle_factor
    
    # Calculate driver premium factors
    driver_premium = 0.0
    for driver in drivers:
        driver_age = driver["age"]
        driver_type = driver["driver_type"]
        
        # Get age factor
        age_factor = _get_age_factor(driver_age, calc_config['driver_age_factors'])
        
        # Get driver type factor
        type_factor = calc_config['driver_type_factors'].get(driver_type, 1.0)
        
        # Calculate driver contribution
        driver_contribution = base_premium * age_factor * type_factor
        driver_premium += driver_contribution
    
    # Average the vehicle and driver premiums
    if vehicles and drivers:
        avg_vehicle_premium = vehicle_premium / len(vehicles)
        avg_driver_premium = driver_premium / len(drivers)
        total_premium = (avg_vehicle_premium + avg_driver_premium) / 2
    
    # Apply multi-vehicle discount
    num_vehicles = len(vehicles)
    discount_rate = _get_multi_vehicle_discount(num_vehicles, calc_config['multi_vehicle_discounts'])
    total_premium *= (1 - discount_rate)
    
    # Add some random variation
    std_dev = CONFIG['policies']['premium']['standard_deviation']
    variation = random.normalvariate(0, std_dev * 0.1)  # 10% of std dev for variation
    total_premium += variation
    
    return round(max(total_premium, 100.0), 2)  # Minimum premium of $100


def _get_age_factor(age: int, age_factors: Dict[int, float]) -> float:
    """Get age factor for premium calculation."""
    # Find the closest age in the factors
    if age in age_factors:
        return age_factors[age]
    
    # Interpolate between closest ages
    sorted_ages = sorted(age_factors.keys())
    
    if age < min(sorted_ages):
        return age_factors[min(sorted_ages)]
    elif age > max(sorted_ages):
        return age_factors[max(sorted_ages)]
    
    # Find surrounding ages
    for i in range(len(sorted_ages) - 1):
        if sorted_ages[i] <= age <= sorted_ages[i + 1]:
            lower_age, upper_age = sorted_ages[i], sorted_ages[i + 1]
            lower_factor, upper_factor = age_factors[lower_age], age_factors[upper_age]
            
            # Linear interpolation
            ratio = (age - lower_age) / (upper_age - lower_age)
            interpolated_factor = lower_factor + ratio * (upper_factor - lower_factor)
            return interpolated_factor
    
    return 1.0  # fallback


def _get_multi_vehicle_discount(num_vehicles: int, discounts: Dict[int, float]) -> float:
    """Get multi-vehicle discount rate."""
    if num_vehicles in discounts:
        return discounts[num_vehicles]
    
    # For 4+ vehicles, use the 4-vehicle discount
    if num_vehicles >= 4:
        return discounts.get(4, 0.0)
    
    return 0.0


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
