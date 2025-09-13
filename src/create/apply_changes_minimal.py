"""
Minimal Apply Changes Module

A simplified version that works with the new family-centric data structure.
This is a temporary solution to generate the inforce view for testing.
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
    """Simulate monthly changes over the full period (minimal version)."""
    print("Simulating monthly changes over 90 months...")
    print(f"Date range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
    
    # Load initial master data
    print("  - Loading initial master data...")
    master_data = _load_master_data()
    
    # For now, just create a minimal changes log
    changes = []
    
    # Create some basic changes for demonstration
    current_date = START_DATE
    month_count = 0
    
    while current_date <= END_DATE:
        month_count += 1
        year = current_date.year
        month = current_date.month
        
        print(f"  - Processing month {month_count}: {year}-{month:02d}")
        
        # Add some basic changes (minimal for now)
        if month_count % 12 == 0:  # Every year
            changes.append({
                "change_type": "policy_renewal",
                "date": current_date.strftime("%Y-%m-%d"),
                "description": "Annual renewal cycle"
            })
        
        # Move to next month
        if month == 12:
            current_date = current_date.replace(year=year + 1, month=1)
        else:
            current_date = current_date.replace(month=month + 1)
    
    # Save changes log
    print("  - Saving changes log...")
    _save_changes_log(changes)
    
    # Save updated master data (unchanged for now)
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
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = changes[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(changes)
    
    print(f"  Saved {len(changes)} changes to {filepath}")


if __name__ == "__main__":
    simulate_monthly_changes()
