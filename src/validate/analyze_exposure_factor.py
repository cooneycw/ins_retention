#!/usr/bin/env python3
"""
Analyze Policy 00000008 at 2023-04 to investigate an incorrect exposure factor,
specifically checking the driver details and the logged change event.
"""
import pandas as pd
import sys
from pathlib import Path
import glob

def find_latest_inforce_file():
    """Finds the most recent part-*.csv file."""
    search_path = "data/inforce/inforce_monthly_view_sorted/part-*.csv"
    files = glob.glob(search_path)
    if not files:
        raise FileNotFoundError(f"No inforce file found at {search_path}")
    latest_file = max(files, key=lambda f: Path(f).stat().st_mtime)
    print(f"Using inforce file: {Path(latest_file).name}\n")
    return latest_file

def analyze_policy_change():
    """Analyzes a specific policy change for incorrect exposure factors."""
    POLICY_ID = '00000008'
    YEAR = 2023
    MONTH = 4
    
    print(f"=== ANALYZING POLICY {POLICY_ID} AT {YEAR}-{MONTH:02d} FOR EXPOSURE FACTOR ERROR ===\n")
    
    inforce_file = find_latest_inforce_file()
    
    df = pd.read_csv(inforce_file, dtype={'policy': str})
    
    # Dynamically find Family ID from policies_master.csv
    try:
        policies_df = pd.read_csv('data/policy_system/policies_master.csv', dtype={'policy_no': str, 'family_id': str})
        policy_info = policies_df[policies_df['policy_no'] == POLICY_ID]
        if not policy_info.empty:
            FAMILY_ID = policy_info.iloc[0]['family_id']
            print(f"Found Family ID for Policy {POLICY_ID}: {FAMILY_ID}\n")
        else:
            print(f"❌ Could not find Policy {POLICY_ID} in policies_master.csv")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Error reading policies_master.csv: {e}")
        sys.exit(1)

    policy_data = df[df['policy'] == POLICY_ID].copy()
    
    if policy_data.empty:
        print(f"❌ No data found for policy {POLICY_ID}")
        return

    # Get the specific months around the event
    focus_months_data = policy_data[
        ((policy_data['inforce_yy'] == YEAR) & (policy_data['inforce_mm'].isin([MONTH - 1, MONTH])))
    ].copy()
    
    if focus_months_data.empty:
        print(f"❌ No data found for policy {POLICY_ID} in the specified period.")
        return
        
    # --- Print Monthly Snapshots with full driver details ---
    for m in [MONTH - 1, MONTH]:
        month_data = focus_months_data[focus_months_data['inforce_mm'] == m]
        if month_data.empty:
            print(f"\n--- Snapshot for {YEAR}-{m:02d}: No data ---")
            continue
            
        print(f"\n--- Snapshot for {YEAR}-{m:02d} ---")
        
        drivers = month_data[['driver_no', 'driver_name', 'driver_age', 'driver_type']].drop_duplicates()
        print("Drivers on Policy:")
        for _, driver in drivers.iterrows():
            print(f"  - D{driver['driver_no']}: {driver['driver_name']} (Age {driver['driver_age']}, Type: {driver['driver_type']})")

        print("\nAssignments:")
        for _, assignment in month_data.iterrows():
            exposure = assignment['exposure_factor']
            assignment_type = assignment['assignment_type']
            highlight = ""
            if (assignment_type == 'primary' and exposure != 1.0 and len(drivers) == 1) or \
               (assignment_type == 'primary' and exposure == 1.0 and len(drivers) > 1):
                highlight = " <--- ⚠️ INCORRECT"

            print(f"  - V{assignment['vehicle_no']} -> D{assignment['driver_no']} ({assignment_type}), Exposure: {exposure}{highlight}")

    # --- Cross-reference with Log File ---
    print("\n--- LOG FILE VERIFICATION ---")
    try:
        changes_df = pd.read_csv('data/policy_system/policy_changes_log.csv', dtype={'family_id': str})
        
        log_entry = changes_df[
            (changes_df['family_id'] == FAMILY_ID) &
            (changes_df['date'].str.startswith(f"{YEAR}-{MONTH:02d}"))
        ]
        
        if not log_entry.empty:
            entry = log_entry.iloc[0]
            print(f"✅ Log entry found for {YEAR}-{MONTH:02d}:")
            print(f"  - Type: {entry['change_type']}")
            print(f"  - Reason: {entry.get('reason', 'N/A')}")
            if 'driver_name' in entry:
                print(f"  - Driver Name: {entry['driver_name']}")
        else:
            print("❌ No corresponding entry found in the policy changes log for this month.")
            
    except Exception as e:
        print(f"⚠️  Could not load or process policy_changes_log.csv: {e}")

if __name__ == "__main__":
    analyze_policy_change()
