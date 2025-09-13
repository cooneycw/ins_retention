"""
Major Change Detection Module

Post-processes the inforce data to identify policies that have had "major changes"
including vehicle additions/substitutions, driver changes, or other significant modifications.
"""

import pandas as pd
import yaml
from pathlib import Path
from typing import Dict, List, Any, Set
from datetime import datetime


class MajorChangeDetector:
    """
    Detects major changes in policies by comparing monthly snapshots.
    
    Major changes include:
    - Vehicle additions/substitutions
    - Driver changes (excluding normal aging)
    - Policy cancellations/reinstatements
    """
    
    def __init__(self):
        """Initialize the major change detector."""
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        config_path = Path("config/policy_system_config.yaml")
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def detect_major_changes(self, inforce_df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect major changes for each policy-month combination.
        
        Args:
            inforce_df: DataFrame with inforce data
            
        Returns:
            DataFrame with major_change flag added
        """
        print("Detecting major changes in policies...")
        
        # Sort by policy, year, month for proper comparison
        inforce_df = inforce_df.sort_values(['policy', 'inforce_yy', 'inforce_mm'])
        
        # Initialize major_change flag
        inforce_df['major_change'] = False
        
        # Group by policy to detect changes
        policies = inforce_df['policy'].unique()
        total_policies = len(policies)
        
        for i, policy in enumerate(policies):
            if i % 100 == 0:
                print(f"  Processing policy {i+1}/{total_policies}: {policy}")
            
            policy_data = inforce_df[inforce_df['policy'] == policy].copy()
            
            # Detect changes for this policy
            self._detect_policy_changes(policy_data, inforce_df)
        
        print(f"Major change detection completed for {total_policies} policies")
        return inforce_df
    
    def _detect_policy_changes(self, policy_data: pd.DataFrame, full_df: pd.DataFrame) -> None:
        """
        Detect major changes for a single policy.
        
        Args:
            policy_data: Data for a single policy
            full_df: Full DataFrame to update
        """
        if len(policy_data) <= 1:
            return  # No changes possible with only one record
        
        # Sort by date
        policy_data = policy_data.sort_values(['inforce_yy', 'inforce_mm'])
        
        for i in range(1, len(policy_data)):
            current_record = policy_data.iloc[i]
            previous_record = policy_data.iloc[i-1]
            
            # Check for major changes
            has_major_change = False
            
            # 1. Vehicle changes
            if self._has_vehicle_change(previous_record, current_record):
                has_major_change = True
            
            # 2. Driver changes (excluding normal aging)
            if self._has_driver_change(previous_record, current_record):
                has_major_change = True
            
            # 3. Policy status changes
            if self._has_policy_status_change(previous_record, current_record):
                has_major_change = True
            
            # 4. New policy (first appearance)
            if i == 1:  # First comparison for this policy
                has_major_change = True
            
            # Update the flag in the full DataFrame
            if has_major_change:
                mask = (full_df['policy'] == current_record['policy']) & \
                       (full_df['inforce_yy'] == current_record['inforce_yy']) & \
                       (full_df['inforce_mm'] == current_record['inforce_mm'])
                full_df.loc[mask, 'major_change'] = True
    
    def _has_vehicle_change(self, prev_record: pd.Series, curr_record: pd.Series) -> bool:
        """Check if there's a vehicle change between records."""
        # Compare vehicle details
        vehicle_changed = (
            prev_record['vehicle_no'] != curr_record['vehicle_no'] or
            prev_record['vehicle_vin'] != curr_record['vehicle_vin'] or
            prev_record['vehicle_type'] != curr_record['vehicle_type'] or
            prev_record['model_year'] != curr_record['model_year']
        )
        
        return vehicle_changed
    
    def _has_driver_change(self, prev_record: pd.Series, curr_record: pd.Series) -> bool:
        """Check if there's a driver change (excluding normal aging)."""
        # Compare driver details
        driver_changed = (
            prev_record['driver_no'] != curr_record['driver_no'] or
            prev_record['driver_license_no'] != curr_record['driver_license_no'] or
            prev_record['driver_name'] != curr_record['driver_name'] or
            prev_record['driver_type'] != curr_record['driver_type']
        )
        
        # Check for significant age change (more than 1 year difference)
        age_diff = abs(curr_record['driver_age'] - prev_record['driver_age'])
        significant_age_change = age_diff > 1
        
        return driver_changed or significant_age_change
    
    def _has_policy_status_change(self, prev_record: pd.Series, curr_record: pd.Series) -> bool:
        """Check if there's a policy status change."""
        # This would require additional status tracking in the data
        # For now, we'll focus on vehicle and driver changes
        return False
    
    def generate_change_summary(self, inforce_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate a summary of major changes detected.
        
        Args:
            inforce_df: DataFrame with major_change flag
            
        Returns:
            Dictionary with change statistics
        """
        print("Generating major change summary...")
        
        # Overall statistics
        total_records = len(inforce_df)
        records_with_changes = inforce_df['major_change'].sum()
        change_percentage = (records_with_changes / total_records) * 100
        
        # Changes by year
        yearly_changes = inforce_df.groupby('inforce_yy')['major_change'].agg(['count', 'sum']).reset_index()
        yearly_changes['change_rate'] = (yearly_changes['sum'] / yearly_changes['count']) * 100
        
        # Changes by month
        monthly_changes = inforce_df.groupby(['inforce_yy', 'inforce_mm'])['major_change'].agg(['count', 'sum']).reset_index()
        monthly_changes['change_rate'] = (monthly_changes['sum'] / monthly_changes['count']) * 100
        
        # Policy-level statistics
        policy_changes = inforce_df.groupby('policy')['major_change'].agg(['count', 'sum']).reset_index()
        policies_with_changes = (policy_changes['sum'] > 0).sum()
        total_policies = len(policy_changes)
        
        summary = {
            'total_records': total_records,
            'records_with_changes': records_with_changes,
            'change_percentage': change_percentage,
            'total_policies': total_policies,
            'policies_with_changes': policies_with_changes,
            'policy_change_rate': (policies_with_changes / total_policies) * 100,
            'yearly_changes': yearly_changes.to_dict('records'),
            'monthly_changes': monthly_changes.to_dict('records')
        }
        
        # Print summary
        print(f"  Total records: {total_records:,}")
        print(f"  Records with major changes: {records_with_changes:,} ({change_percentage:.1f}%)")
        print(f"  Total policies: {total_policies:,}")
        print(f"  Policies with changes: {policies_with_changes:,} ({summary['policy_change_rate']:.1f}%)")
        
        return summary


def add_major_change_flags(inforce_file_path: str, output_file_path: str = None) -> None:
    """
    Add major change flags to the inforce data.
    
    Args:
        inforce_file_path: Path to the inforce CSV file
        output_file_path: Path for output file (defaults to overwriting input)
    """
    if output_file_path is None:
        output_file_path = inforce_file_path
    
    print("Adding major change flags to inforce data...")
    
    # Load the inforce data
    inforce_df = pd.read_csv(inforce_file_path)
    print(f"  Loaded {len(inforce_df):,} records")
    
    # Detect major changes
    detector = MajorChangeDetector()
    inforce_df_with_changes = detector.detect_major_changes(inforce_df)
    
    # Generate summary
    summary = detector.generate_change_summary(inforce_df_with_changes)
    
    # Save the updated data
    inforce_df_with_changes.to_csv(output_file_path, index=False)
    print(f"  Saved updated inforce data to {output_file_path}")
    
    return summary


if __name__ == "__main__":
    # Test the major change detection
    inforce_path = "data/inforce/inforce_monthly_view.csv"
    summary = add_major_change_flags(inforce_path)
