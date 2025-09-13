"""
Family Type Configuration Manager

Manages family type configurations and provides utilities for switching between
different family composition presets.
"""

import yaml
from pathlib import Path
from typing import Dict, Any, List


class FamilyTypeManager:
    """Manages family type configurations and presets."""
    
    def __init__(self, config_path: str = "config/policy_system_config.yaml"):
        """Initialize the family type manager."""
        self.config_path = Path(config_path)
        self.presets_path = Path("config/family_type_presets.yaml")
        self._load_config()
        self._load_presets()
    
    def _load_config(self) -> None:
        """Load the main configuration file."""
        with open(self.config_path, 'r') as f:
            self.config = yaml.safe_load(f)
    
    def _load_presets(self) -> None:
        """Load the family type presets."""
        with open(self.presets_path, 'r') as f:
            self.presets = yaml.safe_load(f)
    
    def get_available_presets(self) -> List[str]:
        """Get list of available family type presets."""
        return list(self.presets.keys())
    
    def get_current_configuration(self) -> Dict[str, float]:
        """Get current family type configuration."""
        current = {}
        for family_type, details in self.config['family_types'].items():
            current[family_type] = details['percentage']
        return current
    
    def switch_to_preset(self, preset_name: str) -> bool:
        """Switch to a specific family type preset."""
        if preset_name not in self.presets:
            print(f"Error: Preset '{preset_name}' not found.")
            print(f"Available presets: {self.get_available_presets()}")
            return False
        
        print(f"Switching to '{preset_name}' configuration:")
        
        # Update family type percentages
        preset_config = self.presets[preset_name]
        for family_type, percentage in preset_config.items():
            if family_type in self.config['family_types']:
                self.config['family_types'][family_type]['percentage'] = percentage
                print(f"  {family_type}: {percentage}")
        
        # Save updated config
        self._save_config()
        
        print(f"\n✅ Successfully switched to '{preset_name}' configuration!")
        print("Run the system to see the new family composition.")
        return True
    
    def _save_config(self) -> None:
        """Save the updated configuration."""
        with open(self.config_path, 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)
    
    def validate_configuration(self) -> bool:
        """Validate that family type percentages sum to 1.0."""
        total = sum(details['percentage'] for details in self.config['family_types'].values())
        if abs(total - 1.0) > 0.001:  # Allow for small floating point errors
            print(f"Warning: Family type percentages sum to {total:.3f}, not 1.0")
            return False
        return True
    
    def get_family_type_rules(self, family_type: str) -> Dict[str, Any]:
        """Get the rules and constraints for a specific family type."""
        if family_type not in self.config['family_types']:
            return {}
        
        family_config = self.config['family_types'][family_type]
        return {
            'can_add_adults': family_config.get('can_add_adults', False),
            'can_add_teens': family_config.get('can_add_teens', False),
            'can_remove_adults': family_config.get('can_remove_adults', False),
            'can_remove_teens': family_config.get('can_remove_teens', False),
            'can_add_vehicles': family_config.get('can_add_vehicles', True),
            'can_remove_vehicles': family_config.get('can_remove_vehicles', True),
            'can_substitute_vehicles': family_config.get('can_substitute_vehicles', True),
            'max_vehicles': family_config.get('max_vehicles', 1)
        }
    
    def print_current_status(self) -> None:
        """Print current family type configuration status."""
        print("Current Family Type Configuration:")
        print("=" * 40)
        
        current = self.get_current_configuration()
        for family_type, percentage in current.items():
            print(f"{family_type}: {percentage:.1%}")
        
        print(f"\nTotal: {sum(current.values()):.1%}")
        
        if self.validate_configuration():
            print("✅ Configuration is valid")
        else:
            print("❌ Configuration has issues")
        
        print(f"\nAvailable presets: {', '.join(self.get_available_presets())}")


def main():
    """Command line interface for family type management."""
    import sys
    
    manager = FamilyTypeManager()
    
    if len(sys.argv) == 1:
        # No arguments - show current status
        manager.print_current_status()
    elif len(sys.argv) == 2:
        # One argument - switch to preset
        preset_name = sys.argv[1]
        manager.switch_to_preset(preset_name)
    else:
        print("Usage: python -m src.maintain.family_type_manager [preset_name]")
        print("\nAvailable presets:")
        for preset in manager.get_available_presets():
            print(f"  {preset}")


if __name__ == "__main__":
    main()
