#!/usr/bin/env python3
"""
Main validation runner for the Insurance Policy System.

This script runs all critical validations to ensure the system is working correctly.
It should be called after each data generation cycle to verify perfect reconciliation.
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

def run_comprehensive_validation():
    """Run comprehensive validation of the insurance policy system."""
    
    print("=" * 80)
    print("INSURANCE POLICY SYSTEM - COMPREHENSIVE VALIDATION")
    print("=" * 80)
    print(f"Validation started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Change to the validate directory to run the scripts
    validate_dir = Path(__file__).parent
    original_cwd = os.getcwd()
    os.chdir(validate_dir)
    
    validation_results = []
    
    try:
        # 1. Test assignment premium storage
        print("1. Testing Assignment Premium Storage...")
        print("-" * 40)
        import test_assignment_premiums
        test_assignment_premiums.main()
        validation_results.append(("Assignment Premium Storage", "PASSED"))
        print("✅ Assignment premium storage test PASSED")
        print()
        
    except Exception as e:
        print(f"❌ Assignment premium storage test FAILED: {e}")
        validation_results.append(("Assignment Premium Storage", f"FAILED: {e}"))
        print()
    
    try:
        # 2. Test policy fixes
        print("2. Testing Policy Fixes...")
        print("-" * 40)
        import test_policy2_fix
        test_policy2_fix.main()
        validation_results.append(("Policy Fixes", "PASSED"))
        print("✅ Policy fixes test PASSED")
        print()
        
    except Exception as e:
        print(f"❌ Policy fixes test FAILED: {e}")
        validation_results.append(("Policy Fixes", f"FAILED: {e}"))
        print()
    
    try:
        # 3. Run comprehensive reconciliation
        print("3. Running Comprehensive Reconciliation...")
        print("-" * 40)
        import validate_assignment_premiums
        validate_assignment_premiums.main()
        validation_results.append(("Comprehensive Reconciliation", "PASSED"))
        print("✅ Comprehensive reconciliation PASSED")
        print()
        
    except Exception as e:
        print(f"❌ Comprehensive reconciliation FAILED: {e}")
        validation_results.append(("Comprehensive Reconciliation", f"FAILED: {e}"))
        print()
    
    try:
        # 4. Run premium calculation validation
        print("4. Running Premium Calculation Validation...")
        print("-" * 40)
        import validate_premium_calculations
        validate_premium_calculations.main()
        validation_results.append(("Premium Calculation Validation", "PASSED"))
        print("✅ Premium calculation validation PASSED")
        print()
        
    except Exception as e:
        print(f"❌ Premium calculation validation FAILED: {e}")
        validation_results.append(("Premium Calculation Validation", f"FAILED: {e}"))
        print()
    
    # Summary
    print("=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Validation completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    passed_count = sum(1 for _, status in validation_results if status == "PASSED")
    total_count = len(validation_results)
    
    for test_name, status in validation_results:
        status_icon = "✅" if status == "PASSED" else "❌"
        print(f"{status_icon} {test_name}: {status}")
    
    print()
    print(f"Overall Result: {passed_count}/{total_count} validations passed")
    
    # Restore original working directory
    os.chdir(original_cwd)
    
    if passed_count == total_count:
        print("🎉 ALL VALIDATIONS PASSED - SYSTEM IS WORKING PERFECTLY!")
        return True
    else:
        print("⚠️  SOME VALIDATIONS FAILED - SYSTEM NEEDS ATTENTION!")
        return False

def main():
    """Main entry point for validation."""
    success = run_comprehensive_validation()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
