#!/usr/bin/env python3
"""Test script to verify gcloud authentication for BigQuery to ERD tool."""

import sys
import os
from pathlib import Path

def test_gcloud_installation():
    """Test if gcloud CLI is installed."""
    import subprocess
    try:
        result = subprocess.run(['gcloud', '--version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("✓ gcloud CLI is installed")
            print(f"  Version: {result.stdout.split()[0]}")
            return True
        else:
            print("✗ gcloud CLI not found")
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("✗ gcloud CLI not found or not in PATH")
        return False

def test_gcloud_auth():
    """Test gcloud authentication status."""
    import subprocess
    try:
        # Check if user is authenticated
        result = subprocess.run(['gcloud', 'auth', 'list'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0 and "ACTIVE" in result.stdout:
            print("✓ gcloud user authentication active")
            return True
        else:
            print("✗ No active gcloud user authentication")
            print("  Run: gcloud auth login")
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("✗ gcloud CLI not available")
        return False

def test_adc_auth():
    """Test Application Default Credentials."""
    import subprocess
    try:
        # Check ADC
        result = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0 and result.stdout.strip():
            print("✓ Application Default Credentials active")
            return True
        else:
            print("✗ Application Default Credentials not set")
            print("  Run: gcloud auth application-default login")
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("✗ gcloud CLI not available")
        return False

def test_bigquery_access():
    """Test BigQuery access."""
    try:
        from google.cloud import bigquery
        
        # Try to create a client
        client = bigquery.Client()
        
        # Try to list datasets (this will fail if no permissions)
        datasets = list(client.list_datasets(max_results=1))
        print("✓ BigQuery access confirmed")
        return True
        
    except ImportError:
        print("✗ google-cloud-bigquery not installed")
        print("  Run: pip install google-cloud-bigquery")
        return False
    except Exception as e:
        if "credentials" in str(e).lower():
            print("✗ BigQuery access denied - authentication issue")
            print("  Run: gcloud auth application-default login")
        else:
            print(f"✗ BigQuery access failed: {e}")
        return False

def test_project_config():
    """Test if project is configured."""
    import subprocess
    try:
        result = subprocess.run(['gcloud', 'config', 'get-value', 'project'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0 and result.stdout.strip():
            project = result.stdout.strip()
            print(f"✓ Default project set: {project}")
            return True
        else:
            print("✗ No default project set")
            print("  Run: gcloud config set project YOUR_PROJECT_ID")
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("✗ gcloud CLI not available")
        return False

def main():
    """Run all authentication tests."""
    print("Testing gcloud authentication for BigQuery to ERD tool...")
    print("=" * 60)
    
    tests = [
        ("gcloud CLI installation", test_gcloud_installation),
        ("gcloud user authentication", test_gcloud_auth),
        ("Application Default Credentials", test_adc_auth),
        ("BigQuery access", test_bigquery_access),
        ("Project configuration", test_project_config),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        if test_func():
            passed += 1
        else:
            print(f"  → Fix this issue before using the tool")
    
    print("\n" + "=" * 60)
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("✓ All tests passed! You're ready to use the tool.")
        print("\nNext steps:")
        print("1. Update your .env file with PROJECT_ID and DATASET_ID")
        print("2. Run: bigquery-to-erd")
        return True
    else:
        print("✗ Some tests failed. Please fix the issues above.")
        print("\nQuick setup:")
        print("1. gcloud auth login")
        print("2. gcloud auth application-default login")
        print("3. gcloud config set project YOUR_PROJECT_ID")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
