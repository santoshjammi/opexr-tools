#!/usr/bin/env python3
"""
Test script for the Data Query API endpoints.
"""
import requests
import json
from typing import Dict, Any

BASE_URL = "http://localhost:8000"


def test_endpoint(name: str, url: str, method: str = "GET") -> Dict[str, Any]:
    """Test an API endpoint and print results."""
    print(f"\n{'='*60}")
    print(f"Testing: {name}")
    print(f"{'='*60}")
    print(f"URL: {url}")
    
    try:
        if method == "GET":
            response = requests.get(url, timeout=10)
        elif method == "DELETE":
            response = requests.delete(url, timeout=10)
        
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Response (formatted):")
            print(json.dumps(data, indent=2))
            return data
        else:
            print(f"Error: {response.text}")
            return {}
    except Exception as e:
        print(f"Exception: {str(e)}")
        return {}


def main():
    """Run all API tests."""
    
    print("\n" + "="*60)
    print("DATA QUERY API - TEST SUITE")
    print("="*60)
    
    # Test 1: Root endpoint
    test_endpoint(
        "Root Endpoint",
        f"{BASE_URL}/"
    )
    
    # Test 2: List available datasets
    test_endpoint(
        "List Available Datasets",
        f"{BASE_URL}/data/available"
    )
    
    # Test 3: Get metadata
    test_endpoint(
        "Get Dataset Metadata",
        f"{BASE_URL}/data/metadata/ECCSEP05"
    )
    
    # Test 4: Get statistics
    test_endpoint(
        "Get Dataset Statistics",
        f"{BASE_URL}/data/stats/ECCSEP05"
    )
    
    # Test 5: Get records with pagination
    test_endpoint(
        "Get Records (First 5)",
        f"{BASE_URL}/data/records/ECCSEP05?skip=0&limit=5"
    )
    
    # Test 6: Get specific employee records
    test_endpoint(
        "Get Employee EMP_00001 Records",
        f"{BASE_URL}/data/employee/ECCSEP05/EMP_00001"
    )
    
    # Test 7: Search by wage type
    test_endpoint(
        "Search by Wage Type /101",
        f"{BASE_URL}/data/search/ECCSEP05?wage_type=/101&limit=3"
    )
    
    # Test 8: Search by amount range
    test_endpoint(
        "Search by Amount Range (50000-60000)",
        f"{BASE_URL}/data/search/ECCSEP05?min_amount=50000&max_amount=60000&limit=5"
    )
    
    # Test 9: Filter by Personnel Number
    test_endpoint(
        "Filter by Personnel Number 50169260",
        f"{BASE_URL}/data/records/ECCSEP05?pers_no=50169260&limit=10"
    )
    
    print("\n" + "="*60)
    print("ALL TESTS COMPLETED")
    print("="*60)
    print(f"\nAPI Documentation: {BASE_URL}/docs")
    print(f"Alternative Docs: {BASE_URL}/redoc")


if __name__ == "__main__":
    main()
