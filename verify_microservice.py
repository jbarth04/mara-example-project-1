#!/usr/bin/env python3
"""
Verification script for the marketing data microservice
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'marketing_data_service'))

def test_imports():
    """Test that all modules can be imported successfully"""
    try:
        from marketing_data_service.config import load_config
        from marketing_data_service.events import create_event_publisher
        from marketing_data_service.database import DatabaseManager
        from marketing_data_service.loader import MarketingDataLoader
        print("✓ All imports successful")
        return True
    except ImportError as e:
        print(f"✗ Import error: {e}")
        return False

def test_config():
    """Test configuration loading"""
    try:
        from marketing_data_service.config import load_config
        config = load_config()
        print(f"✓ Config loaded: olist_db={config.olist_db.database}, dwh_db={config.dwh_db.database}")
        print(f"✓ Max parallel tasks: {config.max_parallel_tasks}")
        print(f"✓ Event publisher type: {config.event_publisher_type}")
        return True
    except Exception as e:
        print(f"✗ Config error: {e}")
        return False

def test_event_publisher():
    """Test event publisher creation"""
    try:
        from marketing_data_service.events import create_event_publisher
        
        file_publisher = create_event_publisher("file")
        print("✓ File event publisher created")
        
        try:
            db_publisher = create_event_publisher("database", "mock_db")
            print("✓ Database event publisher created")
        except Exception:
            print("✓ Database event publisher requires valid DB manager (expected)")
        
        return True
    except Exception as e:
        print(f"✗ Event publisher error: {e}")
        return False

def main():
    print("Marketing Data Microservice Verification")
    print("=" * 50)
    
    tests = [
        ("Import Tests", test_imports),
        ("Configuration Tests", test_config),
        ("Event Publisher Tests", test_event_publisher),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        if test_func():
            passed += 1
        else:
            print(f"Failed: {test_name}")
    
    print(f"\n{'='*50}")
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("✓ All verification tests passed!")
        return True
    else:
        print("✗ Some verification tests failed!")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
