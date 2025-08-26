#!/usr/bin/env python3

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

def test_imports():
    print("Testing imports...")
    
    try:
        from common.database import DatabaseConfig
        from common.events import EventPublisher, EventSubscriber
        print("✓ Common modules imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import common modules: {e}")
        return False
    
    try:
        from data_loader.service import DataLoaderService
        print("✓ Data loader service imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import data loader service: {e}")
        return False
    
    try:
        from lead_processor.service import LeadProcessorService
        print("✓ Lead processor service imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import lead processor service: {e}")
        return False
    
    try:
        db_config = DatabaseConfig()
        print("✓ Database config created successfully")
    except Exception as e:
        print(f"✗ Failed to create database config: {e}")
        return False
    
    try:
        publisher = EventPublisher()
        print("✓ Event publisher created successfully")
    except Exception as e:
        print(f"✗ Failed to create event publisher: {e}")
        return False
    
    print("All imports and basic instantiation tests passed!")
    return True

if __name__ == "__main__":
    success = test_imports()
    sys.exit(0 if success else 1)
