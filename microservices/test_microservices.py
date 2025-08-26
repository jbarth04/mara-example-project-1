#!/usr/bin/env python3

import sys
import time
import logging
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from common.database import DatabaseConfig
from common.events import EventPublisher, EventSubscriber

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_database_connections():
    logger.info("Testing database connections...")
    db_config = DatabaseConfig()
    
    try:
        dwh_conn = db_config.get_connection('dwh')
        with dwh_conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1
        dwh_conn.close()
        logger.info("✓ DWH database connection successful")
    except Exception as e:
        logger.error(f"✗ DWH database connection failed: {e}")
        return False
    
    try:
        olist_conn = db_config.get_connection('olist')
        with olist_conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1
        olist_conn.close()
        logger.info("✓ Olist database connection successful")
    except Exception as e:
        logger.error(f"✗ Olist database connection failed: {e}")
        return False
    
    return True

def test_redis_connection():
    logger.info("Testing Redis connection...")
    try:
        publisher = EventPublisher()
        publisher.redis_client.ping()
        logger.info("✓ Redis connection successful")
        return True
    except Exception as e:
        logger.error(f"✗ Redis connection failed: {e}")
        return False

def test_event_publishing():
    logger.info("Testing event publishing...")
    try:
        publisher = EventPublisher()
        publisher.publish_event('test_event', {'message': 'test'})
        logger.info("✓ Event publishing successful")
        return True
    except Exception as e:
        logger.error(f"✗ Event publishing failed: {e}")
        return False

def main():
    logger.info("Starting microservice tests...")
    
    tests = [
        test_database_connections,
        test_redis_connection,
        test_event_publishing
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            logger.error(f"Test {test.__name__} failed with exception: {e}")
            failed += 1
    
    logger.info(f"Tests completed: {passed} passed, {failed} failed")
    
    if failed > 0:
        sys.exit(1)
    else:
        logger.info("All tests passed!")

if __name__ == "__main__":
    main()
