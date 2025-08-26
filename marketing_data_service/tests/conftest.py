import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from marketing_data_service.config import ServiceConfig, DatabaseConfig


@pytest.fixture
def mock_config():
    return ServiceConfig(
        olist_db=DatabaseConfig(
            user="test_user",
            host="localhost",
            database="test_olist",
            port=5432
        ),
        dwh_db=DatabaseConfig(
            user="test_user",
            host="localhost",
            database="test_dwh",
            port=5432
        ),
        max_parallel_tasks=2,
        retry_attempts=2,
        retry_delay=0.1,
        log_level="DEBUG",
        event_publisher_type="file"
    )


@pytest.fixture
def mock_db_manager():
    manager = MagicMock()
    manager.initialize = AsyncMock()
    manager.close = AsyncMock()
    manager.initialize_schema = AsyncMock()
    manager.load_table_data = AsyncMock()
    manager.verify_data_integrity = AsyncMock(return_value=True)
    
    mock_conn = AsyncMock()
    manager.get_olist_connection.return_value.__aenter__.return_value = mock_conn
    manager.get_dwh_connection.return_value.__aenter__.return_value = mock_conn
    
    return manager


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
