import pytest
import json
import tempfile
import os
from unittest.mock import AsyncMock, MagicMock
from marketing_data_service.events import DatabaseEventPublisher, FileEventPublisher, create_event_publisher


@pytest.mark.asyncio
async def test_database_event_publisher_success():
    mock_db_manager = MagicMock()
    mock_conn = AsyncMock()
    mock_db_manager.get_dwh_connection.return_value.__aenter__.return_value = mock_conn
    
    publisher = DatabaseEventPublisher(mock_db_manager)
    
    load_results = {
        "total_tables": 2,
        "successful_tables": 2,
        "failed_tables": 0
    }
    
    await publisher.publish_success_event(load_results)
    
    assert mock_conn.execute.call_count == 2


@pytest.mark.asyncio
async def test_database_event_publisher_failure():
    mock_db_manager = MagicMock()
    mock_conn = AsyncMock()
    mock_db_manager.get_dwh_connection.return_value.__aenter__.return_value = mock_conn
    
    publisher = DatabaseEventPublisher(mock_db_manager)
    
    await publisher.publish_failure_event("Test error", {"detail": "test"})
    
    assert mock_conn.execute.call_count == 2


@pytest.mark.asyncio
async def test_file_event_publisher_success():
    with tempfile.TemporaryDirectory() as temp_dir:
        publisher = FileEventPublisher(temp_dir)
        
        load_results = {
            "total_tables": 2,
            "successful_tables": 2,
            "failed_tables": 0
        }
        
        await publisher.publish_success_event(load_results)
        
        files = os.listdir(temp_dir)
        assert len(files) == 1
        assert files[0].startswith("marketing-data-loaded_")
        
        with open(os.path.join(temp_dir, files[0])) as f:
            event_data = json.load(f)
            assert event_data["event_type"] == "marketing-data-loaded"
            assert event_data["status"] == "success"
            assert event_data["results"] == load_results


@pytest.mark.asyncio
async def test_file_event_publisher_failure():
    with tempfile.TemporaryDirectory() as temp_dir:
        publisher = FileEventPublisher(temp_dir)
        
        await publisher.publish_failure_event("Test error", {"detail": "test"})
        
        files = os.listdir(temp_dir)
        assert len(files) == 1
        assert files[0].startswith("marketing-data-load-failed_")
        
        with open(os.path.join(temp_dir, files[0])) as f:
            event_data = json.load(f)
            assert event_data["event_type"] == "marketing-data-load-failed"
            assert event_data["status"] == "failure"
            assert event_data["error"] == "Test error"


def test_create_event_publisher_database():
    mock_db_manager = MagicMock()
    publisher = create_event_publisher("database", mock_db_manager)
    assert isinstance(publisher, DatabaseEventPublisher)


def test_create_event_publisher_file():
    publisher = create_event_publisher("file")
    assert isinstance(publisher, FileEventPublisher)


def test_create_event_publisher_invalid():
    with pytest.raises(ValueError):
        create_event_publisher("invalid")


def test_create_event_publisher_database_no_manager():
    with pytest.raises(ValueError):
        create_event_publisher("database")
