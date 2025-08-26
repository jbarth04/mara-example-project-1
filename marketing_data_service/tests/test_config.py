import os
import pytest
from marketing_data_service.config import load_config, DatabaseConfig, ServiceConfig


def test_load_config_defaults():
    config = load_config()
    
    assert config.olist_db.user == "root"
    assert config.olist_db.host == "localhost"
    assert config.olist_db.database == "olist_ecommerce"
    assert config.olist_db.port == 5432
    
    assert config.dwh_db.user == "root"
    assert config.dwh_db.host == "localhost"
    assert config.dwh_db.database == "example_project_1_dwh"
    assert config.dwh_db.port == 5432
    
    assert config.max_parallel_tasks == 5
    assert config.retry_attempts == 3
    assert config.retry_delay == 1.0
    assert config.log_level == "INFO"
    assert config.event_publisher_type == "database"


def test_load_config_from_env(monkeypatch):
    monkeypatch.setenv("OLIST_DB_USER", "test_user")
    monkeypatch.setenv("OLIST_DB_HOST", "test_host")
    monkeypatch.setenv("OLIST_DB_NAME", "test_db")
    monkeypatch.setenv("OLIST_DB_PORT", "5433")
    monkeypatch.setenv("MAX_PARALLEL_TASKS", "10")
    monkeypatch.setenv("RETRY_ATTEMPTS", "5")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("EVENT_PUBLISHER_TYPE", "file")
    
    config = load_config()
    
    assert config.olist_db.user == "test_user"
    assert config.olist_db.host == "test_host"
    assert config.olist_db.database == "test_db"
    assert config.olist_db.port == 5433
    assert config.max_parallel_tasks == 10
    assert config.retry_attempts == 5
    assert config.log_level == "DEBUG"
    assert config.event_publisher_type == "file"


def test_database_config():
    db_config = DatabaseConfig(
        user="test_user",
        host="test_host",
        database="test_db",
        port=5433,
        password="test_pass"
    )
    
    assert db_config.user == "test_user"
    assert db_config.host == "test_host"
    assert db_config.database == "test_db"
    assert db_config.port == 5433
    assert db_config.password == "test_pass"
