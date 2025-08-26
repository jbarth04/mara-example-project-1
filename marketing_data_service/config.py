import os
from typing import Dict, Any
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    user: str
    host: str
    database: str
    port: int = 5432
    password: str = ""


@dataclass
class ServiceConfig:
    olist_db: DatabaseConfig
    dwh_db: DatabaseConfig
    max_parallel_tasks: int = 5
    retry_attempts: int = 3
    retry_delay: float = 1.0
    log_level: str = "INFO"
    event_publisher_type: str = "database"


def load_config() -> ServiceConfig:
    return ServiceConfig(
        olist_db=DatabaseConfig(
            user=os.getenv("OLIST_DB_USER", "root"),
            host=os.getenv("OLIST_DB_HOST", "localhost"),
            database=os.getenv("OLIST_DB_NAME", "olist_ecommerce"),
            port=int(os.getenv("OLIST_DB_PORT", "5432")),
            password=os.getenv("OLIST_DB_PASSWORD", "")
        ),
        dwh_db=DatabaseConfig(
            user=os.getenv("DWH_DB_USER", "root"),
            host=os.getenv("DWH_DB_HOST", "localhost"),
            database=os.getenv("DWH_DB_NAME", "example_project_1_dwh"),
            port=int(os.getenv("DWH_DB_PORT", "5432")),
            password=os.getenv("DWH_DB_PASSWORD", "")
        ),
        max_parallel_tasks=int(os.getenv("MAX_PARALLEL_TASKS", "5")),
        retry_attempts=int(os.getenv("RETRY_ATTEMPTS", "3")),
        retry_delay=float(os.getenv("RETRY_DELAY", "1.0")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        event_publisher_type=os.getenv("EVENT_PUBLISHER_TYPE", "database")
    )
