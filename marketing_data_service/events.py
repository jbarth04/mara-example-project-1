import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class EventPublisher(ABC):
    @abstractmethod
    async def publish_success_event(self, load_results: Dict[str, Any]):
        pass

    @abstractmethod
    async def publish_failure_event(self, error: str, details: Optional[Dict[str, Any]] = None):
        pass


class DatabaseEventPublisher(EventPublisher):
    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def publish_success_event(self, load_results: Dict[str, Any]):
        event_data = {
            "event_type": "marketing-data-loaded",
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "results": load_results
        }
        
        await self._store_event(event_data)
        logger.info(f"Published success event: {json.dumps(event_data, indent=2)}")

    async def publish_failure_event(self, error: str, details: Optional[Dict[str, Any]] = None):
        event_data = {
            "event_type": "marketing-data-load-failed",
            "status": "failure",
            "timestamp": datetime.utcnow().isoformat(),
            "error": error,
            "details": details or {}
        }
        
        await self._store_event(event_data)
        logger.error(f"Published failure event: {json.dumps(event_data, indent=2)}")

    async def _store_event(self, event_data: Dict[str, Any]):
        try:
            async with self.db_manager.get_dwh_connection() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS m_data.marketing_load_events (
                        id SERIAL PRIMARY KEY,
                        event_type TEXT NOT NULL,
                        status TEXT NOT NULL,
                        timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                        event_data JSONB NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)
                
                await conn.execute("""
                    INSERT INTO m_data.marketing_load_events 
                    (event_type, status, timestamp, event_data)
                    VALUES ($1, $2, $3, $4)
                """, event_data["event_type"], event_data["status"], 
                    datetime.fromisoformat(event_data["timestamp"]), json.dumps(event_data))
                    
        except Exception as e:
            logger.error(f"Failed to store event: {str(e)}")


class FileEventPublisher(EventPublisher):
    def __init__(self, events_dir: str = "./events"):
        self.events_dir = events_dir
        import os
        os.makedirs(events_dir, exist_ok=True)

    async def publish_success_event(self, load_results: Dict[str, Any]):
        event_data = {
            "event_type": "marketing-data-loaded",
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "results": load_results
        }
        
        await self._write_event_file(event_data)
        logger.info(f"Published success event to file: {json.dumps(event_data, indent=2)}")

    async def publish_failure_event(self, error: str, details: Optional[Dict[str, Any]] = None):
        event_data = {
            "event_type": "marketing-data-load-failed",
            "status": "failure",
            "timestamp": datetime.utcnow().isoformat(),
            "error": error,
            "details": details or {}
        }
        
        await self._write_event_file(event_data)
        logger.error(f"Published failure event to file: {json.dumps(event_data, indent=2)}")

    async def _write_event_file(self, event_data: Dict[str, Any]):
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"{self.events_dir}/{event_data['event_type']}_{timestamp}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(event_data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to write event file {filename}: {str(e)}")


def create_event_publisher(publisher_type: str, db_manager=None) -> EventPublisher:
    if publisher_type == "database":
        if not db_manager:
            raise ValueError("Database manager required for database event publisher")
        return DatabaseEventPublisher(db_manager)
    elif publisher_type == "file":
        return FileEventPublisher()
    else:
        raise ValueError(f"Unknown event publisher type: {publisher_type}")
