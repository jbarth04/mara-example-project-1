import asyncio
import logging
from typing import List, Dict, Any
from .database import DatabaseManager
from .events import EventPublisher, create_event_publisher
from .config import ServiceConfig

logger = logging.getLogger(__name__)


class MarketingDataLoader:
    def __init__(self, config: ServiceConfig):
        self.config = config
        self.db_manager = DatabaseManager(config)
        self.event_publisher = create_event_publisher(
            config.event_publisher_type, 
            self.db_manager
        )
        self.tables = ["closed_deal", "marketing_qualified_lead"]

    async def initialize(self):
        await self.db_manager.initialize()
        logger.info("Marketing data loader initialized")

    async def close(self):
        await self.db_manager.close()
        logger.info("Marketing data loader closed")

    async def load_marketing_data(self) -> Dict[str, Any]:
        try:
            logger.info("Starting marketing data load process")
            
            await self.db_manager.initialize_schema()
            
            tasks = []
            for table in self.tables:
                task = self._load_table_with_retry(table)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            load_results = []
            has_errors = False
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    error_result = {
                        "table": self.tables[i],
                        "status": "error",
                        "error": str(result)
                    }
                    load_results.append(error_result)
                    has_errors = True
                    logger.error(f"Failed to load {self.tables[i]}: {str(result)}")
                else:
                    load_results.append(result)
                    if isinstance(result, dict) and result.get("status") == "error":
                        has_errors = True

            summary = {
                "total_tables": len(self.tables),
                "successful_tables": len([r for r in load_results if r["status"] == "success"]),
                "failed_tables": len([r for r in load_results if r["status"] == "error"]),
                "results": load_results
            }

            if has_errors:
                await self.event_publisher.publish_failure_event(
                    "Marketing data load completed with errors",
                    summary
                )
            else:
                await self.event_publisher.publish_success_event(summary)
                
                integrity_checks = []
                for table in self.tables:
                    check_result = await self.db_manager.verify_data_integrity(table)
                    integrity_checks.append({"table": table, "integrity_ok": check_result})
                
                summary["integrity_checks"] = integrity_checks

            logger.info(f"Marketing data load completed: {summary}")
            return summary

        except Exception as e:
            error_msg = f"Marketing data load failed: {str(e)}"
            logger.error(error_msg)
            await self.event_publisher.publish_failure_event(error_msg)
            raise

    async def _load_table_with_retry(self, table_name: str) -> Dict[str, Any]:
        last_error = None
        
        for attempt in range(self.config.retry_attempts):
            try:
                if attempt > 0:
                    logger.info(f"Retrying {table_name} load (attempt {attempt + 1}/{self.config.retry_attempts})")
                    await asyncio.sleep(self.config.retry_delay * attempt)
                
                result = await self.db_manager.load_table_data(table_name)
                
                if result["status"] == "success":
                    return result
                else:
                    last_error = result.get("error", "Unknown error")
                    
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Attempt {attempt + 1} failed for {table_name}: {last_error}")
        
        return {
            "table": table_name,
            "status": "error",
            "error": f"Failed after {self.config.retry_attempts} attempts: {last_error}"
        }

    async def health_check(self) -> Dict[str, Any]:
        try:
            async with self.db_manager.get_olist_connection() as olist_conn:
                await olist_conn.fetchval("SELECT 1")
                olist_status = "healthy"
        except Exception as e:
            olist_status = f"unhealthy: {str(e)}"

        try:
            async with self.db_manager.get_dwh_connection() as dwh_conn:
                await dwh_conn.fetchval("SELECT 1")
                dwh_status = "healthy"
        except Exception as e:
            dwh_status = f"unhealthy: {str(e)}"

        return {
            "service": "marketing-data-loader",
            "status": "healthy" if "unhealthy" not in f"{olist_status}{dwh_status}" else "unhealthy",
            "databases": {
                "olist": olist_status,
                "dwh": dwh_status
            }
        }
