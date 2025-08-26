import asyncio
import asyncpg
import logging
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from .config import DatabaseConfig, ServiceConfig

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, config: ServiceConfig):
        self.config = config
        self.olist_pool: Optional[asyncpg.Pool] = None
        self.dwh_pool: Optional[asyncpg.Pool] = None

    async def initialize(self):
        self.olist_pool = await self._create_pool(self.config.olist_db)
        self.dwh_pool = await self._create_pool(self.config.dwh_db)
        logger.info("Database pools initialized successfully")

    async def close(self):
        if self.olist_pool:
            await self.olist_pool.close()
        if self.dwh_pool:
            await self.dwh_pool.close()
        logger.info("Database pools closed")

    async def _create_pool(self, db_config: DatabaseConfig) -> asyncpg.Pool:
        dsn = f"postgresql://{db_config.user}"
        if db_config.password:
            dsn += f":{db_config.password}"
        dsn += f"@{db_config.host}:{db_config.port}/{db_config.database}"
        
        return await asyncpg.create_pool(
            dsn,
            min_size=1,
            max_size=10,
            command_timeout=60
        )

    @asynccontextmanager
    async def get_olist_connection(self):
        if not self.olist_pool:
            raise RuntimeError("Database not initialized")
        async with self.olist_pool.acquire() as conn:
            yield conn

    @asynccontextmanager
    async def get_dwh_connection(self):
        if not self.dwh_pool:
            raise RuntimeError("Database not initialized")
        async with self.dwh_pool.acquire() as conn:
            yield conn

    async def initialize_schema(self):
        async with self.get_dwh_connection() as conn:
            async with conn.transaction():
                await conn.execute("DROP SCHEMA IF EXISTS m_data CASCADE")
                await conn.execute("CREATE SCHEMA m_data")
                
                await conn.execute("""
                    SELECT util.create_chunking_functions('m_data')
                """)
                
                await self._create_closed_deal_table(conn)
                await self._create_marketing_qualified_lead_table(conn)
                
        logger.info("Schema initialized successfully")

    async def _create_closed_deal_table(self, conn):
        await conn.execute("""
            DROP TABLE IF EXISTS m_data.closed_deal CASCADE;
            CREATE TABLE m_data.closed_deal
            (
                mql_id                        TEXT,
                seller_id                     TEXT,
                sdr_id                        TEXT,
                sr_id                         TEXT,
                won_date                      TIMESTAMP WITH TIME ZONE,
                business_segment              TEXT,
                lead_type                     TEXT,
                lead_behaviour_profile        TEXT,
                has_company                   TEXT,
                has_gtin                      TEXT,
                average_stock                 TEXT,
                business_type                 TEXT,
                declared_product_catalog_size DOUBLE PRECISION,
                declared_monthly_revenue      DOUBLE PRECISION
            );
        """)

    async def _create_marketing_qualified_lead_table(self, conn):
        await conn.execute("""
            DROP TABLE IF EXISTS m_data.marketing_qualified_lead CASCADE;
            CREATE TABLE m_data.marketing_qualified_lead
            (
                mql_id             TEXT,
                first_contact_date TIMESTAMP WITH TIME ZONE,
                landing_page_id    TEXT,
                origin             TEXT
            );
        """)

    async def load_table_data(self, table_name: str) -> Dict[str, Any]:
        source_table = f"marketing.{table_name}s"
        target_table = f"m_data.{table_name}"
        
        start_time = asyncio.get_event_loop().time()
        
        try:
            async with self.get_olist_connection() as source_conn:
                rows = await source_conn.fetch(f"SELECT * FROM {source_table}")
                
            if not rows:
                logger.warning(f"No data found in {source_table}")
                return {
                    "table": table_name,
                    "status": "success",
                    "records_loaded": 0,
                    "duration": 0.0
                }
            
            async with self.get_dwh_connection() as target_conn:
                async with target_conn.transaction():
                    await target_conn.execute(f"TRUNCATE TABLE {target_table}")
                    
                    columns = list(rows[0].keys())
                    placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
                    insert_sql = f"""
                        INSERT INTO {target_table} ({', '.join(columns)})
                        VALUES ({placeholders})
                    """
                    
                    values_list = [tuple(row[col] for col in columns) for row in rows]
                    await target_conn.executemany(insert_sql, values_list)
            
            duration = asyncio.get_event_loop().time() - start_time
            records_loaded = len(rows)
            
            logger.info(f"Successfully loaded {records_loaded} records into {target_table} in {duration:.2f}s")
            
            return {
                "table": table_name,
                "status": "success",
                "records_loaded": records_loaded,
                "duration": duration
            }
            
        except Exception as e:
            duration = asyncio.get_event_loop().time() - start_time
            logger.error(f"Failed to load data for {table_name}: {str(e)}")
            
            return {
                "table": table_name,
                "status": "error",
                "error": str(e),
                "duration": duration
            }

    async def verify_data_integrity(self, table_name: str) -> bool:
        source_table = f"marketing.{table_name}s"
        target_table = f"m_data.{table_name}"
        
        try:
            async with self.get_olist_connection() as source_conn:
                source_count = await source_conn.fetchval(f"SELECT COUNT(*) FROM {source_table}")
                
            async with self.get_dwh_connection() as target_conn:
                target_count = await target_conn.fetchval(f"SELECT COUNT(*) FROM {target_table}")
                
            if source_count == target_count:
                logger.info(f"Data integrity verified for {table_name}: {source_count} records")
                return True
            else:
                logger.error(f"Data integrity check failed for {table_name}: source={source_count}, target={target_count}")
                return False
                
        except Exception as e:
            logger.error(f"Data integrity check failed for {table_name}: {str(e)}")
            return False
