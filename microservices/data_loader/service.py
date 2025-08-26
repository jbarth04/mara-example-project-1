import os
import sys
import logging
import time
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from common.database import DatabaseConfig, execute_sql_file, execute_sql_statement
from common.events import EventPublisher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoaderService:
    def __init__(self):
        self.db_config = DatabaseConfig()
        self.event_publisher = EventPublisher()
        self.sql_dir = Path(__file__).parent / 'sql'
    
    def initialize_schema(self):
        logger.info("Initializing marketing data schema...")
        dwh_conn = self.db_config.get_connection('dwh')
        try:
            execute_sql_file(dwh_conn, self.sql_dir / 'recreate_marketing_data_schema.sql')
            logger.info("Marketing data schema initialized successfully")
        finally:
            dwh_conn.close()
    
    def load_table(self, table_name: str):
        logger.info(f"Loading {table_name} data...")
        
        olist_conn = self.db_config.get_connection('olist')
        dwh_conn = self.db_config.get_connection('dwh')
        
        try:
            execute_sql_file(dwh_conn, self.sql_dir / f'create_{table_name}_table.sql')
            
            copy_sql = f"""
            INSERT INTO m_data.{table_name}
            SELECT * FROM marketing.{table_name}s;
            """
            
            with olist_conn.cursor() as source_cursor:
                source_cursor.execute(f"SELECT * FROM marketing.{table_name}s")
                rows = source_cursor.fetchall()
                
                if rows:
                    columns = [desc[0] for desc in source_cursor.description]
                    placeholders = ','.join(['%s'] * len(columns))
                    insert_sql = f"INSERT INTO m_data.{table_name} ({','.join(columns)}) VALUES ({placeholders})"
                    
                    with dwh_conn.cursor() as target_cursor:
                        target_cursor.executemany(insert_sql, rows)
                        dwh_conn.commit()
                        
                    logger.info(f"Loaded {len(rows)} rows into m_data.{table_name}")
                else:
                    logger.warning(f"No data found in marketing.{table_name}s")
                    
        except Exception as e:
            logger.error(f"Error loading {table_name}: {e}")
            raise
        finally:
            olist_conn.close()
            dwh_conn.close()
    
    def load_marketing_data(self):
        logger.info("Starting marketing data loading process...")
        
        try:
            self.initialize_schema()
            
            tables = ['closed_deal', 'marketing_qualified_lead']
            
            for table in tables:
                self.load_table(table)
            
            self.event_publisher.publish_event(
                'new_marketing_data_available',
                {
                    'tables_loaded': tables,
                    'schema': 'm_data'
                }
            )
            
            logger.info("Marketing data loading completed successfully")
            
        except Exception as e:
            logger.error(f"Marketing data loading failed: {e}")
            raise

def main():
    service = DataLoaderService()
    
    while True:
        try:
            service.load_marketing_data()
            logger.info("Sleeping for 300 seconds before next load...")
            time.sleep(300)
        except KeyboardInterrupt:
            logger.info("Service stopped by user")
            break
        except Exception as e:
            logger.error(f"Service error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()
