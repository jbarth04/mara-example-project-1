import os
import sys
import logging
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from common.database import DatabaseConfig, execute_sql_file, execute_sql_statement
from common.events import EventSubscriber, EventPublisher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LeadProcessorService:
    def __init__(self):
        self.db_config = DatabaseConfig()
        self.event_subscriber = EventSubscriber()
        self.event_publisher = EventPublisher()
        self.sql_dir = Path(__file__).parent / 'sql'
        
        self.event_subscriber.subscribe('new_marketing_data_available', self.process_marketing_data)
    
    def initialize_schemas(self):
        logger.info("Initializing processing schemas...")
        dwh_conn = self.db_config.get_connection('dwh')
        try:
            execute_sql_file(dwh_conn, self.sql_dir / 'create_schemas.sql')
            logger.info("Processing schemas initialized successfully")
        finally:
            dwh_conn.close()
    
    def preprocess_leads(self):
        logger.info("Preprocessing leads...")
        dwh_conn = self.db_config.get_connection('dwh')
        try:
            execute_sql_file(dwh_conn, self.sql_dir / 'preprocess_lead.sql')
            logger.info("Lead preprocessing completed successfully")
        finally:
            dwh_conn.close()
    
    def transform_leads(self):
        logger.info("Transforming leads...")
        dwh_conn = self.db_config.get_connection('dwh')
        try:
            execute_sql_file(dwh_conn, self.sql_dir / 'transform_lead.sql')
            logger.info("Lead transformation completed successfully")
        finally:
            dwh_conn.close()
    
    def replace_schema(self):
        logger.info("Replacing m_dim schema with processed data...")
        dwh_conn = self.db_config.get_connection('dwh')
        try:
            execute_sql_statement(dwh_conn, "DROP SCHEMA IF EXISTS m_dim CASCADE; ALTER SCHEMA m_dim_next RENAME TO m_dim;")
            logger.info("Schema replacement completed successfully")
        finally:
            dwh_conn.close()
    
    def process_marketing_data(self, event_data):
        logger.info(f"Processing marketing data event: {event_data}")
        
        try:
            self.initialize_schemas()
            self.preprocess_leads()
            self.transform_leads()
            self.replace_schema()
            
            self.event_publisher.publish_event(
                'lead_processed',
                {
                    'processed_tables': event_data.get('tables_loaded', []),
                    'output_schema': 'm_dim',
                    'output_table': 'lead'
                }
            )
            
            logger.info("Lead processing completed successfully")
            
        except Exception as e:
            logger.error(f"Lead processing failed: {e}")
            raise
    
    def start(self):
        logger.info("Starting Lead Processor Service...")
        self.event_subscriber.listen()

def main():
    service = LeadProcessorService()
    try:
        service.start()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")

if __name__ == "__main__":
    main()
