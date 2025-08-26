import os
import psycopg2
from typing import Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

class DatabaseConfig:
    def __init__(self):
        self.databases = {
            'dwh': {
                'user': os.getenv('DWH_DB_USER', 'root'),
                'host': os.getenv('DWH_DB_HOST', 'localhost'),
                'database': os.getenv('DWH_DB_NAME', 'example_project_1_dwh'),
                'password': os.getenv('DWH_DB_PASSWORD', ''),
                'port': os.getenv('DWH_DB_PORT', '5432')
            },
            'olist': {
                'user': os.getenv('OLIST_DB_USER', 'root'),
                'host': os.getenv('OLIST_DB_HOST', 'localhost'),
                'database': os.getenv('OLIST_DB_NAME', 'olist_ecommerce'),
                'password': os.getenv('OLIST_DB_PASSWORD', ''),
                'port': os.getenv('OLIST_DB_PORT', '5432')
            }
        }

    def get_connection(self, db_alias: str):
        if db_alias not in self.databases:
            raise ValueError(f"Unknown database alias: {db_alias}")
        
        config = self.databases[db_alias]
        return psycopg2.connect(
            host=config['host'],
            database=config['database'],
            user=config['user'],
            password=config['password'],
            port=config['port']
        )

def execute_sql_file(connection, sql_file_path: str, params: Optional[Dict[str, Any]] = None):
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    
    with connection.cursor() as cursor:
        if params:
            cursor.execute(sql_content, params)
        else:
            cursor.execute(sql_content)
    
    connection.commit()

def execute_sql_statement(connection, sql_statement: str, params: Optional[Dict[str, Any]] = None):
    with connection.cursor() as cursor:
        if params:
            cursor.execute(sql_statement, params)
        else:
            cursor.execute(sql_statement)
    
    connection.commit()
