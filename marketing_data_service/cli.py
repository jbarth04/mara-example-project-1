import asyncio
import logging
import sys
import argparse
from .loader import MarketingDataLoader
from .config import load_config


def setup_logging(level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('marketing_data_service.log')
        ]
    )


async def run_load():
    config = load_config()
    setup_logging(config.log_level)
    
    loader = MarketingDataLoader(config)
    
    try:
        await loader.initialize()
        result = await loader.load_marketing_data()
        
        print(f"Load completed successfully:")
        print(f"  Total tables: {result['total_tables']}")
        print(f"  Successful: {result['successful_tables']}")
        print(f"  Failed: {result['failed_tables']}")
        
        for table_result in result['results']:
            status = table_result['status']
            table = table_result['table']
            if status == 'success':
                records = table_result['records_loaded']
                duration = table_result['duration']
                print(f"  {table}: {records} records in {duration:.2f}s")
            else:
                error = table_result['error']
                print(f"  {table}: FAILED - {error}")
        
        if 'integrity_checks' in result:
            print("\nData integrity checks:")
            for check in result['integrity_checks']:
                status = "PASS" if check['integrity_ok'] else "FAIL"
                print(f"  {check['table']}: {status}")
        
        return result['failed_tables'] == 0
        
    except Exception as e:
        print(f"Load failed: {str(e)}")
        return False
    finally:
        await loader.close()


async def run_health_check():
    config = load_config()
    setup_logging(config.log_level)
    
    loader = MarketingDataLoader(config)
    
    try:
        await loader.initialize()
        health = await loader.health_check()
        
        print(f"Service: {health['service']}")
        print(f"Status: {health['status']}")
        print("Database connections:")
        for db_name, db_status in health['databases'].items():
            print(f"  {db_name}: {db_status}")
        
        return health['status'] == 'healthy'
        
    except Exception as e:
        print(f"Health check failed: {str(e)}")
        return False
    finally:
        await loader.close()


def main():
    parser = argparse.ArgumentParser(description='Marketing Data Loading Microservice CLI')
    parser.add_argument('command', choices=['load', 'health'], help='Command to execute')
    
    args = parser.parse_args()
    
    if args.command == 'load':
        success = asyncio.run(run_load())
        sys.exit(0 if success else 1)
    elif args.command == 'health':
        success = asyncio.run(run_health_check())
        sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
