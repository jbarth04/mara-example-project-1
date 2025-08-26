# Marketing Data Loading Microservice

A standalone, event-driven microservice that extracts marketing data loading functionality from the existing Mara pipeline. This service loads data from `marketing.closed_deals` and `marketing.marketing_qualified_leads` tables in the `olist` database to `m_data.closed_deal` and `m_data.marketing_qualified_lead` tables in the data warehouse.

## Features

- **Event-driven architecture**: Publishes success/failure events with detailed results
- **Parallel processing**: Concurrent loading of both tables (configurable parallelism)
- **Database connection management**: Connection pooling for both source and target databases
- **Schema initialization**: Automatic creation of `m_data` schema and tables
- **Data validation**: Integrity checks to ensure data consistency
- **Monitoring**: Comprehensive logging, health checks, and load metrics
- **Idempotency**: Safe re-running without data duplication
- **Retry logic**: Configurable retry mechanisms for transient failures
- **Backward compatibility**: REST API for integration with existing Mara pipeline

## Architecture

The microservice consists of several key components:

- **Database Manager**: Handles connection pooling and data operations
- **Event Publisher**: Publishes load events (database or file-based)
- **Data Loader**: Orchestrates the loading process with parallel execution
- **REST API**: Provides endpoints for triggering loads and health checks
- **CLI**: Command-line interface for standalone execution

## Configuration

The service is configured via environment variables:

### Database Configuration
- `OLIST_DB_HOST`: Source database host (default: localhost)
- `OLIST_DB_USER`: Source database user (default: root)
- `OLIST_DB_NAME`: Source database name (default: olist_ecommerce)
- `OLIST_DB_PORT`: Source database port (default: 5432)
- `OLIST_DB_PASSWORD`: Source database password (optional)

- `DWH_DB_HOST`: Target database host (default: localhost)
- `DWH_DB_USER`: Target database user (default: root)
- `DWH_DB_NAME`: Target database name (default: example_project_1_dwh)
- `DWH_DB_PORT`: Target database port (default: 5432)
- `DWH_DB_PASSWORD`: Target database password (optional)

### Service Configuration
- `MAX_PARALLEL_TASKS`: Maximum parallel tasks (default: 5)
- `RETRY_ATTEMPTS`: Number of retry attempts (default: 3)
- `RETRY_DELAY`: Delay between retries in seconds (default: 1.0)
- `LOG_LEVEL`: Logging level (default: INFO)
- `EVENT_PUBLISHER_TYPE`: Event publisher type - "database" or "file" (default: database)

## Installation

### Using pip
```bash
cd marketing_data_service
pip install -r requirements.txt
```

### Using Docker
```bash
cd marketing_data_service
docker build -t marketing-data-service .
```

### Using Docker Compose
```bash
cd marketing_data_service
docker-compose up -d
```

## Usage

### Command Line Interface

Run a data load:
```bash
python -m marketing_data_service.cli load
```

Check service health:
```bash
python -m marketing_data_service.cli health
```

### REST API

Start the API server:
```bash
python -m marketing_data_service.main
```

The API will be available at `http://localhost:8000` with the following endpoints:

- `GET /`: Service information
- `GET /health`: Health check
- `POST /load`: Trigger synchronous data load
- `POST /load-async`: Trigger asynchronous data load

### Integration with Existing Mara Pipeline

The service provides a REST API that can be called from the existing Mara pipeline during the transition period:

```python
import requests

# Call the microservice from Mara pipeline
response = requests.post("http://localhost:8000/load")
if response.status_code == 200:
    result = response.json()
    print(f"Load completed: {result}")
else:
    print(f"Load failed: {response.text}")
```

## Data Flow

1. **Schema Initialization**: Creates `m_data` schema and required tables
2. **Parallel Data Loading**: Concurrently loads both marketing tables
3. **Data Validation**: Verifies record counts between source and target
4. **Event Publishing**: Publishes success/failure events with detailed results
5. **Monitoring**: Logs performance metrics and any issues

## Event Schema

### Success Event
```json
{
  "event_type": "marketing-data-loaded",
  "status": "success",
  "timestamp": "2025-08-26T21:19:33.123456",
  "results": {
    "total_tables": 2,
    "successful_tables": 2,
    "failed_tables": 0,
    "results": [
      {
        "table": "closed_deal",
        "status": "success",
        "records_loaded": 1500,
        "duration": 2.34
      },
      {
        "table": "marketing_qualified_lead",
        "status": "success",
        "records_loaded": 8000,
        "duration": 1.87
      }
    ],
    "integrity_checks": [
      {"table": "closed_deal", "integrity_ok": true},
      {"table": "marketing_qualified_lead", "integrity_ok": true}
    ]
  }
}
```

### Failure Event
```json
{
  "event_type": "marketing-data-load-failed",
  "status": "failure",
  "timestamp": "2025-08-26T21:19:33.123456",
  "error": "Connection timeout to source database",
  "details": {
    "total_tables": 2,
    "successful_tables": 1,
    "failed_tables": 1,
    "results": [...]
  }
}
```

## Monitoring

The service provides comprehensive monitoring through:

- **Structured logging**: All operations are logged with timestamps and context
- **Health checks**: Database connectivity and service status
- **Performance metrics**: Load times and record counts
- **Event publishing**: Success/failure events for external monitoring systems

## Error Handling

- **Retry logic**: Configurable retry attempts with exponential backoff
- **Transaction rollback**: Failed loads are rolled back to maintain consistency
- **Graceful degradation**: Individual table failures don't stop the entire process
- **Detailed error reporting**: Comprehensive error messages and context

## Testing

The service can be tested with the existing Mara database setup:

1. Ensure the `olist` and `dwh` databases are available
2. Run the health check to verify connectivity
3. Execute a test load to verify functionality
4. Check the event logs for proper event publishing

## Migration Strategy

1. **Phase 1**: Deploy microservice alongside existing Mara pipeline
2. **Phase 2**: Update Mara pipeline to call microservice API
3. **Phase 3**: Monitor and validate microservice performance
4. **Phase 4**: Remove original Mara pipeline code once stable

## Rollback Strategy

If issues occur, the service supports rollback through:

- **Transaction-based operations**: All changes are wrapped in transactions
- **Schema recreation**: The service can recreate schemas from scratch
- **Event tracking**: All operations are logged for audit purposes
- **API compatibility**: Existing Mara pipeline can be re-enabled quickly
