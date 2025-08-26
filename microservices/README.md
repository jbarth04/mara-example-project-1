# Marketing Lead Processing Microservices

This directory contains the microservice implementation for marketing lead processing, extracted from the monolithic Mara pipeline.

## Architecture

The microservice architecture consists of:

1. **Data Loader Service** (`data_loader/`): Loads marketing data from the `olist` database into staging tables
2. **Lead Processor Service** (`lead_processor/`): Processes leads using the existing transformation logic
3. **Event Infrastructure** (`common/events.py`): Redis-based pub/sub messaging for service communication
4. **Database Utilities** (`common/database.py`): Shared database connection and SQL execution utilities

## Services

### Data Loader Service

- Loads data from `olist.marketing.closed_deals` and `olist.marketing.marketing_qualified_leads`
- Creates and populates `m_data.marketing_qualified_lead` and `m_data.closed_deal` tables
- Publishes "new_marketing_data_available" events when loading completes
- Runs on a scheduled interval (configurable, default 5 minutes)

### Lead Processor Service

- Listens for "new_marketing_data_available" events
- Executes preprocessing logic (joins marketing qualified leads with closed deals)
- Executes transformation logic (creates final dimensional table with seller data)
- Handles cross-domain dependency by joining with `ec_dim.seller` table
- Publishes "lead_processed" events when transformation completes

## Event Flow

```
Data Loader Service → "new_marketing_data_available" → Lead Processor Service → "lead_processed"
```

## Database Dependencies

The services require access to:

- **olist database**: Source data for marketing qualified leads and closed deals
- **dwh database**: Target data warehouse with staging and dimensional tables
- **ec_dim.seller table**: Cross-domain dependency for seller lifetime metrics

## Setup and Running

### Prerequisites

- PostgreSQL with `olist_ecommerce` and `example_project_1_dwh` databases
- Redis server
- Python 3.9+

### Local Development

1. Copy environment configuration:
   ```bash
   cp .env.example .env
   ```

2. Update `.env` with your database credentials

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Start services locally:
   ```bash
   ./start_local.sh
   ```

### Docker Deployment

1. Build and start all services:
   ```bash
   docker-compose up --build
   ```

2. Services will be available:
   - Redis: `localhost:6379`
   - Data Loader: runs automatically
   - Lead Processor: listens for events

## Configuration

Environment variables (see `.env.example`):

- `DWH_DB_*`: Data warehouse database connection
- `OLIST_DB_*`: Source database connection  
- `REDIS_*`: Redis connection settings

## Testing

To test the microservice flow:

1. Start all services
2. Check logs for data loading completion
3. Verify `m_data` tables are populated
4. Check logs for lead processing completion
5. Verify `m_dim.lead` table contains processed data with seller metrics

## Cross-Domain Dependency

The lead processor handles the cross-domain dependency with the seller domain by:

- Joining processed leads with `ec_dim.seller` table
- Retrieving `number_of_orders_lifetime` and `revenue_lifetime` metrics
- This maintains the existing business logic while keeping services decoupled

In future iterations, this dependency can be replaced with:
- API calls to a seller service
- Event-driven data synchronization
- Cached seller data with periodic updates

## Monitoring

Services log key events:
- Data loading start/completion
- Event publishing/consumption
- Processing errors and retries
- Database connection issues

## Migration Notes

This microservice implementation:
- Preserves existing SQL transformation logic
- Maintains data consistency with the monolithic pipeline
- Uses the same database schemas and table structures
- Handles the seller cross-domain dependency through direct database access
- Can run alongside the existing Mara pipeline during migration
