#!/bin/bash

echo "Starting Marketing Lead Processing Microservices..."

if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
fi

echo "Starting Redis server..."
redis-server --daemonize yes --appendonly yes

echo "Waiting for Redis to start..."
sleep 2

echo "Starting Data Loader Service..."
cd data_loader
python service.py &
DATA_LOADER_PID=$!
cd ..

echo "Starting Lead Processor Service..."
cd lead_processor
python service.py &
LEAD_PROCESSOR_PID=$!
cd ..

echo "Services started successfully!"
echo "Data Loader PID: $DATA_LOADER_PID"
echo "Lead Processor PID: $LEAD_PROCESSOR_PID"

echo "Press Ctrl+C to stop all services..."

cleanup() {
    echo "Stopping services..."
    kill $DATA_LOADER_PID $LEAD_PROCESSOR_PID 2>/dev/null
    redis-cli shutdown
    exit 0
}

trap cleanup SIGINT SIGTERM

wait
