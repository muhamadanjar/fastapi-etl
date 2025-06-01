#!/bin/bash

# Start Celery Worker Script
# This script starts Celery workers for the FastAPI application

set -e

# Default values
WORKER_TYPE="default"
CONCURRENCY=4
LOG_LEVEL="INFO"
QUEUES=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--type)
            WORKER_TYPE="$2"
            shift 2
            ;;
        -c|--concurrency)
            CONCURRENCY="$2"
            shift 2
            ;;
        -l|--log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        -q|--queues)
            QUEUES="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  -t, --type TYPE        Worker type (default, priority, background, email, reports, data, notifications)"
            echo "  -c, --concurrency NUM  Number of worker processes (default: 4)"
            echo "  -l, --log-level LEVEL  Log level (default: INFO)"
            echo "  -q, --queues QUEUES    Comma-separated list of queues"
            echo "  -h, --help            Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option $1"
            exit 1
            ;;
    esac
done

# Set environment variables if not already set
export PYTHONPATH="${PYTHONPATH:-.}"

# Check if virtual environment is activated
if [[ -z "${VIRTUAL_ENV}" ]]; then
    echo "Warning: No virtual environment detected. Consider activating your venv."
fi

# Check if Celery is installed
if ! python -c "import celery" 2>/dev/null; then
    echo "Error: Celery is not installed. Install with: pip install celery"
    exit 1
fi

# Check if Redis is running (if using Redis broker)
if command -v redis-cli &> /dev/null; then
    if ! redis-cli ping &> /dev/null; then
        echo "Warning: Redis server is not responding. Make sure Redis is running."
    fi
fi

echo "Starting Celery worker..."
echo "Worker type: $WORKER_TYPE"
echo "Concurrency: $CONCURRENCY"
echo "Log level: $LOG_LEVEL"

# Build celery command
CELERY_CMD="celery -A app.infrastructure.tasks.celery_app worker"

# Add worker-specific options based on type
case $WORKER_TYPE in
    "default")
        CELERY_CMD="$CELERY_CMD --hostname=default_worker@%h"
        if [[ -z "$QUEUES" ]]; then
            QUEUES="celery,normal"
        fi
        ;;
    "priority")
        CELERY_CMD="$CELERY_CMD --hostname=priority_worker@%h"
        if [[ -z "$QUEUES" ]]; then
            QUEUES="critical,high"
        fi
        ;;
    "background")
        CELERY_CMD="$CELERY_CMD --hostname=background_worker@%h"
        if [[ -z "$QUEUES" ]]; then
            QUEUES="normal,low"
        fi
        ;;
    "email")
        CELERY_CMD="$CELERY_CMD --hostname=email_worker@%h"
        if [[ -z "$QUEUES" ]]; then
            QUEUES="email_tasks"
        fi
        ;;
    "reports")
        CELERY_CMD="$CELERY_CMD --hostname=reports_worker@%h"
        if [[ -z "$QUEUES" ]]; then
            QUEUES="report_tasks"
        fi
        CONCURRENCY=1  # CPU intensive, lower concurrency
        ;;
    "data")
        CELERY_CMD="$CELERY_CMD --hostname=data_worker@%h"
        if [[ -z "$QUEUES" ]]; then
            QUEUES="data_tasks"
        fi
        CONCURRENCY=1  # Memory intensive
        ;;
    "notifications")
        CELERY_CMD="$CELERY_CMD --hostname=notifications_worker@%h"
        if [[ -z "$QUEUES" ]]; then
            QUEUES="notification_tasks"
        fi
        ;;
    *)
        echo "Unknown worker type: $WORKER_TYPE"
        echo "Available types: default, priority, background, email, reports, data, notifications"
        exit 1
        ;;
esac

# Add common options
CELERY_CMD="$CELERY_CMD --loglevel=$LOG_LEVEL"
CELERY_CMD="$CELERY_CMD --concurrency=$CONCURRENCY"
CELERY_CMD="$CELERY_CMD --queues=$QUEUES"

# Add additional options for production
CELERY_CMD="$CELERY_CMD --max-tasks-per-child=1000"
CELERY_CMD="$CELERY_CMD --time-limit=7200"  # 2 hours
CELERY_CMD="$CELERY_CMD --soft-time-limit=6600"  # 1h 50m

echo "Queues: $QUEUES"
echo "Command: $CELERY_CMD"
echo ""

# Handle shutdown gracefully
trap 'echo "Shutting down worker..."; kill -TERM $PID; wait $PID' SIGTERM SIGINT

# Start the worker
exec $CELERY_CMD &
PID=$!

# Wait for the process
wait $PID