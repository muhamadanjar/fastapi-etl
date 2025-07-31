#!/bin/bash

# Start Celery Beat Scheduler Script
# This script starts the Celery beat scheduler for periodic tasks

set -e

# Default values
LOG_LEVEL="INFO"
SCHEDULE_FILE="celerybeat-schedule"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -l|--log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        -s|--schedule)
            SCHEDULE_FILE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  -l, --log-level LEVEL  Log level (default: INFO)"
            echo "  -s, --schedule FILE    Schedule database file (default: celerybeat-schedule)"
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

echo "Starting Celery beat scheduler..."
echo "Log level: $LOG_LEVEL"
echo "Schedule file: $SCHEDULE_FILE"

# Build celery beat command
CELERY_CMD="celery -A app.tasks.celery_app beat"
CELERY_CMD="$CELERY_CMD --loglevel=$LOG_LEVEL"
CELERY_CMD="$CELERY_CMD --schedule=$SCHEDULE_FILE"
CELERY_CMD="$CELERY_CMD --pidfile=celerybeat.pid"

echo "Command: $CELERY_CMD"
echo ""

# Handle shutdown gracefully
trap 'echo "Shutting down beat scheduler..."; kill -TERM $PID; wait $PID' SIGTERM SIGINT

# Start the beat scheduler
exec $CELERY_CMD &
PID=$!

# Wait for the process
wait $PID