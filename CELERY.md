# Celery Worker - FastAPI ETL

Complete guide untuk menjalankan Celery worker dan background tasks.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Worker Commands](#worker-commands)
3. [Beat Scheduler](#beat-scheduler)
4. [Monitoring](#monitoring)
5. [Task Queues](#task-queues)
6. [Production Deployment](#production-deployment)
7. [Troubleshooting](#troubleshooting)

---

## Quick Start

### Prerequisites
```bash
# Install dependencies
pip install -r requirements.txt

# Redis must be running
redis-cli ping  # Should return: PONG
```

### Run Worker

**Basic Worker**:
```bash
celery -A app.worker worker --loglevel=info
```

**Worker with Beat Scheduler**:
```bash
celery -A app.worker worker --beat --loglevel=info
```

**Worker with Concurrency**:
```bash
celery -A app.worker worker --loglevel=info --concurrency=4
```

---

## Worker Commands

### Start Worker

```bash
# Development (single process)
celery -A app.worker worker --loglevel=debug --pool=solo

# Production (multi-process)
celery -A app.worker worker --loglevel=info --concurrency=4
```

### Worker Options

| Option | Description | Example |
|--------|-------------|---------|
| `--loglevel` | Log level | `debug`, `info`, `warning`, `error` |
| `--concurrency` | Number of worker processes | `4`, `8`, `16` |
| `--pool` | Worker pool type | `prefork`, `solo`, `gevent` |
| `--autoscale` | Auto-scale workers | `10,2` (max 10, min 2) |
| `-Q` | Specific queues | `etl,cleanup` |

### Examples

```bash
# Debug mode
celery -A app.worker worker --loglevel=debug

# High concurrency
celery -A app.worker worker --concurrency=8

# Auto-scaling
celery -A app.worker worker --autoscale=10,2

# Specific queues
celery -A app.worker worker -Q etl,cleanup
```

---

## Beat Scheduler

Beat scheduler menjalankan periodic tasks (cron-like).

### Run Beat

**With Worker**:
```bash
celery -A app.worker worker --beat --loglevel=info
```

**Separate Process** (Recommended for production):
```bash
# Terminal 1: Worker
celery -A app.worker worker --loglevel=info

# Terminal 2: Beat
celery -A app.worker beat --loglevel=info
```

### Scheduled Tasks

Configured in `app/worker.py`:

```python
celery_app.conf.beat_schedule = {
    'cleanup-old-files': {
        'task': 'cleanup.cleanup_old_files',
        'schedule': 3600.0,  # Every hour
    },
    'cleanup-old-executions': {
        'task': 'cleanup.cleanup_old_executions',
        'schedule': 86400.0,  # Every day
    },
    'monitor-job-health': {
        'task': 'monitoring.monitor_job_health',
        'schedule': 300.0,  # Every 5 minutes
    },
}
```

---

## Monitoring

### 1. Flower (Web UI)

**Start Flower**:
```bash
celery -A app.worker flower --port=5555
```

**Access**: http://localhost:5555

**Features**:
- Real-time task monitoring
- Worker status
- Task history
- Performance graphs
- Task retry/revoke

**With Authentication**:
```bash
celery -A app.worker flower --basic_auth=admin:password
```

### 2. CLI Monitoring

**Active Tasks**:
```bash
celery -A app.worker inspect active
```

**Registered Tasks**:
```bash
celery -A app.worker inspect registered
```

**Worker Stats**:
```bash
celery -A app.worker inspect stats
```

**Scheduled Tasks**:
```bash
celery -A app.worker inspect scheduled
```

**Reserved Tasks**:
```bash
celery -A app.worker inspect reserved
```

### 3. Task Status (Python)

```python
from app.tasks.celery_app import celery_app

# Check task status
result = celery_app.AsyncResult(task_id)
print(result.state)  # PENDING, STARTED, SUCCESS, FAILURE
print(result.result)  # Task result
print(result.traceback)  # If failed
```

---

## Task Queues

### Available Queues

| Queue | Priority | Purpose |
|-------|----------|---------|
| `etl` | 1 (High) | ETL processing tasks |
| `monitoring` | 2 | Monitoring tasks |
| `cleanup` | 3 | Cleanup tasks |
| `default` | 4 (Low) | Default queue |

### Run Workers for Specific Queues

```bash
# ETL worker only
celery -A app.worker worker -Q etl --loglevel=info --concurrency=4

# Cleanup worker only
celery -A app.worker worker -Q cleanup --loglevel=info --concurrency=2

# Multiple queues
celery -A app.worker worker -Q etl,cleanup --loglevel=info
```

### Task Routing

Tasks are automatically routed to queues:

```python
# Configured in celery_app.py
task_routes = {
    'app.tasks.etl_tasks.*': {'queue': 'etl'},
    'app.tasks.monitoring_tasks.*': {'queue': 'monitoring'},
    'app.tasks.cleanup_tasks.*': {'queue': 'cleanup'},
}
```

---

## Production Deployment

### 1. Systemd Service

**Worker Service**: `/etc/systemd/system/celery-worker.service`

```ini
[Unit]
Description=Celery Worker
After=network.target redis.target

[Service]
Type=forking
User=www-data
Group=www-data
WorkingDirectory=/path/to/fastapi-etl
Environment="PATH=/path/to/venv/bin"
ExecStart=/path/to/venv/bin/celery -A app.worker worker \
    --loglevel=info \
    --concurrency=4 \
    --pidfile=/var/run/celery/worker.pid \
    --logfile=/var/log/celery/worker.log

Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Beat Service**: `/etc/systemd/system/celery-beat.service`

```ini
[Unit]
Description=Celery Beat Scheduler
After=network.target redis.target

[Service]
Type=simple
User=www-data
Group=www-data
WorkingDirectory=/path/to/fastapi-etl
Environment="PATH=/path/to/venv/bin"
ExecStart=/path/to/venv/bin/celery -A app.worker beat \
    --loglevel=info \
    --pidfile=/var/run/celery/beat.pid \
    --logfile=/var/log/celery/beat.log

Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Enable & Start**:
```bash
# Create directories
sudo mkdir -p /var/run/celery /var/log/celery
sudo chown www-data:www-data /var/run/celery /var/log/celery

# Enable services
sudo systemctl enable celery-worker celery-beat

# Start services
sudo systemctl start celery-worker celery-beat

# Check status
sudo systemctl status celery-worker celery-beat

# View logs
sudo journalctl -u celery-worker -f
sudo journalctl -u celery-beat -f
```

### 2. Supervisor

**Config**: `/etc/supervisor/conf.d/celery.conf`

```ini
[program:celery-worker]
command=/path/to/venv/bin/celery -A app.worker worker --loglevel=info --concurrency=4
directory=/path/to/fastapi-etl
user=www-data
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/celery/worker.log

[program:celery-beat]
command=/path/to/venv/bin/celery -A app.worker beat --loglevel=info
directory=/path/to/fastapi-etl
user=www-data
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/celery/beat.log

[program:celery-flower]
command=/path/to/venv/bin/celery -A app.worker flower --port=5555
directory=/path/to/fastapi-etl
user=www-data
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/celery/flower.log
```

**Control**:
```bash
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl start celery-worker celery-beat celery-flower
sudo supervisorctl status
sudo supervisorctl restart celery-worker
```

### 3. Docker

**Dockerfile**:
```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["celery", "-A", "app.worker", "worker", "--loglevel=info"]
```

**docker-compose.yml**:
```yaml
services:
  worker:
    build: .
    command: celery -A app.worker worker --loglevel=info --concurrency=4
    environment:
      - CELERY_BROKER_URL=redis://redis:6379/0
      - CELERY_RESULT_BACKEND=redis://redis:6379/0
    depends_on:
      - redis
  
  beat:
    build: .
    command: celery -A app.worker beat --loglevel=info
    environment:
      - CELERY_BROKER_URL=redis://redis:6379/0
    depends_on:
      - redis
  
  flower:
    build: .
    command: celery -A app.worker flower --port=5555
    ports:
      - "5555:5555"
    environment:
      - CELERY_BROKER_URL=redis://redis:6379/0
    depends_on:
      - redis
```

---

## Configuration

### Environment Variables

```bash
# Redis (Broker & Backend)
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0

# Worker Settings
CELERYD_CONCURRENCY=4
CELERYD_PREFETCH_MULTIPLIER=1
CELERYD_MAX_TASKS_PER_CHILD=1000

# Task Settings
CELERY_TASK_TIME_LIMIT=3600        # 1 hour
CELERY_TASK_SOFT_TIME_LIMIT=3000   # 50 minutes
```

### Worker Settings

Configured in `app/tasks/celery_app.py`:

```python
celery_app.conf.update(
    # Task execution limits
    task_time_limit=3600,
    task_soft_time_limit=3000,
    worker_max_tasks_per_child=1000,
    
    # Worker settings
    worker_prefetch_multiplier=1,
    worker_max_memory_per_child=512000,  # 512MB
    
    # Task acknowledgment
    task_acks_late=True,
    task_reject_on_worker_lost=True,
)
```

---

## Troubleshooting

### Worker Not Starting

**Check Redis**:
```bash
redis-cli ping
# Should return: PONG
```

**Check Imports**:
```bash
python -c "from app.worker import celery_app; print(celery_app)"
```

**Check Tasks**:
```bash
celery -A app.worker inspect registered
```

**Check Logs**:
```bash
celery -A app.worker worker --loglevel=debug
```

### Tasks Not Executing

**Check Worker Status**:
```bash
celery -A app.worker inspect active
celery -A app.worker inspect stats
```

**Check Task State**:
```python
from app.tasks.celery_app import celery_app

result = celery_app.AsyncResult(task_id)
print(f"State: {result.state}")
print(f"Result: {result.result}")
if result.failed():
    print(f"Error: {result.traceback}")
```

**Purge Queue**:
```bash
# Clear all pending tasks
celery -A app.worker purge

# Confirm: yes
```

### High Memory Usage

**Set Memory Limit**:
```python
# In celery_app.py
worker_max_memory_per_child=512000  # 512MB
```

**Restart Workers Periodically**:
```python
# In celery_app.py
worker_max_tasks_per_child=1000  # Restart after 1000 tasks
```

**Monitor Memory**:
```bash
celery -A app.worker inspect stats | grep -i memory
```

### Slow Task Execution

**Check Concurrency**:
```bash
# Increase workers
celery -A app.worker worker --concurrency=8
```

**Check Queue Length**:
```bash
celery -A app.worker inspect reserved
```

**Check Task Time Limits**:
```python
# Increase limits if needed
task_time_limit=7200  # 2 hours
```

---

## Best Practices

### 1. Use Separate Workers for Different Queues

```bash
# ETL worker (high priority, more resources)
celery -A app.worker worker -Q etl --concurrency=4 --loglevel=info

# Cleanup worker (low priority, fewer resources)
celery -A app.worker worker -Q cleanup --concurrency=2 --loglevel=info
```

### 2. Set Appropriate Time Limits

```python
@celery_app.task(time_limit=3600, soft_time_limit=3500)
def long_running_task():
    # Task will be killed after 1 hour
    pass
```

### 3. Handle Task Failures

```python
@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def task_with_retry(self):
    try:
        # Task logic
        process_data()
    except Exception as exc:
        # Retry after 60 seconds
        raise self.retry(exc=exc, countdown=60)
```

### 4. Monitor Performance

```bash
# Use Flower for real-time monitoring
celery -A app.worker flower --port=5555

# Check stats regularly
celery -A app.worker inspect stats
```

### 5. Log Properly

```python
import logging
logger = logging.getLogger(__name__)

@celery_app.task
def my_task():
    logger.info("Task started")
    # Task logic
    logger.info("Task completed")
```

---

## Summary

**Development**:
```bash
# Run worker
celery -A app.worker worker --loglevel=debug

# Run with beat
celery -A app.worker worker --beat --loglevel=debug

# Run flower
celery -A app.worker flower
```

**Production**:
```bash
# Use systemd or supervisor
sudo systemctl start celery-worker celery-beat

# Monitor with Flower
celery -A app.worker flower --port=5555 --basic_auth=admin:password
```

**Monitoring**:
- Flower UI: http://localhost:5555
- CLI: `celery -A app.worker inspect`
- Logs: `/var/log/celery/`

**Structure**:
```
app/
├── worker.py           # Celery entry point
├── tasks/
│   ├── celery_app.py   # Celery configuration
│   ├── etl_tasks.py    # ETL tasks
│   ├── cleanup_tasks.py # Cleanup tasks
│   └── monitoring_tasks.py # Monitoring tasks
```

---

## Support

For issues:
- Check logs: `celery -A app.worker worker --loglevel=debug`
- Inspect workers: `celery -A app.worker inspect stats`
- Monitor with Flower: http://localhost:5555
