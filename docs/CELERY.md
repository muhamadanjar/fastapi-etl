# Celery Worker - FastAPI ETL

Complete guide untuk menjalankan Celery worker dan background tasks.

> **💡 Gunakan `python manage.py`** untuk command yang lebih mudah. Semua command celery bisa diakses via `python manage.py worker <subcommand>` dan `python manage.py task <subcommand>`. Lihat [CLI_GUIDE.md](./CLI_GUIDE.md) untuk panduan lengkap.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Worker Commands (manage.py)](#worker-commands-managepy)
3. [Worker Commands (Raw Celery)](#worker-commands-raw-celery)
4. [Beat Scheduler](#beat-scheduler)
5. [Task Management (manage.py)](#task-management-managepy)
6. [Monitoring](#monitoring)
7. [Task Queues](#task-queues)
8. [Production Deployment](#production-deployment)
9. [Troubleshooting](#troubleshooting)

---

## Quick Start

### Prerequisites
```bash
# Install dependencies
pip install -r requirements.txt

# Redis must be running
redis-cli ping  # Should return: PONG
```

### Using manage.py (Recommended)

```bash
# Start all workers
python manage.py worker start

# Start specific worker type
python manage.py worker start --worker-type email

# Start with beat
python manage.py worker start
python manage.py worker beat
```

### Using Raw Celery

**Basic Worker**:
```bash
celery -A app.tasks.celery_app worker --loglevel=info
```

**Worker with Beat Scheduler**:
```bash
celery -A app.tasks.celery_app worker --beat --loglevel=info
```

**Worker with Concurrency**:
```bash
celery -A app.tasks.celery_app worker --loglevel=info --concurrency=4
```

---

## Worker Commands (manage.py)

```bash
# Start workers
python manage.py worker start                         # All workers
python manage.py worker start -t email                # Email worker only
python manage.py worker start -t default -d           # Background
python manage.py worker start --dry-run               # Preview

# Stop workers
python manage.py worker stop --all-workers
python manage.py worker stop -n celery@default

# Restart
python manage.py worker restart -t email

# Status
python manage.py worker status
python manage.py worker status --format json

# Scale
python manage.py worker scale -t default -c 8

# Queues
python manage.py worker queues
python manage.py worker purge -q etl
python manage.py worker purge -q default --force

# Beat scheduler
python manage.py worker beat
python manage.py worker beat --detach

# Generate configs
python manage.py worker systemd -t default -o /etc/systemd/system/etl.service
python manage.py worker docker-compose
```

---

## Task Management (manage.py)

```bash
# List recent tasks
python manage.py task list
python manage.py task list --limit 50
python manage.py task list --status FAILURE
python manage.py task list --format json

# Task detail
python manage.py task show <task-id>
python manage.py task show <task-id> --format json

# Cancel task
python manage.py task cancel <task-id>
python manage.py task cancel <task-id> --force

# Statistics
python manage.py task stats
python manage.py task stats --format json
```

---

## Worker Commands (Raw Celery)

### Start Worker

```bash
# Development (single process)
celery -A app.tasks.celery_app worker --loglevel=debug --pool=solo

# Production (multi-process)
celery -A app.tasks.celery_app worker --loglevel=info --concurrency=4
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
celery -A app.tasks.celery_app worker --loglevel=debug

# High concurrency
celery -A app.tasks.celery_app worker --concurrency=8

# Auto-scaling
celery -A app.tasks.celery_app worker --autoscale=10,2

# Specific queues
celery -A app.tasks.celery_app worker -Q etl,cleanup
```

---

## Beat Scheduler

Beat scheduler menjalankan periodic tasks (cron-like).

### Using manage.py
```bash
python manage.py worker beat
python manage.py worker beat --detach
```

### Raw Celery

**With Worker**:
```bash
celery -A app.tasks.celery_app worker --beat --loglevel=info
```

**Separate Process** (Recommended for production):
```bash
# Terminal 1: Worker
celery -A app.tasks.celery_app worker --loglevel=info

# Terminal 2: Beat
celery -A app.tasks.celery_app beat --loglevel=info
```

### Scheduled Tasks

Configured in `app/tasks/celery_app.py`:

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

### Using manage.py
```bash
# Flower dashboard
python manage.py flower
python manage.py flower --port 6666

# Worker monitoring
python manage.py worker status
python manage.py worker queues

# Task monitoring
python manage.py task list
python manage.py task stats
```

### 1. Flower (Web UI)

**Start Flower**:
```bash
# Via manage.py
python manage.py flower
python manage.py worker flower --port 5555

# Via raw celery
celery -A app.tasks.celery_app flower --port=5555
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
celery -A app.tasks.celery_app flower --basic_auth=admin:password
```

### 2. CLI Monitoring

**Active Tasks**:
```bash
celery -A app.tasks.celery_app inspect active
```

**Registered Tasks**:
```bash
celery -A app.tasks.celery_app inspect registered
```

**Worker Stats**:
```bash
celery -A app.tasks.celery_app inspect stats
```

**Scheduled Tasks**:
```bash
celery -A app.tasks.celery_app inspect scheduled
```

**Reserved Tasks**:
```bash
celery -A app.tasks.celery_app inspect reserved
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
celery -A app.tasks.celery_app worker -Q etl --loglevel=info --concurrency=4

# Cleanup worker only
celery -A app.tasks.celery_app worker -Q cleanup --loglevel=info --concurrency=2

# Multiple queues
celery -A app.tasks.celery_app worker -Q etl,cleanup --loglevel=info
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
ExecStart=/path/to/venv/bin/celery -A app.tasks.celery_app worker \
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
ExecStart=/path/to/venv/bin/celery -A app.tasks.celery_app beat \
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
command=/path/to/venv/bin/celery -A app.tasks.celery_app worker --loglevel=info --concurrency=4
directory=/path/to/fastapi-etl
user=www-data
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/celery/worker.log

[program:celery-beat]
command=/path/to/venv/bin/celery -A app.tasks.celery_app beat --loglevel=info
directory=/path/to/fastapi-etl
user=www-data
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/celery/beat.log

[program:celery-flower]
command=/path/to/venv/bin/celery -A app.tasks.celery_app flower --port=5555
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

CMD ["celery", "-A", "app.tasks.celery_app", "worker", "--loglevel=info"]
```

**docker-compose.yml**:
```yaml
services:
  worker:
    build: .
    command: celery -A app.tasks.celery_app worker --loglevel=info --concurrency=4
    environment:
      - CELERY_BROKER_URL=redis://redis:6379/0
      - CELERY_RESULT_BACKEND=redis://redis:6379/0
    depends_on:
      - redis
  
  beat:
    build: .
    command: celery -A app.tasks.celery_app beat --loglevel=info
    environment:
      - CELERY_BROKER_URL=redis://redis:6379/0
    depends_on:
      - redis
  
  flower:
    build: .
    command: celery -A app.tasks.celery_app flower --port=5555
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
python -c "from app.tasks.celery_app import celery_app; print(celery_app)"
```

**Check Tasks**:
```bash
celery -A app.tasks.celery_app inspect registered
```

**Check Logs**:
```bash
celery -A app.tasks.celery_app worker --loglevel=debug
```

### Tasks Not Executing

**Check Worker Status**:
```bash
celery -A app.tasks.celery_app inspect active
celery -A app.tasks.celery_app inspect stats
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
celery -A app.tasks.celery_app purge

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
celery -A app.tasks.celery_app inspect stats | grep -i memory
```

### Slow Task Execution

**Check Concurrency**:
```bash
# Increase workers
celery -A app.tasks.celery_app worker --concurrency=8
```

**Check Queue Length**:
```bash
celery -A app.tasks.celery_app inspect reserved
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
celery -A app.tasks.celery_app worker -Q etl --concurrency=4 --loglevel=info

# Cleanup worker (low priority, fewer resources)
celery -A app.tasks.celery_app worker -Q cleanup --concurrency=2 --loglevel=info
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
celery -A app.tasks.celery_app flower --port=5555

# Check stats regularly
celery -A app.tasks.celery_app inspect stats
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

**Development (manage.py — Recommended)**:
```bash
python manage.py worker start           # Run all workers
python manage.py worker beat            # Run beat
python manage.py flower                 # Run flower
python manage.py task list              # Monitor tasks
```

**Development (raw celery)**:
```bash
celery -A app.tasks.celery_app worker --loglevel=debug
celery -A app.tasks.celery_app worker --beat --loglevel=debug
celery -A app.tasks.celery_app flower
```

**Production**:
```bash
# Generate systemd config
python manage.py worker systemd -t default
python manage.py worker systemd -t email --output /etc/systemd/system/etl-email.service

# Generate docker-compose
python manage.py worker docker-compose

# Monitoring
python manage.py flower --port 5555
```

**Monitoring**:
- Flower UI: http://localhost:5555
- CLI: `python manage.py worker status`, `python manage.py task list`
- Raw CLI: `celery -A app.tasks.celery_app inspect`
- Logs: `/var/log/celery/`

**Structure**:
```
etl_api/
├── manage.py              # CLI entry point
├── commands/
│   ├── worker.py          # Worker management (typer)
│   └── task.py            # Task management (typer)
├── app/
│   └── tasks/
│       ├── celery_app.py  # Celery configuration
│       ├── etl_tasks.py   # ETL tasks
│       ├── cleanup_tasks.py
│       └── monitoring_tasks.py
```

---

## Support

For issues:
- CLI: `python manage.py --help`, `python manage.py worker --help`, `python manage.py task --help`
- Check logs: `celery -A app.tasks.celery_app worker --loglevel=debug`
- Inspect workers: `python manage.py worker status`
- Monitor with Flower: http://localhost:5555
