"""
Worker management commands — start, stop, restart, scale, and monitor Celery workers.
"""

from commands.base import BaseCommand
import typer
import json
from typing import Optional

# Top-level group commands handled by manage.py's typer groups
# Each "subcommand" becomes a separate Command class registered via manage.py


class WorkerStartCommand(BaseCommand):
    """Start Celery workers"""

    help = "Start Celery workers"

    def add_arguments(self):
        return {
            'worker_type': typer.Option(
                'all', '--worker-type', '-t',
                help='Type of worker to start: default, email, data_sync, priority, all'
            ),
            'detach': typer.Option(
                False, '--detach', '-d',
                help='Run worker in background'
            ),
            'dry_run': typer.Option(
                False, '--dry-run',
                help='Show command without executing'
            ),
        }

    def handle(self, worker_type: str, detach: bool, dry_run: bool, **options):
        self.print_header("Start Workers")

        valid_types = ['default', 'email', 'data_sync', 'priority', 'all']
        if worker_type not in valid_types:
            self.error(f"Invalid worker type: {worker_type}. Valid: {', '.join(valid_types)}")
            raise typer.Exit(1)

        self.info(f"Starting {worker_type} worker(s)...")

        if dry_run:
            self.warning("DRY RUN — No workers will be started")
            self.info(f"Would start: {worker_type}")
            return

        try:
            from app.tasks import celery_app

            if worker_type == 'all':
                for wt in ['default', 'email', 'data_sync', 'priority']:
                    self._start_single_worker(celery_app, wt, detach)
            else:
                self._start_single_worker(celery_app, worker_type, detach)

        except ImportError:
            self.warning("Celery not configured. Cannot start workers.")
            self.info("Ensure app/tasks.py has a celery_app instance.")
        except Exception as e:
            self.error(f"Failed to start workers: {e}")

    def _start_single_worker(self, celery_app, worker_type: str, detach: bool):
        argv = ['worker', '--loglevel=info', f'--queues={worker_type}']
        if detach:
            argv.append('--detach')
        try:
            celery_app.worker_main(argv)
            self.success(f"Started {worker_type} worker")
        except Exception as e:
            self.error(f"Failed to start {worker_type} worker: {e}")


class WorkerStopCommand(BaseCommand):
    """Stop Celery workers"""

    help = "Stop Celery workers"

    def add_arguments(self):
        return {
            'worker_name': typer.Option(
                None, '--worker-name', '-n',
                help='Specific worker name to stop'
            ),
            'all_workers': typer.Option(
                False, '--all-workers', '-a',
                help='Stop all workers'
            ),
        }

    def handle(self, worker_name: Optional[str], all_workers: bool, **options):
        self.print_header("Stop Workers")

        if not worker_name and not all_workers:
            self.error("Please specify --worker-name or --all-workers")
            raise typer.Exit(1)

        try:
            from app.tasks import celery_app

            if all_workers:
                self.info("Stopping all workers...")
                celery_app.control.broadcast('shutdown')
                self.success("Sent shutdown signal to all workers")
            else:
                self.info(f"Stopping worker: {worker_name}")
                celery_app.control.broadcast('shutdown', destination=[worker_name])
                self.success(f"Sent shutdown signal to {worker_name}")

        except ImportError:
            self.warning("Celery not configured.")
        except Exception as e:
            self.error(f"Failed to stop workers: {e}")


class WorkerRestartCommand(BaseCommand):
    """Restart a Celery worker"""

    help = "Restart a Celery worker"

    def add_arguments(self):
        return {
            'worker_type': typer.Option(
                ..., '--worker-type', '-t',
                help='Type of worker to restart: default, email, data_sync, priority'
            ),
        }

    def handle(self, worker_type: str, **options):
        self.print_header("Restart Worker")

        valid_types = ['default', 'email', 'data_sync', 'priority']
        if worker_type not in valid_types:
            self.error(f"Invalid worker type. Valid: {', '.join(valid_types)}")
            raise typer.Exit(1)

        self.info(f"Restarting {worker_type} worker...")

        try:
            from app.tasks import celery_app
            celery_app.control.broadcast('pool_restart', destination=[f'celery@{worker_type}'])
            self.success(f"Restarted {worker_type} worker")
        except ImportError:
            self.warning("Celery not configured.")
        except Exception as e:
            self.error(f"Failed to restart worker: {e}")


class WorkerStatusCommand(BaseCommand):
    """Show worker status"""

    help = "Show worker status"

    def add_arguments(self):
        return {
            'output_format': typer.Option(
                'table', '--format', '-f',
                help='Output format: json or table'
            ),
        }

    def handle(self, output_format: str, **options):
        self.print_header("Worker Status")

        try:
            from app.tasks import celery_app

            inspect = celery_app.control.inspect()
            ping = inspect.ping() or {}
            active = inspect.active() or {}
            stats = inspect.stats() or {}

            if output_format == 'json':
                result = {
                    'ping': ping,
                    'active_tasks': active,
                    'stats': {k: {'total': v.get('total', {})} for k, v in stats.items()},
                }
                self.print(json.dumps(result, indent=2, default=str))
                return

            # Table format
            self.print("")
            self.print("Worker Ping Results:", style="bold")
            if ping:
                for worker, response in ping.items():
                    status = "✅ Online" if response.get('ok') == 'pong' else "❌ Offline"
                    self.print(f"  {worker}: {status}")
            else:
                self.warning("  No workers responding to ping")

            self.print("")
            self.print("Active Tasks:", style="bold")
            if active:
                for worker, tasks in active.items():
                    self.print(f"  {worker}: {len(tasks)} tasks")
                    for task in tasks[:3]:
                        task_name = task.get('name', 'Unknown')
                        task_id = task.get('id', 'Unknown')[:8]
                        self.print(f"    — {task_name} ({task_id})")
                    if len(tasks) > 3:
                        self.print(f"    ... and {len(tasks) - 3} more")
            else:
                self.info("  No active tasks")

        except ImportError:
            self.warning("Celery not configured. Cannot check worker status.")
        except Exception as e:
            self.error(f"Failed to get worker status: {e}")


class WorkerQueuesCommand(BaseCommand):
    """Show queue information"""

    help = "Show queue information"

    def add_arguments(self):
        return {
            'output_format': typer.Option(
                'table', '--format', '-f',
                help='Output format: json or table'
            ),
        }

    def handle(self, output_format: str, **options):
        self.print_header("Queue Information")

        try:
            from app.tasks import celery_app

            inspect = celery_app.control.inspect()
            active_queues = inspect.active_queues() or {}

            if output_format == 'json':
                self.print(json.dumps(active_queues, indent=2, default=str))
                return

            for worker, queues in active_queues.items():
                self.print(f"  {worker}:", style="bold")
                for q in queues:
                    self.print(f"    — {q['name']} ({q.get('messages', '?')} messages)")

        except ImportError:
            self.warning("Celery not configured.")
        except Exception as e:
            self.error(f"Failed to get queue info: {e}")


class WorkerPurgeCommand(BaseCommand):
    """Purge all messages from a queue"""

    help = "Purge all messages from a queue"

    def add_arguments(self):
        return {
            'queue': typer.Option(
                ..., '--queue', '-q',
                help='Queue name to purge'
            ),
            'force': typer.Option(
                False, '--force', '-f',
                help='Skip confirmation prompt'
            ),
        }

    def handle(self, queue: str, force: bool, **options):
        self.print_header(f"Purge Queue: {queue}")

        if not force:
            if not self.confirm(f"Are you sure you want to purge the '{queue}' queue?"):
                self.warning("Operation cancelled")
                return

        try:
            from app.tasks import celery_app

            purged = celery_app.control.purge()
            self.success(f"Purged {purged} messages from all queues")
        except ImportError:
            self.warning("Celery not configured.")
        except Exception as e:
            self.error(f"Failed to purge queue: {e}")


class WorkerBeatCommand(BaseCommand):
    """Start Celery Beat scheduler"""

    help = "Start Celery Beat scheduler"

    def add_arguments(self):
        return {
            'detach': typer.Option(
                False, '--detach', '-d',
                help='Run beat in background'
            ),
            'dry_run': typer.Option(
                False, '--dry-run',
                help='Show command without executing'
            ),
        }

    def handle(self, detach: bool, dry_run: bool, **options):
        self.print_header("Celery Beat")

        if dry_run:
            self.warning("DRY RUN — Beat scheduler will not be started")
            return

        self.info("Starting Celery Beat scheduler...")

        try:
            from app.tasks import celery_app
            argv = ['beat', '--loglevel=info']
            if detach:
                argv.append('--detach')
            celery_app.start(argv)
            self.success("Beat scheduler started")
        except ImportError:
            self.warning("Celery not configured.")
        except Exception as e:
            self.error(f"Failed to start beat: {e}")


class WorkerScaleCommand(BaseCommand):
    """Scale worker concurrency"""

    help = "Scale worker concurrency"

    def add_arguments(self):
        return {
            'worker_type': typer.Option(
                ..., '--worker-type', '-t',
                help='Type of worker to scale'
            ),
            'concurrency': typer.Option(
                ..., '--concurrency', '-c',
                help='Number of concurrent processes'
            ),
        }

    def handle(self, worker_type: str, concurrency: int, **options):
        self.print_header("Scale Worker")
        self.info(f"Scaling {worker_type} worker to {concurrency} processes...")

        try:
            from app.tasks import celery_app
            celery_app.control.pool_grow(n=concurrency, destination=[f'celery@{worker_type}'])
            self.success(f"Scaled {worker_type} worker to {concurrency}")
        except ImportError:
            self.warning("Celery not configured.")
        except Exception as e:
            self.error(f"Failed to scale worker: {e}")


class WorkerFlowerCommand(BaseCommand):
    """Start Flower monitoring tool"""

    help = "Start Flower monitoring tool"

    def add_arguments(self):
        return {
            'port': typer.Option(
                5555, '--port', '-p',
                help='Port number for Flower'
            ),
            'detach': typer.Option(
                False, '--detach', '-d',
                help='Run Flower in background'
            ),
        }

    def handle(self, port: int, detach: bool, **options):
        self.print_header("Flower Monitor")
        self.info(f"Starting Flower on port {port}...")

        try:
            import subprocess
            cmd = ['celery', '-A', 'app.tasks', 'flower', f'--port={port}']
            if detach:
                subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                self.info("Starting Flower in foreground (Ctrl+C to stop)...")
                subprocess.run(cmd)

            self.success(f"Flower started")
            if not detach:
                self.info(f"Flower UI available at: http://localhost:{port}")
        except FileNotFoundError:
            self.warning("Celery CLI not found. Install with: pip install celery[flower]")
        except Exception as e:
            self.error(f"Failed to start Flower: {e}")


class WorkerSystemdCommand(BaseCommand):
    """Generate systemd service file for worker"""

    help = "Generate systemd service file for worker"

    def add_arguments(self):
        return {
            'worker_type': typer.Option(
                ..., '--worker-type', '-t',
                help='Worker type for systemd service'
            ),
            'output': typer.Option(
                None, '--output', '-o',
                help='Output file path'
            ),
        }

    def handle(self, worker_type: str, output: Optional[str], **options):
        self.print_header("Generate Systemd Service")

        import os
        from pathlib import Path

        project_dir = Path(__file__).parent.parent
        python_path = os.sys.executable

        service_content = f"""[Unit]
Description=ETL API Celery Worker ({worker_type})
After=network.target redis.service

[Service]
Type=forking
User=www-data
Group=www-data
WorkingDirectory={project_dir}
Environment="PATH={os.environ.get('PATH', '/usr/local/bin:/usr/bin:/bin')}"
ExecStart={python_path} -m celery -A app.tasks worker --queues={worker_type} --loglevel=info
ExecStop=/bin/kill -TERM $MAINPID
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
"""

        if output:
            with open(output, 'w') as f:
                f.write(service_content)
            self.success(f"Systemd service file written to: {output}")
        else:
            self.print(service_content)
            self.info("Use --output to write to a file")


class WorkerDockerComposeCommand(BaseCommand):
    """Generate docker-compose.yml for workers"""

    help = "Generate docker-compose.yml for workers"

    def add_arguments(self):
        return {
            'output': typer.Option(
                'docker-compose.workers.yml', '--output', '-o',
                help='Output file path'
            ),
        }

    def handle(self, output: str, **options):
        self.print_header("Generate Docker Compose")

        compose_content = """version: '3.8'

services:
  worker-default:
    build: .
    command: celery -A app.tasks worker --queues=default --loglevel=info
    depends_on:
      - redis
    restart: unless-stopped

  worker-email:
    build: .
    command: celery -A app.tasks worker --queues=email --loglevel=info
    depends_on:
      - redis
    restart: unless-stopped

  worker-data-sync:
    build: .
    command: celery -A app.tasks worker --queues=data_sync --loglevel=info
    depends_on:
      - redis
    restart: unless-stopped

  worker-priority:
    build: .
    command: celery -A app.tasks worker --queues=priority --loglevel=info
    depends_on:
      - redis
    restart: unless-stopped

  beat:
    build: .
    command: celery -A app.tasks beat --loglevel=info
    depends_on:
      - redis
    restart: unless-stopped

  flower:
    build: .
    command: celery -A app.tasks flower --port=5555
    ports:
      - "5555:5555"
    depends_on:
      - redis
    restart: unless-stopped
"""

        with open(output, 'w') as f:
            f.write(compose_content)

        self.success(f"Docker Compose file written to: {output}")
        self.info("To start workers with Docker Compose:")
        self.print(f"  docker-compose -f {output} up -d")
