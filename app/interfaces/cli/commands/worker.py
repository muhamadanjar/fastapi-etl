"""
CLI commands untuk mengelola Celery workers.
"""
import click
import json
from typing import Optional
from app.interfaces.background.worker_service import WorkerService
from app.interfaces.background.task_manager import TaskManager

worker_service = WorkerService()
task_manager = TaskManager()

@click.group()
def worker():
    """Celery worker management commands."""
    pass

@worker.command()
@click.option('--worker-type', '-t', 
              type=click.Choice(['default', 'email', 'data_sync', 'priority', 'all']),
              default='all',
              help='Type of worker to start')
@click.option('--detach', '-d', is_flag=True, help='Run worker in background')
@click.option('--dry-run', is_flag=True, help='Show command without executing')
def start(worker_type: str, detach: bool, dry_run: bool):
    """Start Celery workers."""
    click.echo(f"Starting {worker_type} worker(s)...")
    
    if worker_type == 'all':
        results = worker_service.start_all_workers(detach=detach)
        for wtype, success in results.items():
            status = "✓" if success else "✗"
            click.echo(f"{status} {wtype} worker: {'started' if success else 'failed'}")
    else:
        success = worker_service.start_worker(worker_type, detach=detach, dry_run=dry_run)
        status = "✓" if success else "✗"
        click.echo(f"{status} {worker_type} worker: {'started' if success else 'failed'}")

@worker.command()
@click.option('--worker-name', '-n', help='Specific worker name to stop')
@click.option('--all-workers', '-a', is_flag=True, help='Stop all workers')
def stop(worker_name: Optional[str], all_workers: bool):
    """Stop Celery workers."""
    if all_workers:
        click.echo("Stopping all workers...")
        success = worker_service.stop_all_workers()
        status = "✓" if success else "✗"
        click.echo(f"{status} All workers: {'stopped' if success else 'failed to stop'}")
    elif worker_name:
        click.echo(f"Stopping worker: {worker_name}")
        success = worker_service.stop_worker(worker_name)
        status = "✓" if success else "✗"
        click.echo(f"{status} {worker_name}: {'stopped' if success else 'failed to stop'}")
    else:
        click.echo("Please specify --worker-name or --all-workers")

@worker.command()
@click.option('--worker-type', '-t',
              type=click.Choice(['default', 'email', 'data_sync', 'priority']),
              required=True,
              help='Type of worker to restart')
def restart(worker_type: str):
    """Restart a Celery worker."""
    click.echo(f"Restarting {worker_type} worker...")
    success = worker_service.restart_worker(worker_type)
    status = "✓" if success else "✗"
    click.echo(f"{status} {worker_type} worker: {'restarted' if success else 'failed to restart'}")

@worker.command()
@click.option('--format', '-f', type=click.Choice(['json', 'table']), default='table',
              help='Output format')
def status(format: str):
    """Show worker status."""
    click.echo("Getting worker status...")
    
    status_info = worker_service.get_worker_status()
    
    if format == 'json':
        click.echo(json.dumps(status_info, indent=2, default=str))
    else:
        # Table format
        click.echo("\n=== Worker Status ===")
        
        # Ping results
        ping_info = status_info.get('ping', {})
        if ping_info:
            click.echo("\nWorker Ping Results:")
            for worker, response in ping_info.items():
                status = "✓ Online" if response.get('ok') == 'pong' else "✗ Offline"
                click.echo(f"  {worker}: {status}")
        else:
            click.echo("  No workers responding to ping")
        
        # Active tasks
        active_info = status_info.get('active_tasks', {})
        if active_info:
            click.echo("\nActive Tasks:")
            for worker, tasks in active_info.items():
                click.echo(f"  {worker}: {len(tasks)} tasks")
                for task in tasks[:3]:  # Show first 3 tasks
                    task_name = task.get('name', 'Unknown')
                    task_id = task.get('id', 'Unknown')[:8]
                    click.echo(f"    - {task_name} ({task_id})")
                if len(tasks) > 3:
                    click.echo(f"    ... and {len(tasks) - 3} more")
        else:
            click.echo("\nNo active tasks")

@worker.command()
@click.option('--worker-type', '-t',
              type=click.Choice(['default', 'email', 'data_sync', 'priority']),
              required=True,
              help='Type of worker to scale')
@click.option('--concurrency', '-c', type=int, required=True,
              help='Number of concurrent processes')
def scale(worker_type: str, concurrency: int):
    """Scale worker concurrency."""
    click.echo(f"Scaling {worker_type} worker to {concurrency} processes...")
    success = worker_service.scale_worker(worker_type, concurrency)
    status = "✓" if success else "✗"
    click.echo(f"{status} {worker_type} worker: {'scaled' if success else 'failed to scale'}")

@worker.command()
@click.option('--format', '-f', type=click.Choice(['json', 'table']), default='table',
              help='Output format')
def queues(format: str):
    """Show queue information."""
    click.echo("Getting queue information...")
    
    queue_info = worker_service.get_queue_info()
    
    if format == 'json':
        click.echo(json.dumps(queue_info, indent=2))
    else:
        click.echo("\n=== Queue Information ===")
        for queue_name, info in queue_info.items():
            length = info.get('length', 'unknown')
            click.echo(f"  {queue_name}: {length} messages")

@worker.command()
@click.option('--queue', '-q', required=True,
              help='Queue name to purge')
@click.confirmation_option(prompt='Are you sure you want to purge the queue?')
def purge(queue: str):
    """Purge all messages from a queue."""
    click.echo(f"Purging queue: {queue}")
    purged_count = worker_service.purge_queue(queue)
    click.echo(f"✓ Purged {purged_count} messages from {queue}")

@worker.command()
@click.option('--detach', '-d', is_flag=True, help='Run beat in background')
@click.option('--dry-run', is_flag=True, help='Show command without executing')
def beat(detach: bool, dry_run: bool):
    """Start Celery Beat scheduler."""
    click.echo("Starting Celery Beat scheduler...")
    success = worker_service.start_beat(detach=detach, dry_run=dry_run)
    status = "✓" if success else "✗"
    click.echo(f"{status} Beat scheduler: {'started' if success else 'failed'}")

@worker.command()
@click.option('--port', '-p', type=int, default=5555, help='Port number for Flower')
@click.option('--detach', '-d', is_flag=True, help='Run Flower in background')
def flower(port: int, detach: bool):
    """Start Flower monitoring tool."""
    click.echo(f"Starting Flower on port {port}...")
    success = worker_service.start_flower(port=port, detach=detach)
    status = "✓" if success else "✗"
    click.echo(f"{status} Flower: {'started' if success else 'failed'}")
    if success:
        click.echo(f"Flower UI available at: http://localhost:{port}")

@worker.command()
@click.option('--worker-type', '-t',
              type=click.Choice(['default', 'email', 'data_sync', 'priority']),
              required=True,
              help='Worker type for systemd service')
@click.option('--output', '-o', help='Output file path')
def systemd(worker_type: str, output: Optional[str]):
    """Generate systemd service file for worker."""
    try:
        service_content = worker_service.generate_systemd_service(worker_type)
        
        if output:
            with open(output, 'w') as f:
                f.write(service_content)
            click.echo(f"✓ Systemd service file written to: {output}")
        else:
            click.echo(service_content)
    except Exception as e:
        click.echo(f"✗ Error generating systemd service: {e}")

@worker.command()
@click.option('--output', '-o', default='docker-compose.workers.yml',
              help='Output file path')
def docker_compose(output: str):
    """Generate docker-compose.yml for workers."""
    try:
        compose_content = worker_service.generate_docker_compose()
        
        with open(output, 'w') as f:
            f.write(compose_content)
        
        click.echo(f"✓ Docker Compose file written to: {output}")
        click.echo("To start workers with Docker Compose:")
        click.echo(f"  docker-compose -f {output} up -d")
    except Exception as e:
        click.echo(f"✗ Error generating docker-compose file: {e}")

# Task management commands
@click.group()
def task():
    """Task management commands."""
    pass

@task.command()
@click.option('--limit', '-l', type=int, default=20, help='Limit number of tasks to show')
@click.option('--status', '-s', help='Filter by task status')
@click.option('--format', '-f', type=click.Choice(['json', 'table']), default='table',
              help='Output format')
def list(limit: int, status: Optional[str], format: str):
    """List recent tasks."""
    tasks = task_manager.get_all_tasks(status_filter=status)[:limit]
    
    if format == 'json':
        task_data = []
        for task in tasks:
            task_data.append({
                'task_id': task.task_id,
                'task_name': task.task_name,
                'status': task.status,
                'created_at': task.created_at.isoformat(),
                'queue': task.queue,
                'priority': task.priority
            })
        click.echo(json.dumps(task_data, indent=2))
    else:
        click.echo(f"\n=== Recent Tasks (showing {len(tasks)}) ===")
        click.echo(f"{'Task ID':<12} {'Name':<20} {'Status':<10} {'Queue':<10} {'Created':<20}")
        click.echo("-" * 80)
        
        for task in tasks:
            task_id_short = task.task_id[:8] if task.task_id else 'unknown'
            created_str = task.created_at.strftime('%Y-%m-%d %H:%M:%S')
            click.echo(f"{task_id_short:<12} {task.task_name:<20} {task.status:<10} {task.queue:<10} {created_str:<20}")

@task.command()
@click.argument('task_id')
@click.option('--format', '-f', type=click.Choice(['json', 'table']), default='table',
              help='Output format')
def show(task_id: str, format: str):
    """Show detailed task information."""
    task_info = task_manager.get_task_info(task_id)
    
    if not task_info:
        click.echo(f"✗ Task not found: {task_id}")
        return
    
    if format == 'json':
        task_data = {
            'task_id': task_info.task_id,
            'task_name': task_info.task_name,
            'status': task_info.status,
            'created_at': task_info.created_at.isoformat(),
            'started_at': task_info.started_at.isoformat() if task_info.started_at else None,
            'completed_at': task_info.completed_at.isoformat() if task_info.completed_at else None,
            'queue': task_info.queue,
            'priority': task_info.priority,
            'retries': task_info.retries,
            'result': task_info.result,
            'error': task_info.error,
            'progress': task_info.progress
        }
        click.echo(json.dumps(task_data, indent=2))
    else:
        click.echo(f"\n=== Task Details ===")
        click.echo(f"Task ID: {task_info.task_id}")
        click.echo(f"Name: {task_info.task_name}")
        click.echo(f"Status: {task_info.status}")
        click.echo(f"Queue: {task_info.queue}")
        click.echo(f"Priority: {task_info.priority}")
        click.echo(f"Created: {task_info.created_at}")
        click.echo(f"Started: {task_info.started_at or 'Not started'}")
        click.echo(f"Completed: {task_info.completed_at or 'Not completed'}")
        click.echo(f"Retries: {task_info.retries}/{task_info.max_retries}")
        
        if task_info.progress:
            click.echo(f"Progress: {task_info.progress}")
        
        if task_info.result:
            click.echo(f"Result: {task_info.result}")
        
        if task_info.error:
            click.echo(f"Error: {task_info.error}")

@task.command()
@click.argument('task_id')
@click.confirmation_option(prompt='Are you sure you want to cancel this task?')
def cancel(task_id: str):
    """Cancel a running task."""
    success = task_manager.cancel_task(task_id)
    status = "✓" if success else "✗"
    click.echo(f"{status} Task {task_id}: {'cancelled' if success else 'failed to cancel'}")

@task.command()
@click.option('--format', '-f', type=click.Choice(['json', 'table']), default='table',
              help='Output format')
def stats(format: str):
    """Show task and queue statistics."""
    stats_info = task_manager.get_queue_stats()
    
    if format == 'json':
        click.echo(json.dumps(stats_info, indent=2, default=str))
    else:
        click.echo("\n=== Task Statistics ===")
        
        queue_counts = stats_info.get('queue_counts', {})
        if queue_counts:
            click.echo("\nQueue Task Counts:")
            click.echo(f"{'Queue':<12} {'Processing':<12} {'Completed':<12} {'Failed':<8}")
            click.echo("-" * 50)
            
            for queue, counts in queue_counts.items():
                click.echo(f"{queue:<12} {counts['processing']:<12} {counts['completed']:<12} {counts['failed']:<8}")

# Add subcommands to main CLI
def register_commands(cli):
    """Register worker and task commands."""
    cli.add_command(worker)
    cli.add_command(task)