"""
Task management commands — list, show, cancel, and get stats for background tasks.
"""

from commands.base import BaseCommand
import typer
import json
from typing import Optional


class TaskListCommand(BaseCommand):
    """List recent tasks"""

    help = "List recent tasks"

    def add_arguments(self):
        return {
            'limit': typer.Option(
                20, '--limit', '-l',
                help='Limit number of tasks to show'
            ),
            'status_filter': typer.Option(
                None, '--status', '-s',
                help='Filter by task status (PENDING, STARTED, SUCCESS, FAILURE, etc.)'
            ),
            'output_format': typer.Option(
                'table', '--format', '-f',
                help='Output format: json or table'
            ),
        }

    def handle(self, limit: int, status_filter: Optional[str], output_format: str, **options):
        self.print_header("Task List")

        try:
            from app.interfaces.background.task_manager import TaskManager
            task_manager = TaskManager()
            tasks = task_manager.get_all_tasks(status_filter=status_filter)[:limit]

            if output_format == 'json':
                task_data = []
                for task in tasks:
                    task_data.append({
                        'task_id': task.task_id,
                        'task_name': task.task_name,
                        'status': task.status,
                        'created_at': task.created_at.isoformat() if task.created_at else None,
                        'queue': task.queue,
                        'priority': task.priority,
                    })
                self.print(json.dumps(task_data, indent=2))
                return

            if not tasks:
                self.info("No tasks found")
                return

            self.print("")
            self.print(f"{'Task ID':<12} {'Name':<25} {'Status':<12} {'Queue':<12} {'Created'}")
            self.print("─" * 90)

            for task in tasks:
                task_id_short = task.task_id[:8] if task.task_id else 'unknown'
                created_str = task.created_at.strftime('%Y-%m-%d %H:%M:%S') if task.created_at else '-'
                self.print(f"{task_id_short:<12} {task.task_name:<25} {task.status:<12} {task.queue:<12} {created_str}")

            self.print("")
            self.success(f"Showing {len(tasks)} task(s)")

        except ImportError:
            self.warning("Task manager not available.")
        except Exception as e:
            self.error(f"Failed to list tasks: {e}")


class TaskShowCommand(BaseCommand):
    """Show detailed task information"""

    help = "Show detailed task information"

    def add_arguments(self):
        return {
            'task_id': typer.Argument(
                ...,
                help='Task ID to show details for'
            ),
            'output_format': typer.Option(
                'table', '--format', '-f',
                help='Output format: json or table'
            ),
        }

    def handle(self, task_id: str, output_format: str, **options):
        self.print_header(f"Task: {task_id[:16]}...")

        try:
            from app.interfaces.background.task_manager import TaskManager
            task_manager = TaskManager()
            task_info = task_manager.get_task_info(task_id)

            if not task_info:
                self.error(f"Task not found: {task_id}")
                raise typer.Exit(1)

            if output_format == 'json':
                task_data = {
                    'task_id': task_info.task_id,
                    'task_name': task_info.task_name,
                    'status': task_info.status,
                    'created_at': task_info.created_at.isoformat() if task_info.created_at else None,
                    'started_at': task_info.started_at.isoformat() if task_info.started_at else None,
                    'completed_at': task_info.completed_at.isoformat() if task_info.completed_at else None,
                    'queue': task_info.queue,
                    'priority': task_info.priority,
                    'retries': task_info.retries,
                    'result': str(task_info.result) if task_info.result else None,
                    'error': task_info.error,
                    'progress': task_info.progress,
                }
                self.print(json.dumps(task_data, indent=2, default=str))
                return

            self.print("")
            self.print(f"  Task ID:     {task_info.task_id}")
            self.print(f"  Name:        {task_info.task_name}")
            self.print(f"  Status:      {task_info.status}")
            self.print(f"  Queue:       {task_info.queue}")
            self.print(f"  Priority:    {task_info.priority}")
            self.print(f"  Created:     {task_info.created_at}")
            self.print(f"  Started:     {task_info.started_at or 'Not started'}")
            self.print(f"  Completed:   {task_info.completed_at or 'Not completed'}")
            self.print(f"  Retries:     {task_info.retries}/{task_info.max_retries}")

            if task_info.progress:
                self.print(f"  Progress:    {task_info.progress}")

            if task_info.result:
                self.print(f"  Result:      {task_info.result}")

            if task_info.error:
                self.print(f"  Error:       {task_info.error}", style="bold red")

            self.print("")

        except typer.Exit:
            raise
        except ImportError:
            self.warning("Task manager not available.")
        except Exception as e:
            self.error(f"Failed to get task info: {e}")


class TaskCancelCommand(BaseCommand):
    """Cancel a running task"""

    help = "Cancel a running task"

    def add_arguments(self):
        return {
            'task_id': typer.Argument(
                ...,
                help='Task ID to cancel'
            ),
            'force': typer.Option(
                False, '--force', '-f',
                help='Skip confirmation prompt'
            ),
        }

    def handle(self, task_id: str, force: bool, **options):
        self.print_header("Cancel Task")

        if not force:
            if not self.confirm(f"Are you sure you want to cancel task {task_id}?"):
                self.warning("Operation cancelled")
                return

        try:
            from app.interfaces.background.task_manager import TaskManager
            task_manager = TaskManager()
            success = task_manager.cancel_task(task_id)

            if success:
                self.success(f"Task {task_id} cancelled")
            else:
                self.error(f"Failed to cancel task {task_id}")

        except ImportError:
            self.warning("Task manager not available.")
        except Exception as e:
            self.error(f"Failed to cancel task: {e}")


class TaskStatsCommand(BaseCommand):
    """Show task and queue statistics"""

    help = "Show task and queue statistics"

    def add_arguments(self):
        return {
            'output_format': typer.Option(
                'table', '--format', '-f',
                help='Output format: json or table'
            ),
        }

    def handle(self, output_format: str, **options):
        self.print_header("Task Statistics")

        try:
            from app.interfaces.background.task_manager import TaskManager
            task_manager = TaskManager()
            stats_info = task_manager.get_queue_stats()

            if output_format == 'json':
                self.print(json.dumps(stats_info, indent=2, default=str))
                return

            queue_counts = stats_info.get('queue_counts', {})
            if not queue_counts:
                self.info("No queue statistics available")
                return

            self.print("")
            self.print("Queue Task Counts:", style="bold")
            self.print(f"  {'Queue':<14} {'Processing':<12} {'Completed':<12} {'Failed'}")
            self.print("  " + "─" * 50)

            for queue, counts in queue_counts.items():
                self.print(
                    f"  {queue:<14} "
                    f"{counts.get('processing', 0):<12} "
                    f"{counts.get('completed', 0):<12} "
                    f"{counts.get('failed', 0)}"
                )

            self.print("")

        except ImportError:
            self.warning("Task manager not available.")
        except Exception as e:
            self.error(f"Failed to get task stats: {e}")
