#!/usr/bin/env python3
"""
FastAPI Management Script — ETL API
Similar to Django's manage.py with auto-discovery of custom commands
"""

import sys
import os
import importlib
import inspect
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import typer
from typing import Optional
from commands.base import BaseCommand

app = typer.Typer(help="ETL API Management Commands")


# ============================================================================
# 1. Auto-Discovery — Custom Commands from commands/ directory
# ============================================================================

def discover_commands():
    """
    Auto-discover custom commands from the commands/ directory.
    Each command file should have a Command class that inherits from BaseCommand.
    Files with multiple Command classes (e.g., worker.py, task.py) are registered
    as Typer groups.
    """
    commands_dir = Path(__file__).parent / "commands"

    if not commands_dir.exists():
        return

    # Multi-command files that should be registered as Typer groups
    group_commands = {'worker', 'task'}

    for file_path in sorted(commands_dir.glob("*.py")):
        if file_path.name in ["__init__.py", "base.py"]:
            continue

        module_name = f"commands.{file_path.stem}"

        try:
            module = importlib.import_module(module_name)

            if file_path.stem in group_commands:
                _register_group_from_module(file_path.stem, module)
                continue

            # Single-command file: look for Command class
            if hasattr(module, "Command"):
                command_class = getattr(module, "Command")
                if inspect.isclass(command_class) and issubclass(command_class, BaseCommand):
                    register_command(file_path.stem, command_class)

        except Exception as e:
            typer.echo(f"Warning: Failed to load command from {module_name}: {e}", err=True)


def register_command(name: str, command_class):
    """
    Register a single custom command with the Typer app.

    Args:
        name: Command name (derived from filename, e.g., 'clear_cache' -> 'clear-cache')
        command_class: The Command class to register
    """
    cli_name = name.replace("_", "-")
    command_instance = command_class()
    arguments = command_instance.add_arguments()

    def command_wrapper(**kwargs):
        command_instance.execute(**kwargs)

    command_wrapper.__name__ = cli_name
    command_wrapper.__doc__ = command_instance.help

    params = []
    for arg_name, arg_default in arguments.items():
        params.append(
            inspect.Parameter(
                arg_name,
                inspect.Parameter.KEYWORD_ONLY,
                default=arg_default
            )
        )
    command_wrapper.__signature__ = inspect.Signature(params)

    app.command(name=cli_name)(command_wrapper)


def _register_group_from_module(name: str, module):
    """
    Register a module with multiple Command classes as a Typer group.
    Each Command class ending with 'Command' becomes a subcommand.
    E.g., WorkerStartCommand → worker start
    """
    group = typer.Typer(help=f"{name.replace('_', '-')} management commands")

    for attr_name in sorted(dir(module)):
        if attr_name.startswith("_"):
            continue

        obj = getattr(module, attr_name)
        if not inspect.isclass(obj) or not issubclass(obj, BaseCommand) or obj is BaseCommand:
            continue

        # Derive subcommand name: WorkerStartCommand -> start, TaskListCommand -> list
        sub_name = attr_name.replace(name.capitalize(), "").replace("Command", "")
        if not sub_name:
            continue
        # Convert CamelCase to kebab-case
        import re
        sub_name = re.sub(r'([A-Z])', r'-\1', sub_name).lower().lstrip('-')

        command_instance = obj()
        arguments = command_instance.add_arguments()

        def make_wrapper(cmd_instance, sub_help):
            def sub_wrapper(**kwargs):
                cmd_instance.execute(**kwargs)
            sub_wrapper.__doc__ = sub_help
            params = []
            for arg_name, arg_default in arguments.items():
                params.append(
                    inspect.Parameter(
                        arg_name,
                        inspect.Parameter.KEYWORD_ONLY,
                        default=arg_default
                    )
                )
            sub_wrapper.__signature__ = inspect.Signature(params)
            return sub_wrapper

        wrapper = make_wrapper(command_instance, getattr(obj, 'help', ''))
        wrapper.__name__ = sub_name
        group.command(name=sub_name)(wrapper)

    app.add_typer(group, name=name.replace("_", "-"))


# Discover and register custom commands
discover_commands()


# ============================================================================
# 2. Built-in Commands
# ============================================================================

@app.command()
def runserver(
    host: str = typer.Option("127.0.0.1", help="Host to bind"),
    port: int = typer.Option(8000, help="Port to bind"),
    reload: bool = typer.Option(False, help="Enable auto-reload"),
):
    """Run the FastAPI development server"""
    import uvicorn
    typer.echo(f"🚀 Starting ETL API server on http://{host}:{port}")
    uvicorn.run("app.main:app", host=host, port=port, reload=reload)


@app.command()
def shell():
    """Start an interactive Python shell with app context"""
    import IPython
    from app.main import app as fastapi_app
    from app.infrastructure.db.manager import database_manager
    from app.infrastructure.cache import cache_manager

    context = {
        'app': fastapi_app,
        'db': database_manager.get_session(),
        'cache': cache_manager,
    }

    # Add models for convenience
    try:
        from app.infrastructure.db.models.auth import User
        from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob
        from app.infrastructure.db.models.etl_control.job_executions import JobExecution
        from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
        from app.infrastructure.db.models.config.data_sources import DataSource
        from app.infrastructure.db.models.config.system_config import SystemConfig
        from app.infrastructure.db.models.etl_control.error_logs import ErrorLog
        from app.infrastructure.db.models.processed.entities import ProcessedEntity

        context.update({
            'User': User,
            'EtlJob': EtlJob,
            'JobExecution': JobExecution,
            'FileRegistry': FileRegistry,
            'DataSource': DataSource,
            'SystemConfig': SystemConfig,
            'ErrorLog': ErrorLog,
            'ProcessedEntity': ProcessedEntity,
        })
    except ImportError:
        pass

    typer.echo("🚀 Starting ETL API interactive shell...")
    typer.echo("Available variables: app, db, cache + all model classes")

    try:
        IPython.start_ipython(argv=[], user_ns=context)
    except ImportError:
        import code
        banner = """
╔════════════════════════════════════════════════════════════╗
║  ETL API Interactive Shell (Python)                        ║
╠════════════════════════════════════════════════════════════╣
║  Database Session:  db                                     ║
║  Cache Manager:     cache                                  ║
║  FastAPI App:       app                                    ║
║                                                            ║
║  Type 'exit()' or Ctrl+D to quit                           ║
╚════════════════════════════════════════════════════════════╝
"""
        code.interact(banner=banner, local=context)


@app.command()
def flower(
    port: int = typer.Option(5555, '--port', '-p', help="Port for Flower"),
):
    """Start Celery Flower monitoring tool"""
    import subprocess
    typer.echo(f"🌸 Starting Flower on http://localhost:{port}")
    try:
        subprocess.run(['celery', '-A', 'app.tasks', 'flower', f'--port={port}'])
    except FileNotFoundError:
        typer.echo("❌ Celery CLI not found. Install with: pip install celery[flower]", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
