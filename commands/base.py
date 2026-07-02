"""
Base Command Class for Custom Management Commands
Similar to Django's BaseCommand
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import typer
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

console = Console()


class BaseCommand(ABC):
    """
    Base class for creating custom management commands.

    Similar to Django's BaseCommand, this provides a structured way to create
    CLI commands with consistent argument handling and output formatting.

    Example:
        class MyCommand(BaseCommand):
            help = "Description of my command"

            def add_arguments(self) -> Dict[str, Any]:
                return {
                    'count': typer.Option(10, help="Number of items"),
                    'verbose': typer.Option(False, help="Verbose output")
                }

            def handle(self, count: int, verbose: bool, **options):
                self.success(f"Processing {count} items")
                # Your command logic here
    """

    # Command metadata
    help: str = "No help text provided"

    def __init__(self):
        self.console = console

    @abstractmethod
    def handle(self, **options):
        """
        The actual logic of the command. Subclasses must implement this method.

        Args:
            **options: Command arguments and options passed from CLI
        """
        raise NotImplementedError("Subclasses must implement handle() method")

    def add_arguments(self) -> Dict[str, Any]:
        """
        Define command arguments and options.

        Returns:
            Dict mapping argument names to typer.Option/typer.Argument definitions

        Example:
            return {
                'name': typer.Argument(..., help="Name of the item"),
                'count': typer.Option(10, help="Number of items"),
                'force': typer.Option(False, help="Force operation")
            }
        """
        return {}

    def success(self, message: str):
        """Print a success message in green"""
        self.console.print(f"✅ {message}", style="bold green")

    def error(self, message: str):
        """Print an error message in red"""
        self.console.print(f"❌ {message}", style="bold red")

    def warning(self, message: str):
        """Print a warning message in yellow"""
        self.console.print(f"⚠️  {message}", style="bold yellow")

    def info(self, message: str):
        """Print an info message in blue"""
        self.console.print(f"ℹ️  {message}", style="bold blue")

    def print(self, message: str, style: Optional[str] = None):
        """Print a message with optional styling"""
        self.console.print(message, style=style)

    def confirm(self, message: str, default: bool = False) -> bool:
        """
        Ask for user confirmation

        Args:
            message: The confirmation message
            default: Default value if user just presses Enter

        Returns:
            True if user confirms, False otherwise
        """
        return typer.confirm(message, default=default)

    def print_header(self, title: str):
        """Print a formatted header"""
        self.console.print(Panel(
            Text(title, justify="center", style="bold cyan"),
            border_style="cyan"
        ))

    def execute(self, **kwargs):
        """
        Execute the command. This is called by the CLI framework.
        You should not override this method.
        """
        try:
            self.handle(**kwargs)
        except Exception as e:
            self.error(f"Command failed: {str(e)}")
            raise typer.Exit(1)
