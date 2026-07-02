"""
Command untuk clear cache
"""

from commands.base import BaseCommand
import typer


class Command(BaseCommand):
    help = "Clear application cache"

    def add_arguments(self):
        return {
            'pattern': typer.Option(
                None, '--pattern', '-p',
                help='Clear cache keys matching pattern (e.g., "auth:*", "job:*")'
            ),
            'flush_all': typer.Option(
                False, '--flush-all',
                help='Flush all cache data (destructive)'
            ),
            'dry_run': typer.Option(
                False, '--dry-run',
                help='Show what would be cleared without actually clearing'
            ),
        }

    def handle(self, pattern: str, flush_all: bool, dry_run: bool, **options):
        self.print_header("Clear Cache")

        try:
            from app.infrastructure.cache import cache_manager

            if dry_run:
                self.warning("DRY RUN MODE — No data will be deleted")

            if flush_all:
                if dry_run:
                    self.warning("Would flush all cache data")
                else:
                    cache_manager.flush_all()
                    self.success("Flushed all cache data")

            elif pattern:
                if dry_run:
                    self.warning(f"Would clear cache keys matching: {pattern}")
                else:
                    cache_manager.delete_pattern(pattern)
                    self.success(f"Cleared cache keys matching: {pattern}")

            else:
                self.info("No pattern or --flush-all specified. Common patterns:")
                self.print("  auth:*              — Clear auth cache")
                self.print("  job:*               — Clear job cache")
                self.print("  file:*              — Clear file cache")
                self.print("  --flush-all         — Clear everything")
                self.print("")
                self.print("Examples:")
                self.print("  python manage.py clear-cache --pattern 'job:*'")
                self.print("  python manage.py clear-cache --flush-all --dry-run")

        except Exception as e:
            self.error(f"Error clearing cache: {e}")
            raise typer.Exit(1)
