"""
Command untuk clear cache
"""

import sys
from pathlib import Path

current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
from base import BaseCommand

project_root = current_dir.parent.parent.parent.parent
sys.path.insert(0, str(project_root))


class Command(BaseCommand):
    description = "Clear application cache"

    def add_arguments(self, parser):
        parser.add_argument(
            '--pattern',
            help='Clear cache keys matching pattern (e.g., "auth:*", "job:*")'
        )
        parser.add_argument(
            '--flush-all',
            action='store_true',
            help='Flush all cache data (destructive)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be cleared without actually clearing'
        )

    def handle(self, **kwargs):
        pattern = kwargs.get('pattern')
        flush_all = kwargs.get('flush_all', False)
        dry_run = kwargs.get('dry_run', False)

        try:
            from app.infrastructure.cache import cache_manager

            self.print_info("Clearing cache...")

            if dry_run:
                self.print_warning("DRY RUN: No data will be deleted")

            if flush_all:
                if dry_run:
                    self.print_warning("Would flush all cache data")
                else:
                    cache_manager.flush_all()
                    self.print_success("Flushed all cache data")

            elif pattern:
                if dry_run:
                    self.print_warning(f"Would clear cache keys matching: {pattern}")
                else:
                    cache_manager.delete_pattern(pattern)
                    self.print_success(f"Cleared cache keys matching: {pattern}")

            else:
                self.print_info("No pattern or flush-all specified. Common patterns:")
                print("  auth:*              - Clear auth cache")
                print("  job:*               - Clear job cache")
                print("  file:*              - Clear file cache")
                print("  --flush-all         - Clear everything")
                print("\nExample:")
                print("  python manage.py clear-cache --pattern 'job:*'")
                print("  python manage.py clear-cache --flush-all --dry-run")

        except Exception as e:
            self.print_error(f"Error clearing cache: {e}")
            sys.exit(1)
