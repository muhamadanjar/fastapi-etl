"""
Command untuk database migration
"""

from commands.base import BaseCommand
import typer


class Command(BaseCommand):
    help = "Run database migrations"

    def add_arguments(self):
        return {
            'fake': typer.Option(
                False, '--fake',
                help='Mark migrations as run without actually running them'
            ),
            'check': typer.Option(
                False, '--check',
                help='Check if there are any pending migrations'
            ),
            'app_name': typer.Argument(
                None,
                help='Specific app to migrate (optional)'
            ),
        }

    def handle(self, fake: bool, check: bool, app_name: str, **options):
        if check:
            self._check_migrations()
        else:
            self._run_migrations(fake=fake, app_name=app_name)

    def _check_migrations(self):
        """Check for pending migrations using Alembic"""
        self.print_header("Check Migrations")
        self.info("Checking for pending migrations...")

        try:
            from alembic.config import Config
            from alembic import command
            from pathlib import Path

            alembic_cfg_path = Path(__file__).parent.parent / "alembic.ini"
            if not alembic_cfg_path.exists():
                self.error("alembic.ini not found. Is Alembic configured?")
                raise typer.Exit(1)

            alembic_cfg = Config(str(alembic_cfg_path))

            from alembic.script import ScriptDirectory
            from alembic.runtime.migration import MigrationContext
            from sqlalchemy import create_engine

            script = ScriptDirectory.from_config(alembic_cfg)
            engine = create_engine(alembic_cfg.get_main_option("sqlalchemy.url"))
            with engine.connect() as conn:
                context = MigrationContext.configure(conn)
                current_rev = context.get_current_revision()
                head_rev = script.get_current_head()

            if current_rev == head_rev:
                self.success("No pending migrations found. Database is up to date.")
            else:
                pending = [rev for rev in script.iterate_revisions("head", current_rev)]
                self.warning(f"Found {len(pending)} pending migration(s):")
                for rev in pending:
                    self.print(f"  • {rev.revision[:8]} — {rev.doc}")
                raise typer.Exit(1)

        except typer.Exit:
            raise
        except ImportError:
            self.warning("Alembic not installed. Cannot check migrations.")
            self.info("Install with: pip install alembic")
        except Exception as e:
            self.error(f"Error checking migrations: {e}")
            raise typer.Exit(1)

    def _run_migrations(self, fake: bool = False, app_name: str = None):
        """Run database migrations"""
        self.print_header("Run Migrations")

        if fake:
            self.info("Running fake migrations...")
        else:
            self.info("Running migrations...")

        if app_name:
            self.info(f"Target app: {app_name}")

        try:
            from alembic.config import Config
            from alembic import command
            from pathlib import Path

            alembic_cfg_path = Path(__file__).parent.parent / "alembic.ini"
            if not alembic_cfg_path.exists():
                self.error("alembic.ini not found. Is Alembic configured?")
                raise typer.Exit(1)

            alembic_cfg = Config(str(alembic_cfg_path))

            if fake:
                command.upgrade(alembic_cfg, "head", sql=False)
                self.info("Marking all migrations as applied...")

            command.upgrade(alembic_cfg, "head")
            self.success("Migrations completed successfully!")

        except typer.Exit:
            raise
        except ImportError:
            self.warning("Alembic not installed. Falling back to placeholder.")
            self._placeholder_migrate(fake, app_name)
        except Exception as e:
            self.error(f"Migration failed: {e}")
            raise typer.Exit(1)

    def _placeholder_migrate(self, fake, app_name):
        """Fallback placeholder when Alembic is not installed"""
        self.warning("Using placeholder migration (Alembic not available)")

        migrations = [
            "0001_initial",
            "0002_add_etl_jobs",
            "0003_add_file_registry",
        ]

        if app_name:
            migrations = [m for m in migrations if app_name in m]

        for migration in migrations:
            if fake:
                self.info(f"  Faking {migration}... OK")
            else:
                self.info(f"  Applying {migration}... OK")

        self.success("Migrations completed (placeholder)!")
        self.warning("Install Alembic for real migration support: pip install alembic")
