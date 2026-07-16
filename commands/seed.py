"""
Command untuk seeding database dengan data dummy
"""

from commands.base import BaseCommand
import typer
from typing import Optional


class Command(BaseCommand):
    help = "Seed database with dummy data"

    def add_arguments(self):
        return {
            'flush': typer.Option(
                False, '--flush',
                help='Flush existing data before seeding'
            ),
            'count': typer.Option(
                10, '--count', '-c',
                help='Number of records to create (default: 10)'
            ),
            'model': typer.Option(
                None, '--model', '-m',
                help='Specific model to seed (e.g., users, jobs, files)'
            ),
        }

    def handle(self, flush: bool, count: int, model: Optional[str], **options):
        self.print_header("Seed Database")

        try:
            count = int(count)
        except (ValueError, TypeError):
            self.error("Count must be a valid number")
            raise typer.Exit(1)

        if count <= 0:
            self.error("Count must be a positive number")
            raise typer.Exit(1)

        if flush:
            self._flush_data()

        if model:
            self._seed_specific_model(model, count)
        else:
            self._seed_all_models(count)

        self.success("Database seeding completed!")

    def _flush_data(self):
        """Flush existing data"""
        self.warning("Flushing existing data...")
        try:
            from app.infrastructure.db.manager import database_manager

            db = database_manager.get_session()
            from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob
            from app.infrastructure.db.models.etl_control.job_executions import JobExecution
            from app.infrastructure.db.models.raw_data.file_registry import FileRegistry

            db.query(JobExecution).delete()
            db.query(EtlJob).delete()
            db.query(FileRegistry).delete()
            db.commit()

            self.info("Existing data flushed")
        except Exception as e:
            self.warning(f"Could not flush data: {e}")

    def _seed_all_models(self, count):
        """Seed all models"""
        self.info("Seeding all models...")
        self._seed_users(count)
        self._seed_etl_jobs(count)

    def _seed_specific_model(self, model, count):
        """Seed specific model"""
        self.info(f"Seeding {model} model...")

        valid_models = {
            'users': self._seed_users,
            'jobs': self._seed_etl_jobs,
            'files': self._seed_files,
        }

        if model in valid_models:
            valid_models[model](count)
        else:
            self.error(f"Unknown model: {model}")
            self.info(f"Available models: {', '.join(valid_models.keys())}")

    def _seed_users(self, count):
        """Seed users table"""
        self.info(f"Creating {count} users...")
        try:
            from app.infrastructure.db.manager import database_manager
            from app.infrastructure.db.models.auth import User
            from app.core.security import get_password_hash
            from faker import Faker
            import random

            fake = Faker()
            db = database_manager.get_session()

            created = 0
            for i in range(count):
                username = fake.unique.user_name()
                email = fake.unique.email()
                name = fake.name()

                existing = db.query(User).filter(
                    (User.email == email) | (User.username == username)
                ).first()

                if existing:
                    self.warning(f"User {username} or {email} already exists, skipping...")
                    continue

                user = User(
                    username=username,
                    email=email,
                    name=name,
                    is_superuser=random.choice([True, False]),
                    is_active=True,
                    is_verified=random.choice([True, False]),
                    password=get_password_hash("password123"),
                )
                db.add(user)
                created += 1
                self.info(f"  Created: {username} ({email})")

            db.commit()
            self.success(f"Created {created} user(s)")
            if created > 0:
                self.info("Default password for all users: password123")

        except ImportError as e:
            self.warning(f"Could not seed users (missing dependency): {e}")
            self._placeholder_seed("users", count)
        except Exception as e:
            self.error(f"Failed to seed users: {e}")

    def _seed_etl_jobs(self, count):
        """Seed ETL jobs"""
        self.info(f"Creating {count} ETL jobs...")
        try:
            from app.infrastructure.db.manager import database_manager
            from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob
            from faker import Faker
            import random

            fake = Faker()
            db = database_manager.get_session()

            job_types = ['ingestion', 'transformation', 'validation', 'export']
            statuses = ['active', 'inactive', 'draft']

            created = 0
            for i in range(count):
                job = EtlJob(
                    name=f"job_{fake.unique.slug()}",
                    description=fake.sentence(),
                    job_type=random.choice(job_types),
                    status=random.choice(statuses),
                    schedule=f"0 */{random.randint(1, 12)} * * *",
                    config={"source": fake.url(), "destination": fake.file_path()},
                )
                db.add(job)
                created += 1
                self.info(f"  Created job: {job.name}")

            db.commit()
            self.success(f"Created {created} ETL job(s)")

        except ImportError as e:
            self.warning(f"Could not seed ETL jobs (missing dependency): {e}")
            self._placeholder_seed("etl jobs", count)
        except Exception as e:
            self.error(f"Failed to seed ETL jobs: {e}")

    def _seed_files(self, count):
        """Seed file registry entries"""
        self.info(f"Creating {count} file registry entries...")
        try:
            from app.infrastructure.db.manager import database_manager
            from app.infrastructure.db.models.raw_data.file_registry import FileRegistry
            from faker import Faker
            import random
            from datetime import datetime, timedelta

            fake = Faker()
            db = database_manager.get_session()

            created = 0
            for i in range(count):
                days_ago = random.randint(0, 30)
                file_entry = FileRegistry(
                    filename=f"{fake.slug()}.csv",
                    file_path=f"/uploads/{fake.slug()}/{fake.slug()}.csv",
                    file_size=random.randint(1024, 10485760),
                    mime_type="text/csv",
                    status=random.choice(['uploaded', 'processing', 'processed', 'failed']),
                    uploaded_at=datetime.utcnow() - timedelta(days=days_ago),
                    metadata={"source": fake.company()},
                )
                db.add(file_entry)
                created += 1
                self.info(f"  Created file: {file_entry.filename}")

            db.commit()
            self.success(f"Created {created} file registry entrie(s)")

        except ImportError as e:
            self.warning(f"Could not seed files (missing dependency): {e}")
            self._placeholder_seed("files", count)
        except Exception as e:
            self.error(f"Failed to seed files: {e}")

    def _placeholder_seed(self, entity: str, count: int):
        """Fallback placeholder"""
        self.warning(f"Using placeholder seeding for {entity}")
        for i in range(count):
            self.info(f"  Creating {entity} entry {i+1}/{count}")
        self.success(f"Created {count} {entity} (placeholder)")
