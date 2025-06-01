"""
Database migration utilities.
"""

import logging
from typing import List, Dict, Any
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import text

from .connection import database_manager
from ...core.config import get_settings

logger = logging.getLogger(__name__)


class MigrationManager:
    """
    Database migration manager using Alembic.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.migrations_dir = Path(__file__).parent.parent.parent.parent / "migrations"
        self.alembic_cfg = self._get_alembic_config()
    
    def _get_alembic_config(self) -> Config:
        """Get Alembic configuration."""
        config = Config()
        config.set_main_option("script_location", str(self.migrations_dir))
        config.set_main_option("sqlalchemy.url", self.settings.DATABASE_URL)
        return config
    
    def create_migration(self, message: str, autogenerate: bool = True) -> str:
        """
        Create a new migration.
        
        Args:
            message: Migration message
            autogenerate: Whether to auto-generate migration from model changes
            
        Returns:
            Migration revision ID
        """
        try:
            logger.info(f"Creating migration: {message}")
            
            if autogenerate:
                command.revision(self.alembic_cfg, message=message, autogenerate=True)
            else:
                command.revision(self.alembic_cfg, message=message)
            
            logger.info(f"Migration created: {message}")
            return message
            
        except Exception as e:
            logger.error(f"Failed to create migration: {e}")
            raise
    
    def run_migrations(self, revision: str = "head") -> None:
        """
        Run database migrations.
        
        Args:
            revision: Target revision (default: "head")
        """
        try:
            logger.info(f"Running migrations to: {revision}")
            command.upgrade(self.alembic_cfg, revision)
            logger.info("Migrations completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to run migrations: {e}")
            raise
    
    def downgrade_migration(self, revision: str) -> None:
        """
        Downgrade to a specific migration.
        
        Args:
            revision: Target revision
        """
        try:
            logger.info(f"Downgrading to: {revision}")
            command.downgrade(self.alembic_cfg, revision)
            logger.info("Downgrade completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to downgrade migration: {e}")
            raise
    
    def get_current_revision(self) -> str:
        """
        Get current database revision.
        
        Returns:
            Current revision ID
        """
        try:
            with database_manager.get_session() as session:
                context = MigrationContext.configure(session.connection())
                return context.get_current_revision()
                
        except Exception as e:
            logger.error(f"Failed to get current revision: {e}")
            raise
    
    def get_migration_history(self) -> List[Dict[str, Any]]:
        """
        Get migration history.
        
        Returns:
            List of migration information
        """
        try:
            script = ScriptDirectory.from_config(self.alembic_cfg)
            history = []
            
            for revision in script.walk_revisions():
                history.append({
                    "revision": revision.revision,
                    "down_revision": revision.down_revision,
                    "branch_labels": revision.branch_labels,
                    "depends_on": revision.depends_on,
                    "doc": revision.doc,
                })
            
            return history
            
        except Exception as e:
            logger.error(f"Failed to get migration history: {e}")
            raise
    
    def reset_database(self) -> None:
        """
        Reset database (drop all tables and recreate).
        
        Warning: This will delete all data!
        """
        try:
            logger.warning("Resetting database - ALL DATA WILL BE LOST!")
            
            # Drop all tables
            with database_manager.get_session() as session:
                # Get all table names
                result = session.exec(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """))
                
                tables = [row[0] for row in result.fetchall()]
                
                # Drop tables
                for table in tables:
                    session.exec(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
                
                session.commit()
            
            # Recreate tables
            from sqlmodel import SQLModel
            engine = database_manager.get_engine()
            SQLModel.metadata.create_all(bind=engine)
            
            logger.warning("Database reset completed")
            
        except Exception as e:
            logger.error(f"Failed to reset database: {e}")
            raise


# Global migration manager instance
migration_manager = MigrationManager()