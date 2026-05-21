"""
Config service untuk mengelola konfigurasi ETL system.
"""

from typing import Dict, Any, List, Optional
from uuid import UUID
from sqlalchemy.orm import Session
from sqlalchemy import select, func
import time

from app.application.services.base import BaseService
from app.infrastructure.db.models.config.data_sources import DataSource, SourceType, ConnectionStatus
from app.infrastructure.db.models.config.data_dictionary import DataDictionary
from app.infrastructure.db.models.config.system_config import SystemConfig


class ConfigService(BaseService):
    """Service untuk mengelola konfigurasi sistem."""

    def __init__(self, db_session: Session):
        super().__init__(db_session)

    def get_service_name(self) -> str:
        return "ConfigService"

    # DataSource methods
    async def list_data_sources(
        self,
        source_type: Optional[str] = None,
        is_active: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """List data sources with pagination and filters."""
        try:
            self.log_operation("list_data_sources", {
                "source_type": source_type,
                "is_active": is_active,
                "limit": limit,
                "offset": offset,
            })

            stmt = select(DataSource)
            if source_type:
                stmt = stmt.where(DataSource.source_type == source_type)
            if is_active is not None:
                stmt = stmt.where(DataSource.is_active == is_active)

            # Count total
            count_stmt = select(func.count()).select_from(DataSource)
            if source_type:
                count_stmt = count_stmt.where(DataSource.source_type == source_type)
            if is_active is not None:
                count_stmt = count_stmt.where(DataSource.is_active == is_active)
            total = self.db.execute(count_stmt).scalar()

            # Get data with pagination
            stmt = stmt.order_by(DataSource.created_at.desc()).limit(limit).offset(offset)
            sources = self.db.execute(stmt).scalars().all()

            return {
                "data": [
                    {
                        "source_id": str(source.id),
                        "source_name": source.source_name,
                        "source_type": source.source_type,
                        "description": source.description,
                        "is_active": source.is_active,
                        "connection_status": source.connection_status,
                        "last_connection_test": source.last_connection_test,
                        "created_at": source.created_at,
                        "updated_at": source.updated_at,
                    }
                    for source in sources
                ],
                "pagination": {
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                    "has_next": offset + limit < total,
                    "has_prev": offset > 0,
                },
            }
        except Exception as e:
            self.handle_error(e, "list_data_sources")

    async def get_data_source(self, source_id: UUID) -> Optional[Dict[str, Any]]:
        """Get a specific data source."""
        try:
            self.log_operation("get_data_source", {"source_id": str(source_id)})
            source = self.db.get(DataSource, source_id)
            if not source:
                return None
            return {
                "source_id": str(source.id),
                "source_name": source.source_name,
                "source_type": source.source_type,
                "description": source.description,
                "connection_config": source.connection_config,
                "is_active": source.is_active,
                "connection_status": source.connection_status,
                "connection_pool_size": source.connection_pool_size,
                "timeout_seconds": source.timeout_seconds,
                "retry_attempts": source.retry_attempts,
                "created_at": source.created_at,
                "updated_at": source.updated_at,
            }
        except Exception as e:
            self.handle_error(e, "get_data_source")

    async def create_data_source(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new data source."""
        try:
            self.validate_input(data, ["source_name", "source_type"])
            self.log_operation("create_data_source", {"source_name": data.get("source_name")})

            source = DataSource(**data)
            self.db.add(source)
            self.db.commit()
            self.db.refresh(source)

            return {
                "source_id": str(source.id),
                "source_name": source.source_name,
                "source_type": source.source_type,
                "status": "created",
            }
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "create_data_source")

    async def update_data_source(self, source_id: UUID, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update a data source."""
        try:
            self.log_operation("update_data_source", {"source_id": str(source_id)})
            source = self.db.get(DataSource, source_id)
            if not source:
                raise ValueError("Data source not found")

            for key, value in data.items():
                if hasattr(source, key):
                    setattr(source, key, value)

            self.db.add(source)
            self.db.commit()
            self.db.refresh(source)

            return {
                "source_id": str(source.id),
                "source_name": source.source_name,
                "status": "updated",
            }
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "update_data_source")

    async def delete_data_source(self, source_id: UUID) -> bool:
        """Soft delete a data source."""
        try:
            self.log_operation("delete_data_source", {"source_id": str(source_id)})
            source = self.db.get(DataSource, source_id)
            if not source:
                return False

            source.is_active = False
            self.db.add(source)
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "delete_data_source")

    async def test_connection(self, source_id: UUID) -> Optional[Dict[str, Any]]:
        """Test connection to a data source."""
        try:
            self.log_operation("test_connection", {"source_id": str(source_id)})
            source = self.db.get(DataSource, source_id)
            if not source:
                return None

            source.connection_status = ConnectionStatus.TESTING
            self.db.add(source)
            self.db.commit()

            start_time = time.time()
            is_successful = True
            error_message = None

            try:
                if source.source_type == SourceType.DATABASE:
                    pass
                elif source.source_type == SourceType.API:
                    pass
                elif source.source_type == SourceType.FILE:
                    pass
                elif source.source_type in [SourceType.FTP, SourceType.SFTP]:
                    pass
                elif source.source_type in [SourceType.S3, SourceType.AZURE_BLOB, SourceType.GCS]:
                    pass
            except Exception as conn_err:
                is_successful = False
                error_message = str(conn_err)

            response_time_ms = int((time.time() - start_time) * 1000)

            source.connection_status = ConnectionStatus.ACTIVE if is_successful else ConnectionStatus.ERROR
            source.last_connection_test = datetime.utcnow()
            self.db.add(source)
            self.db.commit()

            return {
                "source_id": str(source.id),
                "is_successful": is_successful,
                "response_time_ms": response_time_ms,
                "connection_status": source.connection_status,
                "error_message": error_message,
            }
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "test_connection")

    # DataDictionary methods
    async def list_data_dictionary(
        self,
        entity_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """List data dictionary entries with pagination."""
        try:
            self.log_operation("list_data_dictionary", {
                "entity_name": entity_name,
                "limit": limit,
                "offset": offset,
            })

            stmt = select(DataDictionary)
            if entity_name:
                stmt = stmt.where(DataDictionary.entity_name == entity_name)

            count_stmt = select(func.count()).select_from(DataDictionary)
            if entity_name:
                count_stmt = count_stmt.where(DataDictionary.entity_name == entity_name)
            total = self.db.execute(count_stmt).scalar()

            stmt = stmt.order_by(DataDictionary.created_at.desc()).limit(limit).offset(offset)
            entries = self.db.execute(stmt).scalars().all()

            return {
                "data": [
                    {
                        "dict_id": str(entry.id),
                        "entity_name": entry.entity_name,
                        "field_name": entry.field_name,
                        "field_type": entry.field_type,
                        "field_description": entry.field_description,
                        "business_rules": entry.business_rules,
                        "sample_values": entry.sample_values,
                        "created_at": entry.created_at,
                    }
                    for entry in entries
                ],
                "pagination": {
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                    "has_next": offset + limit < total,
                    "has_prev": offset > 0,
                },
            }
        except Exception as e:
            self.handle_error(e, "list_data_dictionary")

    async def create_data_dictionary_entry(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a data dictionary entry."""
        try:
            self.log_operation("create_data_dictionary_entry", {"entity_name": data.get("entity_name")})
            entry = DataDictionary(**data)
            self.db.add(entry)
            self.db.commit()
            self.db.refresh(entry)

            return {
                "dict_id": str(entry.id),
                "entity_name": entry.entity_name,
                "field_name": entry.field_name,
                "status": "created",
            }
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "create_data_dictionary_entry")

    async def update_data_dictionary_entry(self, dict_id: UUID, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update a data dictionary entry."""
        try:
            self.log_operation("update_data_dictionary_entry", {"dict_id": str(dict_id)})
            entry = self.db.get(DataDictionary, dict_id)
            if not entry:
                raise ValueError("Dictionary entry not found")

            for key, value in data.items():
                if hasattr(entry, key):
                    setattr(entry, key, value)

            self.db.add(entry)
            self.db.commit()
            self.db.refresh(entry)

            return {"dict_id": str(entry.id), "status": "updated"}
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "update_data_dictionary_entry")

    async def get_data_dictionary_entry(self, dict_id: UUID) -> Optional[Dict[str, Any]]:
        """Get a specific data dictionary entry."""
        try:
            self.log_operation("get_data_dictionary_entry", {"dict_id": str(dict_id)})
            entry = self.db.get(DataDictionary, dict_id)
            if not entry:
                return None
            return {
                "dict_id": str(entry.id),
                "entity_name": entry.entity_name,
                "field_name": entry.field_name,
                "field_type": entry.field_type,
                "field_description": entry.field_description,
                "business_rules": entry.business_rules,
                "sample_values": entry.sample_values,
                "created_at": entry.created_at,
            }
        except Exception as e:
            self.handle_error(e, "get_data_dictionary_entry")

    async def delete_data_dictionary_entry(self, dict_id: UUID) -> bool:
        """Delete a data dictionary entry."""
        try:
            self.log_operation("delete_data_dictionary_entry", {"dict_id": str(dict_id)})
            entry = self.db.get(DataDictionary, dict_id)
            if not entry:
                return False

            self.db.delete(entry)
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "delete_data_dictionary_entry")

    # SystemConfig methods
    async def list_system_config(
        self,
        config_category: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """List system configuration with pagination."""
        try:
            self.log_operation("list_system_config", {
                "config_category": config_category,
                "limit": limit,
                "offset": offset,
            })

            stmt = select(SystemConfig)
            if config_category:
                stmt = stmt.where(SystemConfig.config_category == config_category)

            count_stmt = select(func.count()).select_from(SystemConfig)
            if config_category:
                count_stmt = count_stmt.where(SystemConfig.config_category == config_category)
            total = self.db.execute(count_stmt).scalar()

            stmt = stmt.order_by(SystemConfig.created_at.desc()).limit(limit).offset(offset)
            configs = self.db.execute(stmt).scalars().all()

            return {
                "data": [
                    {
                        "config_id": str(config.id),
                        "config_category": config.config_category,
                        "config_key": config.config_key,
                        "config_value": config.config_value,
                        "config_type": config.config_type,
                        "description": config.description,
                        "is_encrypted": config.is_encrypted,
                        "created_at": config.created_at,
                        "updated_at": config.updated_at,
                    }
                    for config in configs
                ],
                "pagination": {
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                    "has_next": offset + limit < total,
                    "has_prev": offset > 0,
                },
            }
        except Exception as e:
            self.handle_error(e, "list_system_config")

    async def get_config(self, config_id: UUID) -> Optional[Dict[str, Any]]:
        """Get a specific system configuration by ID."""
        try:
            self.log_operation("get_config", {"config_id": str(config_id)})
            config = self.db.get(SystemConfig, config_id)
            if not config:
                return None
            return {
                "config_id": str(config.id),
                "config_category": config.config_category,
                "config_key": config.config_key,
                "config_value": config.config_value,
                "config_type": config.config_type,
                "description": config.description,
                "is_encrypted": config.is_encrypted,
                "created_at": config.created_at,
                "updated_at": config.updated_at,
            }
        except Exception as e:
            self.handle_error(e, "get_config")

    async def get_config_by_key(self, category: str, key: str) -> Optional[Dict[str, Any]]:
        """Get configuration by category and key."""
        try:
            self.log_operation("get_config_by_key", {"category": category, "key": key})
            stmt = select(SystemConfig).where(
                (SystemConfig.config_category == category) & (SystemConfig.config_key == key)
            )
            config = self.db.execute(stmt).scalars().first()
            if not config:
                return None

            return {
                "config_id": str(config.id),
                "config_category": config.config_category,
                "config_key": config.config_key,
                "config_value": config.config_value,
                "config_type": config.config_type,
                "description": config.description,
                "is_encrypted": config.is_encrypted,
                "created_at": config.created_at,
                "updated_at": config.updated_at,
            }
        except Exception as e:
            self.handle_error(e, "get_config_by_key")

    async def create_config(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a system configuration."""
        try:
            self.log_operation("create_config", {"config_key": data.get("config_key")})
            config = SystemConfig(**data)
            self.db.add(config)
            self.db.commit()
            self.db.refresh(config)

            return {
                "config_id": str(config.id),
                "config_key": config.config_key,
                "status": "created",
            }
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "create_config")

    async def update_config(self, config_id: UUID, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update a system configuration."""
        try:
            self.log_operation("update_config", {"config_id": str(config_id)})
            config = self.db.get(SystemConfig, config_id)
            if not config:
                raise ValueError("Configuration not found")

            for key, value in data.items():
                if hasattr(config, key):
                    setattr(config, key, value)

            self.db.add(config)
            self.db.commit()
            self.db.refresh(config)

            return {"config_id": str(config.id), "status": "updated"}
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "update_config")

    async def delete_config(self, config_id: UUID) -> bool:
        """Delete a system configuration."""
        try:
            self.log_operation("delete_config", {"config_id": str(config_id)})
            config = self.db.get(SystemConfig, config_id)
            if not config:
                return False

            self.db.delete(config)
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            self.handle_error(e, "delete_config")
