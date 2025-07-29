"""
Database seeding utilities.
"""

import logging
from typing import List, Dict, Any
from uuid import UUID, uuid4

from .connection import database_manager
from app.infrastructure.db.models.auth import User, UserProfile
from ...core.security import get_password_hash

logger = logging.getLogger(__name__)


class DatabaseSeeder:
    """
    Database seeding manager.
    """
    
    def __init__(self):
        self.session = None
    
    async def seed_all(self) -> None:
        """Seed all data."""
        async with database_manager.get_async_session() as session:
            self.session = session
            
            logger.info("Starting database seeding...")
            
            await self.seed_users()
            
            logger.info("Database seeding completed")
    
    async def seed_users(self) -> List[User]:
        """
        Seed initial users.
        
        Returns:
            List of created users
        """
        logger.info("Seeding users...")
        
        users_data = [
            {
                "email": "admin@example.com",
                "username": "admin",
                "full_name": "System Administrator",
                "hashed_password": get_password_hash("admin123"),
                "is_active": True,
                "is_verified": True,
                "is_superuser": True,
            },
            {
                "email": "user@example.com", 
                "username": "user",
                "full_name": "Regular User",
                "hashed_password": get_password_hash("user123"),
                "is_active": True,
                "is_verified": True,
                "is_superuser": False,
            },
            {
                "email": "test@example.com",
                "username": "testuser",
                "full_name": "Test User",
                "hashed_password": get_password_hash("test123"),
                "is_active": True,
                "is_verified": False,
                "is_superuser": False,
            }
        ]
        
        created_users = []
        
        for user_data in users_data:
            # Check if user already exists
            from sqlmodel import select
            statement = select(User).where(User.email == user_data["email"])
            result = await self.session.exec(statement)
            existing_user = result.first()
            
            if not existing_user:
                user = User(**user_data)
                self.session.add(user)
                await self.session.flush()
                await self.session.refresh(user)
                
                # Create profile for user
                profile_data = {
                    "user_id": user.id,
                    "bio": f"This is the profile for {user.full_name}",
                    "location": "Jakarta, Indonesia",
                }
                
                profile = UserProfile(**profile_data)
                self.session.add(profile)
                
                created_users.append(user)
                logger.info(f"Created user: {user.email}")
            else:
                logger.info(f"User already exists: {user_data['email']}")
        
        await self.session.commit()
        logger.info(f"Seeded {len(created_users)} users")
        
        return created_users
    
    async def clear_all_data(self) -> None:
        """
        Clear all data from database.
        
        Warning: This will delete all data!
        """
        async with database_manager.get_async_session() as session:
            logger.warning("Clearing all database data...")
            
            # Delete in correct order to respect foreign keys
            from sqlmodel import delete
            
            await session.exec(delete(UserProfile))
            await session.exec(delete(User))
            
            await session.commit()
            logger.warning("All database data cleared")


# Global seeder instance
database_seeder = DatabaseSeeder()


# Convenience functions
async def seed_database() -> None:
    """Seed the database with initial data."""
    await database_seeder.seed_all()


async def clear_database() -> None:
    """Clear all data from database."""
    await database_seeder.clear_all_data()