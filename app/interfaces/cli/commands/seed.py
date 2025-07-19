"""
Command untuk seeding database dengan data dummy
"""

import sys
from pathlib import Path

# Import base command
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
from base import BaseCommand

# Setup path untuk import dari app
project_root = current_dir.parent.parent.parent.parent
sys.path.insert(0, str(project_root))


class Command(BaseCommand):
    description = "Seed database with dummy data"
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--flush',
            action='store_true',
            help='Flush existing data before seeding'
        )
        parser.add_argument(
            '--count',
            type=int,
            default=10,
            help='Number of records to create (default: 10)'
        )
        parser.add_argument(
            '--model',
            help='Specific model to seed (e.g., users, posts)'
        )
    
    def handle(self, **kwargs):
        flush = kwargs.get('flush', False)
        count = kwargs.get('count', 10)
        model = kwargs.get('model')
        
        self.print_info("Starting database seeding...")
        
        if flush:
            self._flush_data()
        
        if model:
            self._seed_specific_model(model, count)
        else:
            self._seed_all_models(count)
        
        self.print_success("Database seeding completed!")
    
    def _flush_data(self):
        """Flush existing data"""
        self.print_warning("Flushing existing data...")
        
        try:
            # Contoh flush data - sesuaikan dengan setup database Anda
            # from app.database import get_db
            # from app.models import User, Post
            
            # db = next(get_db())
            # db.query(Post).delete()
            # db.query(User).delete()
            # db.commit()
            
            self.print_info("Existing data flushed")
            
        except Exception as e:
            self.print_error(f"Error flushing data: {e}")
            raise
    
    def _seed_all_models(self, count):
        """Seed all models"""
        self.print_info("Seeding all models...")
        
        self._seed_users(count)
        self._seed_posts(count)
        # Tambahkan model lain sesuai kebutuhan
    
    def _seed_specific_model(self, model, count):
        """Seed specific model"""
        self.print_info(f"Seeding {model} model...")
        
        if model == 'users':
            self._seed_users(count)
        elif model == 'posts':
            self._seed_posts(count)
        else:
            self.print_error(f"Unknown model: {model}")
            return
    
    def _seed_users(self, count):
        """Seed users table"""
        self.print_info(f"Creating {count} users...")
        
        try:
            # Contoh seeding users - sesuaikan dengan model Anda
            # from app.database import get_db
            # from app.models import User
            # from app.auth import hash_password
            # import faker
            
            # fake = faker.Faker()
            # db = next(get_db())
            
            # for i in range(count):
            #     user = User(
            #         username=fake.user_name(),
            #         email=fake.email(),
            #         password=hash_password("password123"),
            #         is_active=True
            #     )
            #     db.add(user)
            
            # db.commit()
            
            # Placeholder implementation
            for i in range(count):
                self.print_info(f"  Creating user {i+1}/{count}")
            
            self.print_success(f"Created {count} users")
            
        except Exception as e:
            self.print_error(f"Error seeding users: {e}")
            raise
    
    def _seed_posts(self, count):
        """Seed posts table"""
        self.print_info(f"Creating {count} posts...")
        
        try:
            # Contoh seeding posts - sesuaikan dengan model Anda
            # from app.database import get_db
            # from app.models import User, Post
            # import faker
            # import random
            
            # fake = faker.Faker()
            # db = next(get_db())
            
            # # Get existing users
            # users = db.query(User).all()
            # if not users:
            #     self.print_error("No users found. Please seed users first.")
            #     return
            
            # for i in range(count):
            #     post = Post(
            #         title=fake.sentence(),
            #         content=fake.text(),
            #         author_id=random.choice(users).id
            #     )
            #     db.add(post)
            
            # db.commit()
            
            # Placeholder implementation
            for i in range(count):
                self.print_info(f"  Creating post {i+1}/{count}")
            
            self.print_success(f"Created {count} posts")
            
        except Exception as e:
            self.print_error(f"Error seeding posts: {e}")
            raise