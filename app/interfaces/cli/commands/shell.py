"""
Command untuk menjalankan interactive shell
"""

import sys
import code
from pathlib import Path

# Import base command
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
from base import BaseCommand


class Command(BaseCommand):
    description = "Start an interactive Python shell with app context"
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--plain',
            action='store_true',
            help='Use plain Python shell instead of IPython'
        )
        parser.add_argument(
            '--ipython',
            action='store_true',
            help='Force use of IPython shell'
        )
    
    def handle(self, **kwargs):
        plain = kwargs.get('plain', False)
        force_ipython = kwargs.get('ipython', False)
        
        self.print_info("Starting interactive shell...")
        
        # Prepare context
        context = self._get_shell_context()
        
        # Start appropriate shell
        if force_ipython or (not plain and self._has_ipython()):
            self._start_ipython_shell(context)
        else:
            self._start_plain_shell(context)
    
    def _get_shell_context(self):
        """Get context variables for the shell"""
        context = {}

        try:
            # Import app modules
            from app.main import app
            from app.infrastructure.db.manager import database_manager
            from app.infrastructure.cache import cache_manager

            # Create db session
            db_session = database_manager.get_session()

            # Import models for convenience
            from app.infrastructure.db.models.auth import User
            from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob
            from app.infrastructure.db.models.etl_control.job_executions import JobExecution
            from app.infrastructure.db.models.raw_data.file_registry import FileRegistry

            context.update({
                'app': app,
                'db': db_session,
                'cache': cache_manager,
                'database_manager': database_manager,
                # Models
                'User': User,
                'EtlJob': EtlJob,
                'JobExecution': JobExecution,
                'FileRegistry': FileRegistry,
            })

            self.print_success("Database session loaded and auto-available as 'db'")

        except ImportError as e:
            self.print_warning(f"Could not import some modules: {e}")
            context.update({
                'app': 'FastAPI App Instance',
                'db': None,
                'cache': None,
            })

        return context
    
    def _has_ipython(self):
        """Check if IPython is available"""
        try:
            import IPython
            return True
        except ImportError:
            return False
    
    def _start_ipython_shell(self, context):
        """Start IPython shell"""
        try:
            import IPython  # noqa: F401

            self.print_info("Starting IPython shell...")
            self.print_info("Available variables:")
            for key, value in context.items():
                if value is None:
                    print(f"  {key}: {value}")
                elif isinstance(value, str):
                    print(f"  {key}: {value}")
                else:
                    print(f"  {key}: <{type(value).__name__}>")

            IPython.start_ipython(argv=[], user_ns=context, display_banner=True)

        except ImportError:
            self.print_error("IPython not available. Install it with: pip install ipython")
            self._start_plain_shell(context)
    
    def _start_plain_shell(self, context):
        """Start plain Python shell"""
        self.print_info("Starting Python shell...")
        self.print_info("Available variables:")
        for key, value in context.items():
            if value is None:
                print(f"  {key}: {value}")
            elif isinstance(value, str):
                print(f"  {key}: {value}")
            else:
                print(f"  {key}: <{type(value).__name__}>")

        shell_banner = """
╔════════════════════════════════════════════════════════════╗
║  ETL API Interactive Shell (Python)                        ║
╠════════════════════════════════════════════════════════════╣
║  Database Session: db                                      ║
║  Cache Manager: cache                                      ║
║  FastAPI App: app                                          ║
║                                                            ║
║  Useful commands:                                          ║
║    db.query(User).all()       - Query users               ║
║    cache.get('key')            - Get from cache           ║
║    cache.set('key', val, ttl)  - Set in cache            ║
║                                                            ║
║  Type 'exit()' or Ctrl+D to quit                           ║
╚════════════════════════════════════════════════════════════╝
"""

        console = code.InteractiveConsole(locals=context)
        console.interact(banner=shell_banner)