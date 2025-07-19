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
            # Import your app modules here
            # from app.main import app
            # from app.database import get_db
            # from app.models import User, Post
            
            # context.update({
            #     'app': app,
            #     'get_db': get_db,
            #     'User': User,
            #     'Post': Post,
            # })
            
            # Placeholder context
            context.update({
                'app': 'FastAPI App Instance',
                'db': 'Database Session',
                'models': 'Your Models Here'
            })
            
        except ImportError as e:
            self.print_warning(f"Could not import some modules: {e}")
        
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
            import IPython
            
            self.print_info("Starting IPython shell...")
            self.print_info("Available variables:")
            for key, value in context.items():
                print(f"  {key}: {value}")
            
            IPython.start_ipython(argv=[], user_ns=context)
            
        except ImportError:
            self.print_error("IPython not available. Install it with: pip install ipython")
            self._start_plain_shell(context)
    
    def _start_plain_shell(self, context):
        """Start plain Python shell"""
        self.print_info("Starting Python shell...")
        self.print_info("Available variables:")
        for key, value in context.items():
            print(f"  {key}: {value}")
        
        # Banner
        banner = """
FastAPI Interactive Shell
=========================
Your app context has been loaded.
Type 'help()' for Python help, or 'exit()' to quit.
"""
        
        # Start interactive console
        console = code.InteractiveConsole(locals=context)
        console.interact(banner=banner)