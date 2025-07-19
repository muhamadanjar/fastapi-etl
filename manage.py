#!/usr/bin/env python3
"""
Management script untuk menjalankan CLI commands
Letakkan file ini di root project Anda
"""

import sys
import os
from pathlib import Path

# Tambahkan path ke sys.path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import dan jalankan CLI
if __name__ == "__main__":
    # Import main dari cli
    from app.interfaces.cli.main import main
    
    # Jalankan CLI
    main()