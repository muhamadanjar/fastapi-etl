#!/usr/bin/env python3
"""
FastAPI CLI Management Tool
Entry point untuk semua command CLI
"""

import sys
import os
import importlib
from pathlib import Path
from typing import Dict, Any
import argparse

# Tambahkan path app ke sys.path agar bisa import dari app
current_dir = Path(__file__).parent  # app/interfaces/cli
interfaces_dir = current_dir.parent  # app/interfaces
app_dir = interfaces_dir.parent      # app
project_root = app_dir.parent        # project root
sys.path.insert(0, str(project_root))

class CLIManager:
    def __init__(self):
        self.commands_dir = Path(__file__).parent / "commands"
        self.available_commands = self._discover_commands()
    
    def _discover_commands(self) -> Dict[str, Any]:
        """Discover semua command yang tersedia di folder commands"""
        commands = {}
        
        if not self.commands_dir.exists():
            return commands
            
        for file_path in self.commands_dir.glob("*.py"):
            if file_path.name.startswith("_"):
                continue
                
            module_name = file_path.stem
            try:
                # Import module command
                spec = importlib.util.spec_from_file_location(
                    f"commands.{module_name}", file_path
                )
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                # Cek apakah ada class Command
                if hasattr(module, 'Command'):
                    commands[module_name] = module.Command
                    
            except Exception as e:
                print(f"Warning: Could not load command '{module_name}': {e}")
                
        return commands
    
    def list_commands(self):
        """Tampilkan semua command yang tersedia"""
        print("Available commands:")
        print("=" * 40)
        
        if not self.available_commands:
            print("No commands found.")
            return
            
        for name, command_class in self.available_commands.items():
            command_instance = command_class()
            description = getattr(command_instance, 'description', 'No description')
            print(f"  {name:<20} {description}")
    
    def run_command(self, command_name: str, args: list):
        """Jalankan command tertentu"""
        if command_name not in self.available_commands:
            print(f"Unknown command: {command_name}")
            print("Use 'python app/interfaces/cli/main.py help' to see available commands.")
            sys.exit(1)
            
        command_class = self.available_commands[command_name]
        command_instance = command_class()
        
        try:
            command_instance.run(args)
        except Exception as e:
            print(f"Error running command '{command_name}': {e}")
            sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="FastAPI CLI Management Tool",
        add_help=False
    )
    
    # Argument untuk command
    parser.add_argument('command', nargs='?', help='Command to run')
    parser.add_argument('args', nargs='*', help='Arguments for the command')
    
    # Parse known args untuk menangani command-specific arguments
    args, unknown = parser.parse_known_args()
    
    # Gabungkan args yang tidak dikenal dengan args yang sudah di-parse
    if unknown:
        all_args = args.args + unknown
    else:
        all_args = args.args
    
    cli_manager = CLIManager()
    
    # Jika tidak ada command atau command adalah help
    if not args.command or args.command == 'help':
        if len(all_args) > 0:
            # Help untuk command tertentu
            command_name = all_args[0]
            if command_name in cli_manager.available_commands:
                command_class = cli_manager.available_commands[command_name]
                command_instance = command_class()
                if hasattr(command_instance, 'help'):
                    command_instance.help()
                else:
                    print(f"No help available for command: {command_name}")
            else:
                print(f"Unknown command: {command_name}")
        else:
            # Help umum
            print("FastAPI CLI Management Tool")
            print("Usage: python cli/main.py <command> [args...]")
            print()
            cli_manager.list_commands()
            print()
            print("Use 'python cli/main.py help <command>' for help on a specific command.")
        return
    
    # Jalankan command
    cli_manager.run_command(args.command, all_args)

if __name__ == "__main__":
    main()