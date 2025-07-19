"""
Base Command Class untuk semua CLI commands
"""

from abc import ABC, abstractmethod
from typing import List
import argparse


class BaseCommand(ABC):
    """Base class untuk semua command"""
    
    description = "No description provided"
    
    def __init__(self):
        self.parser = self._create_parser()
    
    def _create_parser(self) -> argparse.ArgumentParser:
        """Create argument parser untuk command ini"""
        parser = argparse.ArgumentParser(
            description=self.description,
            add_help=False
        )
        self.add_arguments(parser)
        return parser
    
    def add_arguments(self, parser: argparse.ArgumentParser):
        """Override method ini untuk menambahkan custom arguments"""
        pass
    
    @abstractmethod
    def handle(self, *args, **kwargs):
        """Method utama yang harus di-implement oleh setiap command"""
        pass
    
    def run(self, args: List[str]):
        """Parse arguments dan jalankan command"""
        # Parse arguments
        parsed_args = self.parser.parse_args(args)
        
        # Convert ke dictionary untuk kemudahan
        kwargs = vars(parsed_args)
        
        # Jalankan command
        self.handle(**kwargs)
    
    def help(self):
        """Tampilkan help untuk command ini"""
        self.parser.print_help()
    
    def print_success(self, message: str):
        """Print success message dengan warna hijau"""
        print(f"\033[92m✓ {message}\033[0m")
    
    def print_error(self, message: str):
        """Print error message dengan warna merah"""
        print(f"\033[91m✗ {message}\033[0m")
    
    def print_warning(self, message: str):
        """Print warning message dengan warna kuning"""
        print(f"\033[93m⚠ {message}\033[0m")
    
    def print_info(self, message: str):
        """Print info message dengan warna biru"""
        print(f"\033[94mℹ {message}\033[0m")