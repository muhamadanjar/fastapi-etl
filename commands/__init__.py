"""
Custom Management Commands

This package contains custom management commands similar to Django's management commands.
Each command should inherit from BaseCommand and implement the handle() method.
"""

from commands.base import BaseCommand

__all__ = ['BaseCommand']
