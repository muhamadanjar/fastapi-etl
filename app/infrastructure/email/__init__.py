"""
Email infrastructure package.

This module provides comprehensive email functionality including SMTP,
template rendering, queue management, and multiple email providers.
"""

from .base import (
    EmailInterface,
    EmailMessage,
    EmailTemplate,
    EmailAttachment,
    EmailProvider,
    EmailStatus,
    EmailPriority,
)
from .smtp_email import SMTPEmailService, smtp_email_manager
from .sendgrid_email import SendGridEmailService, sendgrid_email_manager
from .ses_email import SESEmailService, ses_email_manager
from .template_engine import (
    TemplateEngine,
    JinjaTemplateEngine,
    template_manager,
)
from .queue import EmailQueueManager, email_queue_manager
from .manager import EmailManager, email_manager
from .decorators import (
    send_email_async,
    email_template,
    retry_on_failure,
)
from .utils import (
    EmailValidator,
    EmailFormatter,
    parse_email_address,
    sanitize_email_content,
)

__version__ = "1.0.0"

__all__ = [
    # Core interfaces
    "EmailInterface",
    "EmailMessage",
    "EmailTemplate", 
    "EmailAttachment",
    "EmailProvider",
    "EmailStatus",
    "EmailPriority",
    
    # Implementations
    "SMTPEmailService",
    "smtp_email_manager",
    "SendGridEmailService", 
    "sendgrid_email_manager",
    "SESEmailService",
    "ses_email_manager",
    
    # Template engine
    "TemplateEngine",
    "JinjaTemplateEngine",
    "template_manager",
    
    # Queue management
    "EmailQueueManager",
    "email_queue_manager",
    
    # Manager
    "EmailManager",
    "email_manager",
    
    # Decorators
    "send_email_async",
    "email_template",
    "retry_on_failure",
    
    # Utilities
    "EmailValidator",
    "EmailFormatter", 
    "parse_email_address",
    "sanitize_email_content",
]

# Convenience functions
async def send_email(
    to: str,
    subject: str,
    body: str,
    **kwargs
) -> str:
    """Send email using default provider."""
    return await email_manager.send_email(to, subject, body, **kwargs)

async def send_template_email(
    to: str,
    template_name: str,
    context: dict,
    **kwargs
) -> str:
    """Send templated email."""
    return await email_manager.send_template_email(to, template_name, context, **kwargs)
