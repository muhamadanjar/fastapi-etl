"""
Base interfaces and data structures for email system.
"""

import asyncio
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, List, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import uuid
from pathlib import Path


class EmailProvider(Enum):
    """Email provider types."""
    SMTP = "smtp"
    SENDGRID = "sendgrid"
    SES = "ses"
    MAILGUN = "mailgun"
    POSTMARK = "postmark"


class EmailStatus(Enum):
    """Email delivery status."""
    PENDING = "pending"
    QUEUED = "queued"
    SENDING = "sending"
    SENT = "sent"
    DELIVERED = "delivered"
    FAILED = "failed"
    BOUNCED = "bounced"
    REJECTED = "rejected"
    SPAM = "spam"
    UNSUBSCRIBED = "unsubscribed"


class EmailPriority(Enum):
    """Email priority levels."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4


@dataclass
class EmailAttachment:
    """Email attachment data structure."""
    filename: str
    content: bytes
    content_type: str = "application/octet-stream"
    content_id: Optional[str] = None  # For inline attachments
    
    @classmethod
    def from_file(cls, file_path: Union[str, Path], content_type: Optional[str] = None) -> "EmailAttachment":
        """Create attachment from file."""
        import mimetypes
        
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Attachment file not found: {file_path}")
        
        if content_type is None:
            content_type, _ = mimetypes.guess_type(str(path))
            content_type = content_type or "application/octet-stream"
        
        with open(path, 'rb') as f:
            content = f.read()
        
        return cls(
            filename=path.name,
            content=content,
            content_type=content_type
        )


@dataclass
class EmailTemplate:
    """Email template data structure."""
    name: str
    subject_template: str
    body_template: str
    template_type: str = "html"  # html, text, mixed
    variables: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None
    
    def render_subject(self, context: Dict[str, Any], engine: Any) -> str:
        """Render subject with context."""
        return engine.render_string(self.subject_template, context)
    
    def render_body(self, context: Dict[str, Any], engine: Any) -> str:
        """Render body with context."""
        return engine.render_string(self.body_template, context)


@dataclass
class EmailMessage:
    """Comprehensive email message data structure."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    to: List[str] = field(default_factory=list)
    cc: List[str] = field(default_factory=list)
    bcc: List[str] = field(default_factory=list)
    subject: str = ""
    body: str = ""
    html_body: Optional[str] = None
    
    # Sender info
    from_email: Optional[str] = None
    from_name: Optional[str] = None
    reply_to: Optional[str] = None
    
    # Attachments and content
    attachments: List[EmailAttachment] = field(default_factory=list)
    inline_attachments: List[EmailAttachment] = field(default_factory=list)
    
    # Metadata
    headers: Dict[str, str] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Delivery options
    priority: EmailPriority = EmailPriority.NORMAL
    send_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    
    # Tracking
    template_name: Optional[str] = None
    template_context: Dict[str, Any] = field(default_factory=dict)
    provider: Optional[EmailProvider] = None
    
    # Status tracking
    status: EmailStatus = EmailStatus.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    sent_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    
    # Error handling
    retry_count: int = 0
    max_retries: int = 3
    error_message: Optional[str] = None
    
    # External IDs
    provider_message_id: Optional[str] = None
    tracking_id: Optional[str] = None
    
    def __post_init__(self):
        """Post-initialization processing."""
        # Ensure to is a list
        if isinstance(self.to, str):
            self.to = [self.to]
        
        # Set default expiration (30 days)
        if self.expires_at is None:
            self.expires_at = datetime.utcnow() + timedelta(days=30)
    
    def add_recipient(self, email: str, recipient_type: str = "to") -> None:
        """Add recipient to email."""
        if recipient_type == "to":
            self.to.append(email)
        elif recipient_type == "cc":
            self.cc.append(email)
        elif recipient_type == "bcc":
            self.bcc.append(email)
    
    def add_attachment(self, attachment: EmailAttachment, inline: bool = False) -> None:
        """Add attachment to email."""
        if inline:
            self.inline_attachments.append(attachment)
        else:
            self.attachments.append(attachment)
    
    def add_tag(self, tag: str) -> None:
        """Add tag to email."""
        if tag not in self.tags:
            self.tags.append(tag)
    
    def is_expired(self) -> bool:
        """Check if email has expired."""
        return self.expires_at and datetime.utcnow() > self.expires_at
    
    def can_retry(self) -> bool:
        """Check if email can be retried."""
        return (
            self.retry_count < self.max_retries and
            not self.is_expired() and
            self.status in [EmailStatus.FAILED, EmailStatus.PENDING]
        )
    
    def mark_sending(self) -> None:
        """Mark email as being sent."""
        self.status = EmailStatus.SENDING
        self.sent_at = datetime.utcnow()
    
    def mark_sent(self, provider_message_id: Optional[str] = None) -> None:
        """Mark email as sent."""
        self.status = EmailStatus.SENT
        self.sent_at = datetime.utcnow()
        if provider_message_id:
            self.provider_message_id = provider_message_id
    
    def mark_failed(self, error: str) -> None:
        """Mark email as failed."""
        self.status = EmailStatus.FAILED
        self.error_message = error
    
    def mark_retry(self) -> None:
        """Mark email for retry."""
        self.retry_count += 1
        self.status = EmailStatus.PENDING
    
    def get_all_recipients(self) -> List[str]:
        """Get all recipients (to, cc, bcc)."""
        return self.to + self.cc + self.bcc
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "to": self.to,
            "cc": self.cc,
            "bcc": self.bcc,
            "subject": self.subject,
            "body": self.body,
            "html_body": self.html_body,
            "from_email": self.from_email,
            "from_name": self.from_name,
            "reply_to": self.reply_to,
            "headers": self.headers,
            "tags": self.tags,
            "metadata": self.metadata,
            "priority": self.priority.value,
            "send_at": self.send_at.isoformat() if self.send_at else None,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "template_name": self.template_name,
            "template_context": self.template_context,
            "provider": self.provider.value if self.provider else None,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "sent_at": self.sent_at.isoformat() if self.sent_at else None,
            "delivered_at": self.delivered_at.isoformat() if self.delivered_at else None,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "error_message": self.error_message,
            "provider_message_id": self.provider_message_id,
            "tracking_id": self.tracking_id,
        }


class EmailInterface(ABC):
    """Abstract email service interface."""
    
    @abstractmethod
    async def send_email(self, message: EmailMessage) -> str:
        """
        Send email message.
        
        Args:
            message: Email message to send
            
        Returns:
            Provider message ID
        """
        pass
    
    @abstractmethod
    async def send_bulk_email(self, messages: List[EmailMessage]) -> List[str]:
        """
        Send multiple emails.
        
        Args:
            messages: List of email messages
            
        Returns:
            List of provider message IDs
        """
        pass
    
    @abstractmethod
    async def get_delivery_status(self, message_id: str) -> EmailStatus:
        """
        Get delivery status of an email.
        
        Args:
            message_id: Provider message ID
            
        Returns:
            Current email status
        """
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """Check email service health."""
        pass
    
    # Optional webhook handling
    async def handle_webhook(self, payload: Dict[str, Any]) -> None:
        """Handle delivery status webhook."""
        pass


class TemplateEngineInterface(ABC):
    """Abstract template engine interface."""
    
    @abstractmethod
    async def render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        """Render template with context."""
        pass
    
    @abstractmethod
    async def render_string(self, template_string: str, context: Dict[str, Any]) -> str:
        """Render template string with context."""
        pass
    
    @abstractmethod
    async def load_template(self, template_name: str) -> EmailTemplate:
        """Load template by name."""
        pass
    
    @abstractmethod
    async def save_template(self, template: EmailTemplate) -> bool:
        """Save template."""
        pass
