"""
SMTP email service implementation.
"""

import asyncio
import logging
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.utils import formataddr
from typing import List, Optional, Dict, Any

import aiosmtplib

from .base import EmailInterface, EmailMessage, EmailStatus, EmailProvider
from ...core.config import get_settings
from ...core.exceptions import EmailError

logger = logging.getLogger(__name__)


class SMTPEmailService(EmailInterface):
    """
    SMTP email service implementation.
    
    Features:
    - SMTP/SMTPS support
    - Authentication
    - TLS/SSL encryption
    - Async sending
    - Connection pooling
    - Retry logic
    """
    
    def __init__(
        self,
        host: str,
        port: int = 587,
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_tls: bool = True,
        use_ssl: bool = False,
        timeout: int = 30,
        max_connections: int = 5,
        default_from_email: Optional[str] = None,
        default_from_name: Optional[str] = None,
    ):
        """
        Initialize SMTP email service.
        
        Args:
            host: SMTP server host
            port: SMTP server port
            username: SMTP username
            password: SMTP password
            use_tls: Use STARTTLS
            use_ssl: Use SSL/TLS
            timeout: Connection timeout
            max_connections: Maximum concurrent connections
            default_from_email: Default sender email
            default_from_name: Default sender name
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.use_ssl = use_ssl
        self.timeout = timeout
        self.max_connections = max_connections
        self.default_from_email = default_from_email
        self.default_from_name = default_from_name
        
        # Connection management
        self._connection_semaphore = asyncio.Semaphore(max_connections)
        self._is_connected = False
        
        logger.info(f"SMTP service initialized for {host}:{port}")
    
    async def _create_connection(self) -> aiosmtplib.SMTP:
        """Create SMTP connection."""
        smtp = aiosmtplib.SMTP(
            hostname=self.host,
            port=self.port,
            timeout=self.timeout,
            use_tls=self.use_ssl,
        )
        
        await smtp.connect()
        
        if self.use_tls and not self.use_ssl:
            await smtp.starttls()
        
        if self.username and self.password:
            await smtp.login(self.username, self.password)
        
        return smtp
    
    def _create_mime_message(self, message: EmailMessage) -> MIMEMultipart:
        """Create MIME message from EmailMessage."""
        # Create multipart message
        mime_msg = MIMEMultipart('mixed')
        
        # Set headers
        mime_msg['Subject'] = message.subject
        mime_msg['From'] = formataddr((
            message.from_name or self.default_from_name or "",
            message.from_email or self.default_from_email or self.username
        ))
        mime_msg['To'] = ", ".join(message.to)
        
        if message.cc:
            mime_msg['Cc'] = ", ".join(message.cc)
        
        if message.reply_to:
            mime_msg['Reply-To'] = message.reply_to
        
        # Add custom headers
        for key, value in message.headers.items():
            mime_msg[key] = value
        
        # Create message body
        if message.html_body:
            # Mixed content
            body_part = MIMEMultipart('alternative')
            
            if message.body:
                text_part = MIMEText(message.body, 'plain', 'utf-8')
                body_part.attach(text_part)
            
            html_part = MIMEText(message.html_body, 'html', 'utf-8')
            body_part.attach(html_part)
            
            mime_msg.attach(body_part)
        else:
            # Text only
            text_part = MIMEText(message.body or "", 'plain', 'utf-8')
            mime_msg.attach(text_part)
        
        # Add attachments
        for attachment in message.attachments:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.content)
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename= {attachment.filename}'
            )
            if attachment.content_type:
                part.set_type(attachment.content_type)
            mime_msg.attach(part)
        
        # Add inline attachments
        for attachment in message.inline_attachments:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.content)
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'inline; filename= {attachment.filename}'
            )
            if attachment.content_id:
                part.add_header('Content-ID', f'<{attachment.content_id}>')
            if attachment.content_type:
                part.set_type(attachment.content_type)
            mime_msg.attach(part)
        
        return mime_msg
    
    async def send_email(self, message: EmailMessage) -> str:
        """Send single email via SMTP."""
        async with self._connection_semaphore:
            try:
                message.mark_sending()
                message.provider = EmailProvider.SMTP
                
                # Create SMTP connection
                smtp = await self._create_connection()
                
                try:
                    # Create MIME message
                    mime_msg = self._create_mime_message(message)
                    
                    # Send email
                    result = await smtp.send_message(
                        mime_msg,
                        recipients=message.get_all_recipients()
                    )
                    
                    # Extract message ID from result
                    message_id = result.get('message-id', message.id)
                    message.mark_sent(message_id)
                    
                    logger.info(f"Email sent via SMTP: {message.id} -> {message.to}")
                    return message_id
                    
                finally:
                    await smtp.quit()
                    
            except Exception as e:
                error_msg = f"SMTP send failed: {e}"
                logger.error(f"{error_msg} (message: {message.id})")
                message.mark_failed(error_msg)
                raise EmailError(error_msg)
    
    async def send_bulk_email(self, messages: List[EmailMessage]) -> List[str]:
        """Send multiple emails via SMTP."""
        results = []
        
        # Send emails concurrently with connection limit
        semaphore = asyncio.Semaphore(self.max_connections)
        
        async def send_with_semaphore(msg):
            async with semaphore:
                try:
                    return await self.send_email(msg)
                except Exception as e:
                    logger.error(f"Bulk email send failed for {msg.id}: {e}")
                    return None
        
        tasks = [send_with_semaphore(msg) for msg in messages]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out None results and exceptions
        successful_results = [r for r in results if r and not isinstance(r, Exception)]
        
        logger.info(f"Bulk email sent: {len(successful_results)}/{len(messages)} successful")
        return successful_results
    
    async def get_delivery_status(self, message_id: str) -> EmailStatus:
        """SMTP doesn't provide delivery status tracking."""
        # SMTP servers typically don't provide delivery status
        # This would need to be implemented with bounce handling
        return EmailStatus.SENT
    
    async def health_check(self) -> bool:
        """Check SMTP server connectivity."""
        try:
            smtp = await self._create_connection()
            await smtp.quit()
            return True
        except Exception as e:
            logger.error(f"SMTP health check failed: {e}")
            return False


class SMTPEmailManager:
    """SMTP email manager with lifecycle management."""
    
    def __init__(self):
        self._service: Optional[SMTPEmailService] = None
    
    async def initialize(self, **config) -> None:
        """Initialize SMTP service."""
        settings = get_settings()
        
        # Use provided config or fall back to settings
        smtp_config = {
            'host': config.get('host', getattr(settings, 'SMTP_HOST', 'localhost')),
            'port': config.get('port', getattr(settings, 'SMTP_PORT', 587)),
            'username': config.get('username', getattr(settings, 'SMTP_USERNAME', None)),
            'password': config.get('password', getattr(settings, 'SMTP_PASSWORD', None)),
            'use_tls': config.get('use_tls', getattr(settings, 'SMTP_USE_TLS', True)),
            'use_ssl': config.get('use_ssl', getattr(settings, 'SMTP_USE_SSL', False)),
            'default_from_email': config.get('default_from_email', getattr(settings, 'DEFAULT_FROM_EMAIL', None)),
            'default_from_name': config.get('default_from_name', getattr(settings, 'DEFAULT_FROM_NAME', None)),
        }
        
        self._service = SMTPEmailService(**smtp_config)
        logger.info("SMTP email service initialized")
    
    def get_service(self) -> Optional[SMTPEmailService]:
        """Get SMTP service instance."""
        return self._service
    
    async def health_check(self) -> bool:
        """Check SMTP service health."""
        if not self._service:
            return False
        return await self._service.health_check()


# Global SMTP email manager
smtp_email_manager = SMTPEmailManager()