"""
SendGrid email service implementation.
"""

import logging
from typing import List, Optional, Dict, Any

try:
    import sendgrid
    from sendgrid.helpers.mail import (
        Mail, Email, To, Content, Attachment, FileContent, 
        FileName, FileType, Disposition, ContentId
    )
    SENDGRID_AVAILABLE = True
except ImportError:
    SENDGRID_AVAILABLE = False
    logger.warning("SendGrid not available. Install with: pip install sendgrid")

from .base import EmailInterface, EmailMessage, EmailStatus, EmailProvider
from ...core.config import get_settings
from ...core.exceptions import EmailError

logger = logging.getLogger(__name__)


class SendGridEmailService(EmailInterface):
    """
    SendGrid email service implementation.
    
    Features:
    - SendGrid API integration
    - Template support
    - Tracking and analytics
    - Webhook handling
    - Bulk email sending
    """
    
    def __init__(
        self,
        api_key: str,
        default_from_email: Optional[str] = None,
        default_from_name: Optional[str] = None,
        sandbox_mode: bool = False,
    ):
        """
        Initialize SendGrid email service.
        
        Args:
            api_key: SendGrid API key
            default_from_email: Default sender email
            default_from_name: Default sender name
            sandbox_mode: Enable sandbox mode for testing
        """
        if not SENDGRID_AVAILABLE:
            raise EmailError("SendGrid package not installed")
        
        self.api_key = api_key
        self.default_from_email = default_from_email
        self.default_from_name = default_from_name
        self.sandbox_mode = sandbox_mode
        
        self.client = sendgrid.SendGridAPIClient(api_key=api_key)
        
        logger.info("SendGrid service initialized")
    
    def _create_sendgrid_message(self, message: EmailMessage) -> Mail:
        """Create SendGrid Mail object from EmailMessage."""
        # From email
        from_email = Email(
            email=message.from_email or self.default_from_email,
            name=message.from_name or self.default_from_name
        )
        
        # To emails
        to_list = [To(email=email) for email in message.to]
        
        # Subject and content
        subject = message.subject
        
        # Content
        content_list = []
        if message.body:
            content_list.append(Content("text/plain", message.body))
        if message.html_body:
            content_list.append(Content("text/html", message.html_body))
        
        # Create mail object
        mail = Mail(
            from_email=from_email,
            to_emails=to_list,
            subject=subject,
            plain_text_content=message.body,
            html_content=message.html_body
        )
        
        # Add CC recipients
        if message.cc:
            for email in message.cc:
                mail.add_cc(Email(email))
        
        # Add BCC recipients
        if message.bcc:
            for email in message.bcc:
                mail.add_bcc(Email(email))
        
        # Add reply-to
        if message.reply_to:
            mail.reply_to = Email(message.reply_to)
        
        # Add custom headers
        for key, value in message.headers.items():
            mail.add_header(key, value)
        
        # Add attachments
        for attachment in message.attachments:
            sg_attachment = Attachment(
                FileContent(attachment.content.decode('latin-1')),
                FileName(attachment.filename),
                FileType(attachment.content_type),
                Disposition('attachment')
            )
            mail.add_attachment(sg_attachment)
        
        # Add inline attachments
        for attachment in message.inline_attachments:
            sg_attachment = Attachment(
                FileContent(attachment.content.decode('latin-1')),
                FileName(attachment.filename),
                FileType(attachment.content_type),
                Disposition('inline')
            )
            if attachment.content_id:
                sg_attachment.content_id = ContentId(attachment.content_id)
            mail.add_attachment(sg_attachment)
        
        # Add tracking settings
        if self.sandbox_mode:
            mail.mail_settings = {
                "sandbox_mode": {"enable": True}
            }
        
        # Add categories (tags)
        for tag in message.tags:
            mail.add_category(tag)
        
        # Add custom args (metadata)
        for key, value in message.metadata.items():
            mail.add_custom_arg(key, str(value))
        
        return mail
    
    async def send_email(self, message: EmailMessage) -> str:
        """Send email via SendGrid."""
        try:
            message.mark_sending()
            message.provider = EmailProvider.SENDGRID
            
            # Create SendGrid message
            mail = self._create_sendgrid_message(message)
            
            # Send email
            response = self.client.send(mail)
            
            if response.status_code in [200, 202]:
                # Extract message ID from headers
                message_id = response.headers.get('X-Message-Id', message.id)
                message.mark_sent(message_id)
                
                logger.info(f"SendGrid bulk email sent: {len(results)}/{len(messages)} successful")
        except:
            pass
        return results
    
    async def get_delivery_status(self, message_id: str) -> EmailStatus:
        """Get delivery status from SendGrid."""
        try:
            # Use SendGrid API to get message status
            response = self.client.get(f"/v3/messages/{message_id}")
            
            if response.status_code == 200:
                data = response.to_dict()
                status = data.get('status', 'unknown')
                
                # Map SendGrid status to EmailStatus
                status_mapping = {
                    'delivered': EmailStatus.DELIVERED,
                    'processed': EmailStatus.SENT,
                    'deferred': EmailStatus.QUEUED,
                    'bounce': EmailStatus.BOUNCED,
                    'dropped': EmailStatus.REJECTED,
                    'spam_report': EmailStatus.SPAM,
                    'unsubscribe': EmailStatus.UNSUBSCRIBED,
                }
                
                return status_mapping.get(status, EmailStatus.SENT)
            else:
                return EmailStatus.SENT
                
        except Exception as e:
            logger.error(f"Failed to get SendGrid delivery status: {e}")
            return EmailStatus.SENT
    
    async def health_check(self) -> bool:
        """Check SendGrid API connectivity."""
        try:
            response = self.client.get("/v3/user/profile")
            return response.status_code == 200
        except Exception as e:
            logger.error(f"SendGrid health check failed: {e}")
            return False
    
    async def handle_webhook(self, payload: Dict[str, Any]) -> None:
        """Handle SendGrid webhook events."""
        events = payload if isinstance(payload, list) else [payload]
        
        for event in events:
            event_type = event.get('event')
            message_id = event.get('sg_message_id')
            email = event.get('email')
            timestamp = event.get('timestamp')
            
            logger.info(f"SendGrid webhook: {event_type} for {message_id} ({email})")
            
            # Process different event types
            if event_type == 'delivered':
                # Update message status to delivered
                pass
            elif event_type in ['bounce', 'blocked', 'dropped']:
                # Handle bounces and blocks
                reason = event.get('reason', 'Unknown')
                logger.warning(f"SendGrid delivery failed: {event_type} - {reason}")
            elif event_type == 'spam_report':
                # Handle spam reports
                logger.warning(f"SendGrid spam report for {email}")
            elif event_type == 'unsubscribe':
                # Handle unsubscribes
                logger.info(f"SendGrid unsubscribe: {email}")


class SendGridEmailManager:
    """SendGrid email manager with lifecycle management."""
    
    def __init__(self):
        self._service: Optional[SendGridEmailService] = None
    
    async def initialize(self, **config) -> None:
        """Initialize SendGrid service."""
        if not SENDGRID_AVAILABLE:
            raise EmailError("SendGrid package not installed")
        
        settings = get_settings()
        
        sendgrid_config = {
            'api_key': config.get('api_key', getattr(settings, 'SENDGRID_API_KEY', None)),
            'default_from_email': config.get('default_from_email', getattr(settings, 'DEFAULT_FROM_EMAIL', None)),
            'default_from_name': config.get('default_from_name', getattr(settings, 'DEFAULT_FROM_NAME', None)),
            'sandbox_mode': config.get('sandbox_mode', getattr(settings, 'SENDGRID_SANDBOX_MODE', False)),
        }
        
        if not sendgrid_config['api_key']:
            raise EmailError("SendGrid API key not provided")
        
        self._service = SendGridEmailService(**sendgrid_config)
        logger.info("SendGrid email service initialized")
    
    def get_service(self) -> Optional[SendGridEmailService]:
        """Get SendGrid service instance."""
        return self._service
    
    async def health_check(self) -> bool:
        """Check SendGrid service health."""
        if not self._service:
            return False
        return await self._service.health_check()


# Global SendGrid email manager
sendgrid_email_manager = SendGridEmailManager()