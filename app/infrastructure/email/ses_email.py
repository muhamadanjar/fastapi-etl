"""
Amazon SES email service implementation.
"""

import logging
from typing import List, Optional, Dict, Any
import base64

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    SES_AVAILABLE = True
except ImportError:
    SES_AVAILABLE = False
    logger.warning("AWS SDK not available. Install with: pip install boto3")

from .base import EmailInterface, EmailMessage, EmailStatus, EmailProvider
from ...core.config import get_settings
from ...core.exceptions import EmailError

logger = logging.getLogger(__name__)


class SESEmailService(EmailInterface):
    """
    Amazon SES email service implementation.
    
    Features:
    - SES API integration
    - Raw email sending
    - Delivery tracking
    - Configuration sets
    - Reputation tracking
    """
    
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        region_name: str = 'us-east-1',
        configuration_set: Optional[str] = None,
        default_from_email: Optional[str] = None,
        default_from_name: Optional[str] = None,
    ):
        """
        Initialize SES email service.
        
        Args:
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            aws_session_token: AWS session token
            region_name: AWS region
            configuration_set: SES configuration set
            default_from_email: Default sender email
            default_from_name: Default sender name
        """
        if not SES_AVAILABLE:
            raise EmailError("AWS SDK (boto3) not installed")
        
        self.region_name = region_name
        self.configuration_set = configuration_set
        self.default_from_email = default_from_email
        self.default_from_name = default_from_name
        
        # Initialize SES client
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name
        )
        
        self.ses_client = session.client('ses')
        
        logger.info(f"SES service initialized for region {region_name}")
    
    def _create_raw_email(self, message: EmailMessage) -> str:
        """Create raw email string from EmailMessage."""
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders
        from email.utils import formataddr
        
        # Create multipart message
        mime_msg = MIMEMultipart('mixed')
        
        # Set headers
        mime_msg['Subject'] = message.subject
        mime_msg['From'] = formataddr((
            message.from_name or self.default_from_name or "",
            message.from_email or self.default_from_email
        ))
        mime_msg['To'] = ", ".join(message.to)
        
        if message.cc:
            mime_msg['Cc'] = ", ".join(message.cc)
        
        if message.reply_to:
            mime_msg['Reply-To'] = message.reply_to
        
        # Add custom headers
        for key, value in message.headers.items():
            mime_msg[key] = value
        
        # Add SES-specific headers
        if self.configuration_set:
            mime_msg['X-SES-CONFIGURATION-SET'] = self.configuration_set
        
        # Add tags as headers
        for i, tag in enumerate(message.tags):
            mime_msg[f'X-SES-MESSAGE-TAG-{i}'] = tag
        
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
        
        return mime_msg.as_string()
    
    async def send_email(self, message: EmailMessage) -> str:
        """Send email via SES."""
        try:
            message.mark_sending()
            message.provider = EmailProvider.SES
            
            # Create raw email
            raw_email = self._create_raw_email(message)
            
            # Prepare SES parameters
            ses_params = {
                'Source': message.from_email or self.default_from_email,
                'Destinations': message.get_all_recipients(),
                'RawMessage': {
                    'Data': raw_email
                }
            }
            
            # Add configuration set if configured
            if self.configuration_set:
                ses_params['ConfigurationSetName'] = self.configuration_set
            
            # Send email
            response = self.ses_client.send_raw_email(**ses_params)
            
            # Extract message ID
            message_id = response['MessageId']
            message.mark_sent(message_id)
            
            logger.info(f"Email sent via SES: {message.id} -> {message.to}")
            return message_id
            
        except (ClientError, BotoCoreError) as e:
            error_msg = f"SES send failed: {e}"
            logger.error(f"{error_msg} (message: {message.id})")
            message.mark_failed(error_msg)
            raise EmailError(error_msg)
        except Exception as e:
            error_msg = f"SES send failed: {e}"
            logger.error(f"{error_msg} (message: {message.id})")
            message.mark_failed(error_msg)
            raise EmailError(error_msg)
    
    async def send_bulk_email(self, messages: List[EmailMessage]) -> List[str]:
        """Send multiple emails via SES."""
        results = []
        
        for message in messages:
            try:
                message_id = await self.send_email(message)
                results.append(message_id)
            except Exception as e:
                logger.error(f"SES bulk email send failed for {message.id}: {e}")
        
        logger.info(f"SES bulk email sent: {len(results)}/{len(messages)} successful")
        return results
    
    async def get_delivery_status(self, message_id: str) -> EmailStatus:
        """Get delivery status from SES (limited without SNS)."""
        try:
            # SES doesn't provide direct delivery status API
            # This would require SNS notifications to be set up
            # For now, return SENT as we can't track without SNS
            return EmailStatus.SENT
        except Exception as e:
            logger.error(f"Failed to get SES delivery status: {e}")
            return EmailStatus.SENT
    
    async def health_check(self) -> bool:
        """Check SES service health."""
        try:
            # Try to get sending quota
            response = self.ses_client.get_send_quota()
            return 'Max24HourSend' in response
        except Exception as e:
            logger.error(f"SES health check failed: {e}")
            return False
    
    async def get_sending_statistics(self) -> Dict[str, Any]:
        """Get SES sending statistics."""
        try:
            # Get sending quota
            quota_response = self.ses_client.get_send_quota()
            
            # Get sending statistics
            stats_response = self.ses_client.get_send_statistics()
            
            return {
                'quota': {
                    'max_24_hour_send': quota_response['Max24HourSend'],
                    'max_send_rate': quota_response['MaxSendRate'],
                    'sent_last_24_hours': quota_response['SentLast24Hours']
                },
                'statistics': stats_response['SendDataPoints']
            }
        except Exception as e:
            logger.error(f"Failed to get SES statistics: {e}")
            return {}
    
    async def handle_webhook(self, payload: Dict[str, Any]) -> None:
        """Handle SES SNS webhook notifications."""
        # SES sends notifications via SNS
        message_type = payload.get('Type')
        
        if message_type == 'Notification':
            message_data = payload.get('Message', {})
            if isinstance(message_data, str):
                import json
                message_data = json.loads(message_data)
            
            event_type = message_data.get('eventType')
            message_id = message_data.get('mail', {}).get('messageId')
            
            logger.info(f"SES SNS notification: {event_type} for {message_id}")
            
            # Process different event types
            if event_type == 'delivery':
                logger.info(f"SES delivery confirmation for {message_id}")
            elif event_type == 'bounce':
                bounce_type = message_data.get('bounce', {}).get('bounceType')
                logger.warning(f"SES bounce: {bounce_type} for {message_id}")
            elif event_type == 'complaint':
                logger.warning(f"SES complaint for {message_id}")
            elif event_type == 'reject':
                reason = message_data.get('reject', {}).get('reason')
                logger.error(f"SES reject: {reason} for {message_id}")


class SESEmailManager:
    """SES email manager with lifecycle management."""
    
    def __init__(self):
        self._service: Optional[SESEmailService] = None
    
    async def initialize(self, **config) -> None:
        """Initialize SES service."""
        if not SES_AVAILABLE:
            raise EmailError("AWS SDK (boto3) not installed")
        
        settings = get_settings()
        
        ses_config = {
            'aws_access_key_id': config.get('aws_access_key_id', getattr(settings, 'AWS_ACCESS_KEY_ID', None)),
            'aws_secret_access_key': config.get('aws_secret_access_key', getattr(settings, 'AWS_SECRET_ACCESS_KEY', None)),
            'aws_session_token': config.get('aws_session_token', getattr(settings, 'AWS_SESSION_TOKEN', None)),
            'region_name': config.get('region_name', getattr(settings, 'AWS_DEFAULT_REGION', 'us-east-1')),
            'configuration_set': config.get('configuration_set', getattr(settings, 'SES_CONFIGURATION_SET', None)),
            'default_from_email': config.get('default_from_email', getattr(settings, 'DEFAULT_FROM_EMAIL', None)),
            'default_from_name': config.get('default_from_name', getattr(settings, 'DEFAULT_FROM_NAME', None)),
        }
        
        self._service = SESEmailService(**ses_config)
        logger.info("SES email service initialized")
    
    def get_service(self) -> Optional[SESEmailService]:
        """Get SES service instance."""
        return self._service
    
    async def health_check(self) -> bool:
        """Check SES service health."""
        if not self._service:
            return False
        return await self._service.health_check()


# Global SES email manager
ses_email_manager = SESEmailManager()