"""
Utility functions and helpers for email system.
"""

import re
import html
import hashlib
import base64
import mimetypes
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders
from urllib.parse import urlparse, parse_qs
import uuid

from .base import EmailMessage, EmailAttachment, EmailStatus, EmailPriority, EmailProvider

logger = logging.getLogger(__name__)


class EmailValidator:
    """Email validation utilities."""
    
    # RFC 5322 compliant email regex (simplified)
    EMAIL_REGEX = re.compile(
        r'^[a-zA-Z0-9.!#$%&\'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$'
    )
    
    @classmethod
    def is_valid_email(cls, email: str) -> bool:
        """Validate email address format."""
        if not email or not isinstance(email, str):
            return False
        
        email = email.strip()
        if len(email) > 254:  # RFC 5321 limit
            return False
        
        return bool(cls.EMAIL_REGEX.match(email))
    
    @classmethod
    def validate_email_list(cls, emails: List[str]) -> Tuple[List[str], List[str]]:
        """
        Validate list of email addresses.
        
        Returns:
            Tuple of (valid_emails, invalid_emails)
        """
        valid = []
        invalid = []
        
        for email in emails:
            if cls.is_valid_email(email):
                valid.append(email.strip().lower())
            else:
                invalid.append(email)
        
        return valid, invalid
    
    @classmethod
    def normalize_email(cls, email: str) -> str:
        """Normalize email address."""
        if not cls.is_valid_email(email):
            raise ValueError(f"Invalid email address: {email}")
        
        return email.strip().lower()


class EmailTemplateProcessor:
    """Email template processing utilities."""
    
    @staticmethod
    def extract_variables(template: str) -> List[str]:
        """Extract template variables from template string."""
        # Find variables in format {{variable}} or {variable}
        pattern = r'\{\{?\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}?\}'
        matches = re.findall(pattern, template)
        return list(set(matches))
    
    @staticmethod
    def render_simple_template(template: str, context: Dict[str, Any]) -> str:
        """
        Render simple template with context.
        Supports {{variable}} and {variable} syntax.
        """
        result = template
        
        for key, value in context.items():
            # Handle both {{key}} and {key} formats
            patterns = [
                f'{{{{{key}}}}}',  # {{key}}
                f'{{{key}}}'       # {key}
            ]
            
            for pattern in patterns:
                result = result.replace(pattern, str(value))
        
        return result
    
    @staticmethod
    def validate_template_context(template: str, context: Dict[str, Any]) -> List[str]:
        """
        Validate that all required template variables are provided.
        
        Returns:
            List of missing variables
        """
        required_vars = EmailTemplateProcessor.extract_variables(template)
        missing_vars = []
        
        for var in required_vars:
            if var not in context:
                missing_vars.append(var)
        
        return missing_vars


class EmailContentProcessor:
    """Email content processing utilities."""
    
    @staticmethod
    def html_to_text(html_content: str) -> str:
        """Convert HTML content to plain text."""
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Get text and clean up
            text = soup.get_text()
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = ' '.join(chunk for chunk in chunks if chunk)
            
            return text
        except ImportError:
            # Fallback: simple HTML tag removal
            text = re.sub(r'<[^>]+>', '', html_content)
            text = html.unescape(text)
            return ' '.join(text.split())
    
    @staticmethod
    def text_to_html(text_content: str) -> str:
        """Convert plain text to HTML."""
        # Escape HTML characters
        html_content = html.escape(text_content)
        
        # Convert line breaks to <br>
        html_content = html_content.replace('\n', '<br>')
        
        # Convert URLs to links
        url_pattern = re.compile(
            r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        )
        html_content = url_pattern.sub(r'<a href="\g<0>">\g<0></a>', html_content)
        
        return f'<html><body>{html_content}</body></html>'
    
    @staticmethod
    def extract_links(content: str) -> List[str]:
        """Extract all links from content."""
        # Extract from HTML
        html_links = re.findall(r'href=[\'"]?([^\'" >]+)', content)
        
        # Extract plain URLs
        url_pattern = re.compile(
            r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        )
        plain_links = url_pattern.findall(content)
        
        return list(set(html_links + plain_links))
    
    @staticmethod
    def add_tracking_pixels(html_content: str, tracking_id: str, base_url: str) -> str:
        """Add tracking pixel to HTML content."""
        tracking_pixel = f'<img src="{base_url}/track/open/{tracking_id}" width="1" height="1" style="display:none;" alt="">'
        
        # Try to insert before </body>
        if '</body>' in html_content:
            html_content = html_content.replace('</body>', f'{tracking_pixel}</body>')
        else:
            html_content += tracking_pixel
        
        return html_content
    
    @staticmethod
    def add_unsubscribe_link(html_content: str, unsubscribe_url: str) -> str:
        """Add unsubscribe link to HTML content."""
        unsubscribe_html = f'''
        <div style="margin-top: 20px; padding: 10px; border-top: 1px solid #ccc; font-size: 12px; color: #666;">
            <p>If you no longer wish to receive these emails, you can <a href="{unsubscribe_url}">unsubscribe here</a>.</p>
        </div>
        '''
        
        # Try to insert before </body>
        if '</body>' in html_content:
            html_content = html_content.replace('</body>', f'{unsubscribe_html}</body>')
        else:
            html_content += unsubscribe_html
        
        return html_content


class MimeMessageBuilder:
    """MIME message builder utility."""
    
    @staticmethod
    def build_mime_message(email_message: EmailMessage) -> MIMEMultipart:
        """Build MIME message from EmailMessage."""
        # Create base message
        if email_message.html_body and email_message.body:
            msg = MIMEMultipart('alternative')
        elif email_message.attachments or email_message.inline_attachments:
            msg = MIMEMultipart('mixed')
        else:
            msg = MIMEMultipart()
        
        # Set headers
        msg['Subject'] = email_message.subject
        msg['From'] = f"{email_message.from_name} <{email_message.from_email}>" if email_message.from_name else email_message.from_email
        msg['To'] = ', '.join(email_message.to)
        
        if email_message.cc:
            msg['Cc'] = ', '.join(email_message.cc)
        
        if email_message.reply_to:
            msg['Reply-To'] = email_message.reply_to
        
        # Add custom headers
        for key, value in email_message.headers.items():
            msg[key] = value
        
        # Add priority header
        if email_message.priority != EmailPriority.NORMAL:
            priority_map = {
                EmailPriority.LOW: '5',
                EmailPriority.NORMAL: '3',
                EmailPriority.HIGH: '2',
                EmailPriority.URGENT: '1'
            }
            msg['X-Priority'] = priority_map[email_message.priority]
        
        # Create body content
        if email_message.html_body and email_message.body:
            # Create alternative container
            body_container = MIMEMultipart('alternative')
            
            # Add plain text
            text_part = MIMEText(email_message.body, 'plain', 'utf-8')
            body_container.attach(text_part)
            
            # Add HTML
            html_part = MIMEText(email_message.html_body, 'html', 'utf-8')
            body_container.attach(html_part)
            
            msg.attach(body_container)
        elif email_message.html_body:
            html_part = MIMEText(email_message.html_body, 'html', 'utf-8')
            msg.attach(html_part)
        elif email_message.body:
            text_part = MIMEText(email_message.body, 'plain', 'utf-8')
            msg.attach(text_part)
        
        # Add inline attachments
        for attachment in email_message.inline_attachments:
            MimeMessageBuilder._add_attachment(msg, attachment, inline=True)
        
        # Add regular attachments
        for attachment in email_message.attachments:
            MimeMessageBuilder._add_attachment(msg, attachment, inline=False)
        
        return msg
    
    @staticmethod
    def _add_attachment(msg: MIMEMultipart, attachment: EmailAttachment, inline: bool = False):
        """Add attachment to MIME message."""
        # Determine if it's an image for inline embedding
        is_image = attachment.content_type.startswith('image/')
        
        if inline and is_image:
            # Add as inline image
            img = MIMEImage(attachment.content)
            img.add_header('Content-Disposition', 'inline', filename=attachment.filename)
            if attachment.content_id:
                img.add_header('Content-ID', f'<{attachment.content_id}>')
            msg.attach(img)
        else:
            # Add as regular attachment
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.content)
            encoders.encode_base64(part)
            
            disposition = 'inline' if inline else 'attachment'
            part.add_header(
                'Content-Disposition',
                f'{disposition}; filename= {attachment.filename}'
            )
            part.add_header('Content-Type', attachment.content_type)
            
            if attachment.content_id:
                part.add_header('Content-ID', f'<{attachment.content_id}>')
            
            msg.attach(part)


class EmailHasher:
    """Email hashing and deduplication utilities."""
    
    @staticmethod
    def generate_message_hash(email_message: EmailMessage) -> str:
        """Generate unique hash for email message."""
        # Create hash from key components
        hash_components = [
            ','.join(sorted(email_message.to)),
            ','.join(sorted(email_message.cc)),
            ','.join(sorted(email_message.bcc)),
            email_message.subject or '',
            email_message.body or '',
            email_message.html_body or '',
        ]
        
        # Add attachment hashes
        for attachment in email_message.attachments:
            hash_components.append(f"{attachment.filename}:{len(attachment.content)}")
        
        combined = '|'.join(hash_components)
        return hashlib.sha256(combined.encode('utf-8')).hexdigest()
    
    @staticmethod
    def generate_content_hash(content: str) -> str:
        """Generate hash for content."""
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    @staticmethod
    def generate_tracking_id() -> str:
        """Generate unique tracking ID."""
        return str(uuid.uuid4())


class EmailStatistics:
    """Email statistics and analytics utilities."""
    
    @staticmethod
    def calculate_bounce_rate(sent_count: int, bounced_count: int) -> float:
        """Calculate bounce rate percentage."""
        if sent_count == 0:
            return 0.0
        return (bounced_count / sent_count) * 100
    
    @staticmethod
    def calculate_delivery_rate(sent_count: int, delivered_count: int) -> float:
        """Calculate delivery rate percentage."""
        if sent_count == 0:
            return 0.0
        return (delivered_count / sent_count) * 100
    
    @staticmethod
    def calculate_open_rate(delivered_count: int, opened_count: int) -> float:
        """Calculate open rate percentage."""
        if delivered_count == 0:
            return 0.0
        return (opened_count / delivered_count) * 100
    
    @staticmethod
    def calculate_click_rate(delivered_count: int, clicked_count: int) -> float:
        """Calculate click rate percentage."""
        if delivered_count == 0:
            return 0.0
        return (clicked_count / delivered_count) * 100


class EmailFormatter:
    """Email formatting utilities."""
    
    @staticmethod
    def format_email_address(email: str, name: Optional[str] = None) -> str:
        """Format email address with optional name."""
        if name:
            return f"{name} <{email}>"
        return email
    
    @staticmethod
    def parse_email_address(formatted_email: str) -> Tuple[str, Optional[str]]:
        """Parse formatted email address."""
        # Pattern for "Name <email@domain.com>"
        pattern = r'^(.+?)\s*<([^>]+)>'
        match = re.match(pattern, formatted_email.strip())
        
        if match:
            name = match.group(1).strip().strip('"\'')
            email = match.group(2).strip()
            return email, name
        else:
            return formatted_email.strip(), None
    
    @staticmethod
    def format_subject_with_prefix(subject: str, prefix: str) -> str:
        """Add prefix to subject if not already present."""
        if not subject.startswith(prefix):
            return f"{prefix} {subject}"
        return subject
    
    @staticmethod
    def truncate_content(content: str, max_length: int = 100) -> str:
        """Truncate content with ellipsis."""
        if len(content) <= max_length:
            return content
        return content[:max_length - 3] + "..."


class EmailUrlBuilder:
    """URL building utilities for email tracking."""
    
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
    
    def build_tracking_url(self, tracking_id: str, event_type: str) -> str:
        """Build tracking URL for email events."""
        return f"{self.base_url}/track/{event_type}/{tracking_id}"
    
    def build_unsubscribe_url(self, email: str, token: str) -> str:
        """Build unsubscribe URL."""
        encoded_email = base64.urlsafe_b64encode(email.encode()).decode()
        return f"{self.base_url}/unsubscribe/{encoded_email}/{token}"
    
    def build_click_tracking_url(self, original_url: str, tracking_id: str) -> str:
        """Build click tracking URL."""
        encoded_url = base64.urlsafe_b64encode(original_url.encode()).decode()
        return f"{self.base_url}/track/click/{tracking_id}?url={encoded_url}"


class EmailConfigValidator:
    """Email configuration validation utilities."""
    
    @staticmethod
    def validate_smtp_config(config: Dict[str, Any]) -> List[str]:
        """Validate SMTP configuration."""
        errors = []
        required_fields = ['host', 'port', 'username', 'password']
        
        for field in required_fields:
            if field not in config or not config[field]:
                errors.append(f"Missing required SMTP field: {field}")
        
        if 'port' in config:
            try:
                port = int(config['port'])
                if port < 1 or port > 65535:
                    errors.append("SMTP port must be between 1 and 65535")
            except (ValueError, TypeError):
                errors.append("SMTP port must be a valid integer")
        
        return errors
    
    @staticmethod
    def validate_provider_config(provider: EmailProvider, config: Dict[str, Any]) -> List[str]:
        """Validate provider-specific configuration."""
        errors = []
        
        if provider == EmailProvider.SMTP:
            errors.extend(EmailConfigValidator.validate_smtp_config(config))
        elif provider == EmailProvider.SENDGRID:
            if 'api_key' not in config or not config['api_key']:
                errors.append("SendGrid API key is required")
        elif provider == EmailProvider.SES:
            required_fields = ['aws_access_key_id', 'aws_secret_access_key', 'region']
            for field in required_fields:
                if field not in config or not config[field]:
                    errors.append(f"Missing required AWS SES field: {field}")
        elif provider == EmailProvider.MAILGUN:
            required_fields = ['api_key', 'domain']
            for field in required_fields:
                if field not in config or not config[field]:
                    errors.append(f"Missing required Mailgun field: {field}")
        
        return errors


class EmailSanitizer:
    """Email content sanitization utilities."""
    
    @staticmethod
    def sanitize_html_content(html_content: str) -> str:
        """Sanitize HTML content for email."""
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove dangerous tags
            dangerous_tags = ['script', 'object', 'embed', 'form', 'input', 'iframe']
            for tag in soup.find_all(dangerous_tags):
                tag.decompose()
            
            # Remove dangerous attributes
            dangerous_attrs = ['onclick', 'onload', 'onerror', 'onmouseover', 'javascript:']
            for tag in soup.find_all():
                for attr in list(tag.attrs.keys()):
                    if attr.lower() in dangerous_attrs or any(danger in str(tag.attrs[attr]).lower() for danger in dangerous_attrs):
                        del tag.attrs[attr]
            
            return str(soup)
        except ImportError:
            # Fallback: basic script tag removal
            html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
            html_content = re.sub(r'on\w+\s*=\s*["\'][^"\']*["\']', '', html_content, flags=re.IGNORECASE)
            return html_content
    
    @staticmethod
    def sanitize_subject(subject: str) -> str:
        """Sanitize email subject."""
        # Remove control characters and excessive whitespace
        subject = re.sub(r'[\x00-\x1f\x7f]', '', subject)
        subject = ' '.join(subject.split())
        
        # Truncate if too long
        if len(subject) > 998:  # RFC 5322 limit
            subject = subject[:995] + "..."
        
        return subject
    
    @staticmethod
    def sanitize_email_address(email: str) -> str:
        """Sanitize email address."""
        email = email.strip().lower()
        
        # Remove dangerous characters
        email = re.sub(r'[^\w@.-]', '', email)
        
        return email


class EmailBatchProcessor:
    """Batch processing utilities for emails."""
    
    @staticmethod
    def chunk_email_list(emails: List[EmailMessage], chunk_size: int = 100) -> List[List[EmailMessage]]:
        """Split email list into chunks."""
        chunks = []
        for i in range(0, len(emails), chunk_size):
            chunks.append(emails[i:i + chunk_size])
        return chunks
    
    @staticmethod
    def group_by_priority(emails: List[EmailMessage]) -> Dict[EmailPriority, List[EmailMessage]]:
        """Group emails by priority."""
        groups = {priority: [] for priority in EmailPriority}
        
        for email in emails:
            groups[email.priority].append(email)
        
        return groups
    
    @staticmethod
    def group_by_provider(emails: List[EmailMessage]) -> Dict[Optional[EmailProvider], List[EmailMessage]]:
        """Group emails by provider."""
        groups = {}
        
        for email in emails:
            provider = email.provider
            if provider not in groups:
                groups[provider] = []
            groups[provider].append(email)
        
        return groups
    
    @staticmethod
    def sort_by_send_time(emails: List[EmailMessage]) -> List[EmailMessage]:
        """Sort emails by scheduled send time."""
        return sorted(emails, key=lambda x: x.send_at or datetime.min.replace(tzinfo=timezone.utc))


class EmailErrorAnalyzer:
    """Email error analysis utilities."""
    
    ERROR_PATTERNS = {
        'invalid_email': [
            r'invalid.*email',
            r'bad.*recipient',
            r'user.*unknown',
            r'mailbox.*not.*found'
        ],
        'quota_exceeded': [
            r'quota.*exceeded',
            r'mailbox.*full',
            r'storage.*exceeded'
        ],
        'rate_limit': [
            r'rate.*limit',
            r'too.*many.*requests',
            r'sending.*too.*fast'
        ],
        'authentication': [
            r'authentication.*failed',
            r'invalid.*credentials',
            r'unauthorized'
        ],
        'network': [
            r'connection.*timeout',
            r'network.*error',
            r'dns.*error'
        ]
    }
    
    @staticmethod
    def categorize_error(error_message: str) -> str:
        """Categorize error message."""
        error_lower = error_message.lower()
        
        for category, patterns in EmailErrorAnalyzer.ERROR_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, error_lower):
                    return category
        
        return 'unknown'
    
    @staticmethod
    def is_permanent_error(error_message: str) -> bool:
        """Determine if error is permanent (no retry needed)."""
        permanent_categories = ['invalid_email', 'authentication']
        category = EmailErrorAnalyzer.categorize_error(error_message)
        return category in permanent_categories
    
    @staticmethod
    def suggest_retry_delay(error_message: str) -> int:
        """Suggest retry delay in seconds based on error type."""
        category = EmailErrorAnalyzer.categorize_error(error_message)
        
        delays = {
            'rate_limit': 300,  # 5 minutes
            'quota_exceeded': 3600,  # 1 hour
            'network': 60,  # 1 minute
            'unknown': 180  # 3 minutes
        }
        
        return delays.get(category, 180)


def create_email_from_template(
    template_name: str,
    context: Dict[str, Any],
    recipients: List[str],
    sender_email: str,
    sender_name: Optional[str] = None
) -> EmailMessage:
    """
    Utility function to create EmailMessage from template.
    
    Args:
        template_name: Name of the template
        context: Template context variables
        recipients: List of recipient emails
        sender_email: Sender email address
        sender_name: Sender name (optional)
    
    Returns:
        EmailMessage instance
    """
    # Validate recipients
    valid_recipients, invalid_recipients = EmailValidator.validate_email_list(recipients)
    
    if invalid_recipients:
        logger.warning(f"Invalid email addresses found: {invalid_recipients}")
    
    if not valid_recipients:
        raise ValueError("No valid recipients provided")
    
    # Create message
    message = EmailMessage(
        to=valid_recipients,
        from_email=sender_email,
        from_name=sender_name,
        template_name=template_name,
        template_context=context
    )
    
    return message


def bulk_validate_emails(emails: List[str]) -> Dict[str, List[str]]:
    """
    Bulk validate email addresses.
    
    Args:
        emails: List of email addresses to validate
    
    Returns:
        Dictionary with 'valid' and 'invalid' keys
    """
    valid, invalid = EmailValidator.validate_email_list(emails)
    
    return {
        'valid': valid,
        'invalid': invalid,
        'total': len(emails),
        'valid_count': len(valid),
        'invalid_count': len(invalid)
    }