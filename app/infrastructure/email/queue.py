"""
Email manager for handling email operations with multiple providers and features.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Union, Type
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import uuid

from .base import (
    EmailMessage, EmailTemplate, EmailProvider, EmailStatus, 
    EmailPriority, EmailInterface, TemplateEngineInterface
)
from .queue import EmailQueue, QueueMessage
from .utils import (
    EmailValidator, EmailTemplateProcessor, EmailHasher,
    EmailConfigValidator, create_email_from_template
)
from .decorators import robust_email_sender, RetryConfig, RateLimitConfig, CircuitBreakerConfig

logger = logging.getLogger(__name__)


@dataclass
class EmailManagerConfig:
    """Configuration for EmailManager."""
    default_provider: EmailProvider = EmailProvider.SMTP
    fallback_providers: List[EmailProvider] = field(default_factory=list)
    max_concurrent_sends: int = 10
    queue_enabled: bool = True
    retry_failed_emails: bool = True
    track_email_metrics: bool = True
    enable_templates: bool = True
    enable_deduplication: bool = True
    deduplication_window_hours: int = 24
    default_from_email: Optional[str] = None
    default_from_name: Optional[str] = None
    rate_limit_per_minute: int = 60
    circuit_breaker_enabled: bool = True


@dataclass
class EmailSendResult:
    """Result of email send operation."""
    message_id: str
    provider_message_id: Optional[str] = None
    status: EmailStatus = EmailStatus.PENDING
    provider: Optional[EmailProvider] = None
    error_message: Optional[str] = None
    sent_at: Optional[datetime] = None
    retry_count: int = 0


@dataclass
class BulkEmailSendResult:
    """Result of bulk email send operation."""
    total_emails: int
    successful_sends: List[EmailSendResult] = field(default_factory=list)
    failed_sends: List[EmailSendResult] = field(default_factory=list)
    queued_emails: List[str] = field(default_factory=list)
    
    @property
    def success_count(self) -> int:
        return len(self.successful_sends)
    
    @property
    def failure_count(self) -> int:
        return len(self.failed_sends)
    
    @property
    def queued_count(self) -> int:
        return len(self.queued_emails)
    
    @property
    def success_rate(self) -> float:
        if self.total_emails == 0:
            return 0.0
        return (self.success_count / self.total_emails) * 100


class EmailManager:
    """
    Main email manager class that orchestrates email sending with multiple providers,
    queueing, templates, and advanced features.
    """
    
    def __init__(
        self,
        config: EmailManagerConfig,
        providers: Dict[EmailProvider, EmailInterface],
        queue: Optional[EmailQueue] = None,
        template_engine: Optional[TemplateEngineInterface] = None
    ):
        self.config = config
        self.providers = providers
        self.queue = queue
        self.template_engine = template_engine
        
        # Internal state
        self._sent_message_hashes: Dict[str, datetime] = {}
        self._circuit_breakers: Dict[EmailProvider, Any] = {}
        self._semaphore = asyncio.Semaphore(config.max_concurrent_sends)
        
        # Validate configuration
        self._validate_config()
        
        logger.info(f"EmailManager initialized with {len(providers)} providers")
    
    def _validate_config(self):
        """Validate manager configuration."""
        if self.config.default_provider not in self.providers:
            raise ValueError(f"Default provider {self.config.default_provider} not available")
        
        for provider in self.config.fallback_providers:
            if provider not in self.providers:
                logger.warning(f"Fallback provider {provider} not available")
        
        if self.config.queue_enabled and self.queue is None:
            logger.warning("Queue enabled but no queue instance provided")
        
        if self.config.enable_templates and self.template_engine is None:
            logger.warning("Templates enabled but no template engine provided")
    
    async def send_email(
        self,
        message: EmailMessage,
        provider: Optional[EmailProvider] = None,
        immediate: bool = False
    ) -> EmailSendResult:
        """
        Send a single email.
        
        Args:
            message: Email message to send
            provider: Specific provider to use (optional)
            immediate: Skip queue and send immediately
        
        Returns:
            EmailSendResult with send details
        """
        try:
            # Set defaults
            if not message.from_email and self.config.default_from_email:
                message.from_email = self.config.default_from_email
            if not message.from_name and self.config.default_from_name:
                message.from_name = self.config.default_from_name
            
            # Validate message
            await self._validate_message(message)
            
            # Check for deduplication
            if self.config.enable_deduplication and self._is_duplicate(message):
                logger.info(f"Duplicate email detected: {message.id}")
                return EmailSendResult(
                    message_id=message.id,
                    status=EmailStatus.REJECTED,
                    error_message="Duplicate email detected"
                )
            
            # Process template if needed
            if message.template_name and self.template_engine:
                await self._process_template(message)
            
            # Determine if should queue or send immediately
            if (
                not immediate and 
                self.config.queue_enabled and 
                self.queue and 
                (message.send_at is not None or message.priority == EmailPriority.LOW)
            ):
                await self._queue_email(message)
                return EmailSendResult(
                    message_id=message.id,
                    status=EmailStatus.QUEUED
                )
            
            # Send immediately
            return await self._send_email_immediately(message, provider)
            
        except Exception as e:
            logger.error(f"Failed to send email {message.id}: {e}")
            return EmailSendResult(
                message_id=message.id,
                status=EmailStatus.FAILED,
                error_message=str(e)
            )
    
    async def send_bulk_emails(
        self,
        messages: List[EmailMessage],
        provider: Optional[EmailProvider] = None,
        immediate: bool = False,
        batch_size: int = 50
    ) -> BulkEmailSendResult:
        """
        Send multiple emails in batches.
        
        Args:
            messages: List of email messages
            provider: Specific provider to use
            immediate: Skip queue and send immediately
            batch_size: Number of emails per batch
        
        Returns:
            BulkEmailSendResult with detailed results
        """
        result = BulkEmailSendResult(total_emails=len(messages))
        
        # Process in batches
        for i in range(0, len(messages), batch_size):
            batch = messages[i:i + batch_size]
            
            # Send batch concurrently
            tasks = [
                self.send_email(message, provider, immediate)
                for message in batch
            ]
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for send_result in batch_results:
                if isinstance(send_result, Exception):
                    result.failed_sends.append(EmailSendResult(
                        message_id="unknown",
                        status=EmailStatus.FAILED,
                        error_message=str(send_result)
                    ))
                elif send_result.status == EmailStatus.QUEUED:
                    result.queued_emails.append(send_result.message_id)
                elif send_result.status in [EmailStatus.SENT, EmailStatus.SENDING]:
                    result.successful_sends.append(send_result)
                else:
                    result.failed_sends.append(send_result)
            
            # Brief pause between batches
            if i + batch_size < len(messages):
                await asyncio.sleep(0.1)
        
        logger.info(f"Bulk send completed: {result.success_count} sent, {result.failure_count} failed, {result.queued_count} queued")
        return result
    
    async def send_template_email(
        self,
        template_name: str,
        context: Dict[str, Any],
        recipients: List[str],
        subject_override: Optional[str] = None,
        provider: Optional[EmailProvider] = None,
        **kwargs
    ) -> EmailSendResult:
        """
        Send email using template.
        
        Args:
            template_name: Name of the template
            context: Template context variables
            recipients: List of recipient emails
            subject_override: Override template subject
            provider: Specific provider to use
            **kwargs: Additional EmailMessage parameters
        
        Returns:
            EmailSendResult
        """
        if not self.template_engine:
            raise ValueError("Template engine not configured")
        
        # Create message from template
        message = create_email_from_template(
            template_name=template_name,
            context=context,
            recipients=recipients,
            sender_email=self.config.default_from_email or "",
            sender_name=self.config.default_from_name
        )
        
        # Apply overrides
        if subject_override:
            message.subject = subject_override
        
        # Apply additional parameters
        for key, value in kwargs.items():
            if hasattr(message, key):
                setattr(message, key, value)
        
        return await self.send_email(message, provider)
    
    async def process_queue(self, batch_size: int = 100) -> Dict[str, int]:
        """
        Process queued emails.
        
        Args:
            batch_size: Number of emails to process at once
        
        Returns:
            Dictionary with processing statistics
        """
        if not self.queue:
            raise ValueError("Queue not configured")
        
        stats = {
            'processed': 0,
            'sent': 0,
            'failed': 0,
            'requeued': 0
        }
        
        while True:
            # Get batch of emails to process
            queue_messages = await self.queue.get_batch(batch_size)
            if not queue_messages:
                break
            
            for queue_msg in queue_messages:
                stats['processed'] += 1
                
                try:
                    # Reconstruct EmailMessage
                    email_data = queue_msg.data
                    message = EmailMessage(**email_data)
                    
                    # Check if should send now
                    if message.send_at and message.send_at > datetime.utcnow():
                        # Requeue for later
                        await self.queue.requeue(queue_msg)
                        stats['requeued'] += 1
                        continue
                    
                    # Send email
                    result = await self._send_email_immediately(message)
                    
                    if result.status in [EmailStatus.SENT, EmailStatus.SENDING]:
                        await self.queue.mark_completed(queue_msg.id)
                        stats['sent'] += 1
                    else:
                        # Check if should retry
                        if message.can_retry():
                            message.mark_retry()
                            retry_queue_msg = QueueMessage(
                                id=str(uuid.uuid4()),
                                data=message.to_dict(),
                                priority=message.priority.value,
                                retry_count=message.retry_count,
                                scheduled_at=datetime.utcnow() + timedelta(minutes=5)
                            )
                            await self.queue.enqueue(retry_queue_msg)
                            stats['requeued'] += 1
                        else:
                            await self.queue.mark_failed(queue_msg.id, result.error_message or "Max retries exceeded")
                            stats['failed'] += 1
                        
                        await self.queue.mark_completed(queue_msg.id)
                
                except Exception as e:
                    logger.error(f"Error processing queued email {queue_msg.id}: {e}")
                    await self.queue.mark_failed(queue_msg.id, str(e))
                    stats['failed'] += 1
        
        logger.info(f"Queue processing completed: {stats}")
        return stats
    
    async def get_email_status(self, message_id: str) -> Optional[EmailStatus]:
        """Get status of sent email."""
        # Check queue first
        if self.queue:
            queue_status = await self.queue.get_message_status(message_id)
            if queue_status:
                return queue_status
        
        # Check with providers
        for provider_interface in self.providers.values():
            try:
                status = await provider_interface.get_delivery_status(message_id)
                if status != EmailStatus.PENDING:
                    return status
            except Exception as e:
                logger.debug(f"Error getting status from provider: {e}")
        
        return None
    
    async def get_provider_health(self) -> Dict[EmailProvider, bool]:
        """Check health of all providers."""
        health_status = {}
        
        for provider, interface in self.providers.items():
            try:
                health_status[provider] = await interface.health_check()
            except Exception as e:
                logger.error(f"Health check failed for {provider}: {e}")
                health_status[provider] = False
        
        return health_status
    
    async def get_statistics(self, time_period: timedelta = timedelta(hours=24)) -> Dict[str, Any]:
        """Get email statistics for a time period."""
        if not self.queue:
            return {}
        
        return await self.queue.get_statistics(time_period)
    
    async def _validate_message(self, message: EmailMessage):
        """Validate email message."""
        if not message.to:
            raise ValueError("Email must have at least one recipient")
        
        # Validate email addresses
        all_recipients = message.get_all_recipients()
        valid_emails, invalid_emails = EmailValidator.validate_email_list(all_recipients)
        
        if invalid_emails:
            raise ValueError(f"Invalid email addresses: {invalid_emails}")
        
        if message.is_expired():
            raise ValueError("Email message has expired")
    
    async def _process_template(self, message: EmailMessage):
        """Process email template."""
        if not self.template_engine or not message.template_name:
            return
        
        try:
            template = await self.template_engine.load_template(message.template_name)
            
            # Validate template context
            missing_vars = EmailTemplateProcessor.validate_template_context(
                template.subject_template + template.body_template,
                message.template_context
            )
            
            if missing_vars:
                raise ValueError(f"Missing template variables: {missing_vars}")
            
            # Render template
            message.subject = await self.template_engine.render_string(
                template.subject_template, 
                message.template_context
            )
            
            if template.template_type == "html":
                message.html_body = await self.template_engine.render_string(
                    template.body_template,
                    message.template_context
                )
            else:
                message.body = await self.template_engine.render_string(
                    template.body_template,
                    message.template_context
                )
                
        except Exception as e:
            logger.error(f"Template processing failed for {message.template_name}: {e}")
            raise
    
    def _is_duplicate(self, message: EmailMessage) -> bool:
        """Check if message is a duplicate."""
        message_hash = EmailHasher.generate_message_hash(message)
        current_time = datetime.utcnow()
        
        # Clean old hashes
        cutoff_time = current_time - timedelta(hours=self.config.deduplication_window_hours)
        self._sent_message_hashes = {
            h: t for h, t in self._sent_message_hashes.items() 
            if t > cutoff_time
        }
        
        # Check for duplicate
        if message_hash in self._sent_message_hashes:
            return True
        
        # Record this hash
        self._sent_message_hashes[message_hash] = current_time
        return False
    
    async def _queue_email(self, message: EmailMessage):
        """Queue email for later processing."""
        if not self.queue:
            raise ValueError("Queue not configured")
        
        queue_message = QueueMessage(
            id=message.id,
            data=message.to_dict(),
            priority=message.priority.value,
            scheduled_at=message.send_at or datetime.utcnow()
        )
        
        await self.queue.enqueue(queue_message)
        message.status = EmailStatus.QUEUED
        logger.info(f"Email queued: {message.id}")
    
    async def _send_email_immediately(
        self, 
        message: EmailMessage,
        preferred_provider: Optional[EmailProvider] = None
    ) -> EmailSendResult:
        """Send email immediately using available providers."""
        async with self._semaphore:
            # Determine provider order
            providers_to_try = self._get_provider_order(preferred_provider)
            
            last_error = None
            
            for provider in providers_to_try:
                try:
                    provider_interface = self.providers[provider]
                    
                    # Check circuit breaker
                    if self.config.circuit_breaker_enabled:
                        if not self._can_use_provider(provider):
                            logger.warning(f"Circuit breaker open for provider {provider}")
                            continue
                    
                    # Apply decorators for robust sending
                    send_func = self._get_decorated_send_function(provider_interface)
                    
                    # Send email
                    message.mark_sending()
                    message.provider = provider
                    
                    provider_message_id = await send_func(message)
                    
                    # Mark as sent
                    message.mark_sent(provider_message_id)
                    
                    # Record success for circuit breaker
                    if self.config.circuit_breaker_enabled:
                        self._record_provider_success(provider)
                    
                    logger.info(f"Email sent successfully: {message.id} via {provider}")
                    
                    return EmailSendResult(
                        message_id=message.id,
                        provider_message_id=provider_message_id,
                        status=EmailStatus.SENT,
                        provider=provider,
                        sent_at=datetime.utcnow()
                    )
                    
                except Exception as e:
                    last_error = e
                    logger.warning(f"Failed to send via {provider}: {e}")
                    
                    # Record failure for circuit breaker
                    if self.config.circuit_breaker_enabled:
                        self._record_provider_failure(provider)
                    
                    # Continue to next provider
                    continue
            
            # All providers failed
            error_msg = f"All providers failed. Last error: {last_error}"
            message.mark_failed(error_msg)
            
            return EmailSendResult(
                message_id=message.id,
                status=EmailStatus.FAILED,
                error_message=error_msg
            )
    
    def _get_provider_order(self, preferred_provider: Optional[EmailProvider]) -> List[EmailProvider]:
        """Get ordered list of providers to try."""
        providers = []
        
        # Add preferred provider first
        if preferred_provider and preferred_provider in self.providers:
            providers.append(preferred_provider)
        
        # Add default provider
        if self.config.default_provider not in providers:
            providers.append(self.config.default_provider)
        
        # Add fallback providers
        for provider in self.config.fallback_providers:
            if provider in self.providers and provider not in providers:
                providers.append(provider)
        
        return providers
    
    def _get_decorated_send_function(self, provider_interface: EmailInterface):
        """Get decorated send function with retry, rate limiting, etc."""
        # Configure decorators
        retry_config = RetryConfig(
            max_attempts=3,
            base_delay=1.0,
            exponential_base=2.0
        )
        
        rate_limit_config = RateLimitConfig(
            max_calls=self.config.rate_limit_per_minute,
            time_window=60
        )
        
        circuit_breaker_config = CircuitBreakerConfig(
            failure_threshold=5,
            recovery_timeout=300
        )
        
        # Apply robust email sender decorator
        @robust_email_sender(
            retry_config=retry_config,
            rate_limit_config=rate_limit_config,
            circuit_breaker_config=circuit_breaker_config if self.config.circuit_breaker_enabled else None
        )
        async def send_email_with_decorators(message: EmailMessage) -> str:
            return await provider_interface.send_email(message)
        
        return send_email_with_decorators
    
    def _can_use_provider(self, provider: EmailProvider) -> bool:
        """Check if provider can be used (circuit breaker check)."""
        # Simple circuit breaker implementation
        if provider not in self._circuit_breakers:
            self._circuit_breakers[provider] = {
                'failures': 0,
                'last_failure': None,
                'state': 'closed'  # closed, open, half-open
            }
        
        breaker = self._circuit_breakers[provider]
        
        if breaker['state'] == 'closed':
            return True
        elif breaker['state'] == 'open':
            # Check if recovery timeout has passed
            if breaker['last_failure']:
                time_since_failure = (datetime.utcnow() - breaker['last_failure']).total_seconds()
                if time_since_failure > 300:  # 5 minutes
                    breaker['state'] = 'half-open'
                    return True
            return False
        elif breaker['state'] == 'half-open':
            return True
        
        return False
    
    def _record_provider_success(self, provider: EmailProvider):
        """Record successful provider operation."""
        if provider in self._circuit_breakers:
            self._circuit_breakers[provider] = {
                'failures': 0,
                'last_failure': None,
                'state': 'closed'
            }
    
    def _record_provider_failure(self, provider: EmailProvider):
        """Record failed provider operation."""
        if provider not in self._circuit_breakers:
            self._circuit_breakers[provider] = {
                'failures': 0,
                'last_failure': None,
                'state': 'closed'
            }
        
        breaker = self._circuit_breakers[provider]
        breaker['failures'] += 1
        breaker['last_failure'] = datetime.utcnow()
        
        if breaker['failures'] >= 5:  # Threshold
            breaker['state'] = 'open'
            logger.warning(f"Circuit breaker opened for provider {provider}")


class EmailManagerBuilder:
    """Builder class for EmailManager."""
    
    def __init__(self):
        self.config = EmailManagerConfig()
        self.providers: Dict[EmailProvider, EmailInterface] = {}
        self.queue: Optional[EmailQueue] = None
        self.template_engine: Optional[TemplateEngineInterface] = None
    
    def with_config(self, config: EmailManagerConfig) -> "EmailManagerBuilder":
        """Set manager configuration."""
        self.config = config
        return self
    
    def with_provider(self, provider: EmailProvider, interface: EmailInterface) -> "EmailManagerBuilder":
        """Add email provider."""
        self.providers[provider] = interface
        return self
    
    def with_queue(self, queue: EmailQueue) -> "EmailManagerBuilder":
        """Set email queue."""
        self.queue = queue
        return self
    
    def with_template_engine(self, engine: TemplateEngineInterface) -> "EmailManagerBuilder":
        """Set template engine."""
        self.template_engine = engine
        return self
    
    def with_smtp_provider(self, config: Dict[str, Any]) -> "EmailManagerBuilder":
        """Add SMTP provider with configuration."""
        # Validate SMTP config
        errors = EmailConfigValidator.validate_smtp_config(config)
        if errors:
            raise ValueError(f"Invalid SMTP config: {errors}")
        
        # Create SMTP provider (implementation would be in a separate module)
        # from .providers.smtp import SMTPEmailProvider
        # provider = SMTPEmailProvider(config)
        # self.providers[EmailProvider.SMTP] = provider
        
        logger.info("SMTP provider would be added here")
        return self
    
    def with_sendgrid_provider(self, api_key: str) -> "EmailManagerBuilder":
        """Add SendGrid provider."""
        config = {'api_key': api_key}
        errors = EmailConfigValidator.validate_provider_config(EmailProvider.SENDGRID, config)
        if errors:
            raise ValueError(f"Invalid SendGrid config: {errors}")
        
        # Create SendGrid provider
        # from .providers.sendgrid import SendGridEmailProvider
        # provider = SendGridEmailProvider(config)
        # self.providers[EmailProvider.SENDGRID] = provider
        
        logger.info("SendGrid provider would be added here")
        return self
    
    def with_default_settings(self) -> "EmailManagerBuilder":
        """Apply default settings for common use cases."""
        self.config.max_concurrent_sends = 20
        self.config.rate_limit_per_minute = 100
        self.config.queue_enabled = True
        self.config.circuit_breaker_enabled = True
        self.config.enable_deduplication = True
        return self
    
    def build(self) -> EmailManager:
        """Build EmailManager instance."""
        if not self.providers:
            raise ValueError("At least one email provider must be configured")
        
        if self.config.default_provider not in self.providers:
            # Use first available provider as default
            self.config.default_provider = next(iter(self.providers.keys()))
        
        return EmailManager(
            config=self.config,
            providers=self.providers,
            queue=self.queue,
            template_engine=self.template_engine
        )


# Factory functions for common configurations
def create_simple_email_manager(
    smtp_config: Dict[str, Any],
    default_from_email: str,
    default_from_name: Optional[str] = None
) -> EmailManager:
    """Create a simple email manager with SMTP only."""
    config = EmailManagerConfig(
        default_from_email=default_from_email,
        default_from_name=default_from_name,
        queue_enabled=False,
        enable_templates=False
    )
    
    builder = EmailManagerBuilder()
    return (builder
            .with_config(config)
            .with_smtp_provider(smtp_config)
            .build())


def create_production_email_manager(
    providers_config: Dict[EmailProvider, Dict[str, Any]],
    queue: EmailQueue,
    template_engine: TemplateEngineInterface,
    default_from_email: str
) -> EmailManager:
    """Create a production-ready email manager with all features."""
    config = EmailManagerConfig(
        default_from_email=default_from_email,
        max_concurrent_sends=50,
        rate_limit_per_minute=200,
        queue_enabled=True,
        circuit_breaker_enabled=True,
        enable_templates=True,
        enable_deduplication=True
    )
    
    builder = (EmailManagerBuilder()
               .with_config(config)
               .with_queue(queue)
               .with_template_engine(template_engine)
               .with_default_settings())
    
    # Add providers based on config
    for provider, provider_config in providers_config.items():
        if provider == EmailProvider.SMTP:
            builder.with_smtp_provider(provider_config)
        elif provider == EmailProvider.SENDGRID:
            builder.with_sendgrid_provider(provider_config['api_key'])
        # Add more providers as needed
    
    return builder.build()