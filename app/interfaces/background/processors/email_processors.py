"""
Email background processor.

This module contains background tasks for processing email operations
asynchronously to improve application performance and reliability.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

from ....infrastructure.background.celery_config import get_celery_app
# from ....infrastructure.email.smtp_service import SMTPEmailService, EmailMessage, EmailAttachment
# from ....infrastructure.database.connection import get_database_session
# from ....infrastructure.database.repositories.user_repository import SQLUserRepository
from ....core.config import get_settings
from ....core.exceptions import EmailError

app = get_celery_app()
settings = get_settings()
logger = logging.getLogger(__name__)


@app.task(bind=True, max_retries=3, default_retry_delay=60, queue='email')
def send_email_task(
    self,
    to: str,
    subject: str,
    body: str,
    from_email: Optional[str] = None,
    from_name: Optional[str] = None,
    is_html: bool = True,
    cc: Optional[List[str]] = None,
    bcc: Optional[List[str]] = None,
    attachments: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Send email asynchronously.
    
    Args:
        to: Recipient email address
        subject: Email subject
        body: Email body content
        from_email: Sender email address
        from_name: Sender name
        is_html: Whether body is HTML
        cc: CC recipients
        bcc: BCC recipients
        attachments: List of attachment data
        
    Returns:
        Task result with send status
    """
    try:
        logger.info(f"Processing email task {self.request.id} for {to}")
        
        # Create email service
        email_service = SMTPEmailService()
        
        # Convert attachment data back to EmailAttachment objects
        email_attachments = []
        if attachments:
            for att_data in attachments:
                attachment = EmailAttachment(
                    filename=att_data['filename'],
                    content=att_data['content'].encode() if isinstance(att_data['content'], str) else att_data['content'],
                    content_type=att_data.get('content_type', 'application/octet-stream')
                )
                email_attachments.append(attachment)
        
        # Create email message
        message = EmailMessage(
            to=to,
            subject=subject,
            body=body,
            from_email=from_email,
            from_name=from_name,
            is_html=is_html,
            cc=cc,
            bcc=bcc,
            attachments=email_attachments,
        )
        
        # Send email
        success = email_service.send_email(message)
        
        result = {
            'success': success,
            'recipient': to,
            'subject': subject,
            'task_id': self.request.id,
            'sent_at': datetime.utcnow().isoformat(),
        }
        
        logger.info(f"Email task {self.request.id} completed successfully for {to}")
        return result
        
    except EmailError as e:
        logger.error(f"Email error in task {self.request.id}: {e}")
        # Retry on email errors
        try:
            raise self.retry(exc=e, countdown=60 * (self.request.retries + 1))
        except self.MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for email task {self.request.id}")
            raise e
    
    except Exception as e:
        logger.error(f"Unexpected error in email task {self.request.id}: {e}")
        # Don't retry on unexpected errors
        raise


@app.task(bind=True, max_retries=3, default_retry_delay=30, queue='email')
def send_welcome_email_task(
    self,
    user_id: str,
    verification_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Send welcome email to new user.
    
    Args:
        user_id: User's ID
        verification_url: Email verification URL
        
    Returns:
        Task result
    """
    try:
        logger.info(f"Processing welcome email task {self.request.id} for user {user_id}")
        
        # Get user from database
        with get_database_session() as session:
            user_repo = SQLUserRepository(session)
            user = user_repo.get_by_id_or_raise(user_id)
        
        # Send welcome email
        email_service = SMTPEmailService()
        success = email_service.send_welcome_email(
            to=user.email.value,
            user_name=user.full_name,
            verification_url=verification_url,
        )
        
        result = {
            'success': success,
            'user_id': user_id,
            'email': user.email.value,
            'user_name': user.full_name,
            'task_id': self.request.id,
            'sent_at': datetime.utcnow().isoformat(),
        }
        
        logger.info(f"Welcome email task {self.request.id} completed for user {user_id}")
        return result
        
    except Exception as e:
        logger.error(f"Error in welcome email task {self.request.id}: {e}")
        try:
            raise self.retry(exc=e)
        except self.MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for welcome email task {self.request.id}")
            raise e


@app.task(bind=True, max_retries=3, default_retry_delay=30, queue='email')
def send_password_reset_email_task(
    self,
    user_id: str,
    reset_url: str,
    expires_in_hours: int = 1,
) -> Dict[str, Any]:
    """
    Send password reset email.
    
    Args:
        user_id: User's ID
        reset_url: Password reset URL
        expires_in_hours: Hours until reset link expires
        
    Returns:
        Task result
    """
    try:
        logger.info(f"Processing password reset email task {self.request.id} for user {user_id}")
        
        # Get user from database
        with get_database_session() as session:
            user_repo = SQLUserRepository(session)
            user = user_repo.get_by_id_or_raise(user_id)
        
        # Send password reset email
        email_service = SMTPEmailService()
        success = email_service.send_password_reset_email(
            to=user.email.value,
            user_name=user.full_name,
            reset_url=reset_url,
            expires_in_hours=expires_in_hours,
        )
        
        result = {
            'success': success,
            'user_id': user_id,
            'email': user.email.value,
            'user_name': user.full_name,
            'task_id': self.request.id,
            'sent_at': datetime.utcnow().isoformat(),
        }
        
        logger.info(f"Password reset email task {self.request.id} completed for user {user_id}")
        return result
        
    except Exception as e:
        logger.error(f"Error in password reset email task {self.request.id}: {e}")
        try:
            raise self.retry(exc=e)
        except self.MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for password reset email task {self.request.id}")
            raise e


@app.task(bind=True, max_retries=2, default_retry_delay=120, queue='email')
def send_bulk_email_task(
    self,
    recipients: List[str],
    subject: str,
    body: str,
    from_email: Optional[str] = None,
    from_name: Optional[str] = None,
    is_html: bool = True,
    batch_size: int = 50,
) -> Dict[str, Any]:
    """
    Send bulk email to multiple recipients.
    
    Args:
        recipients: List of recipient email addresses
        subject: Email subject
        body: Email body content
        from_email: Sender email address
        from_name: Sender name
        is_html: Whether body is HTML
        batch_size: Number of emails to send per batch
        
    Returns:
        Task result with send statistics
    """
    try:
        logger.info(f"Processing bulk email task {self.request.id} for {len(recipients)} recipients")
        
        email_service = SMTPEmailService()
        
        # Create messages for each recipient
        messages = []
        for recipient in recipients:
            message = EmailMessage(
                to=recipient,
                subject=subject,
                body=body,
                from_email=from_email,
                from_name=from_name,
                is_html=is_html,
            )
            messages.append(message)
        
        # Send bulk email
        results = email_service.send_bulk_email(messages, batch_size=batch_size)
        
        result = {
            'total_recipients': len(recipients),
            'sent': results['sent'],
            'failed': results['failed'],
            'errors': results['errors'],
            'task_id': self.request.id,
            'completed_at': datetime.utcnow().isoformat(),
        }
        
        logger.info(f"Bulk email task {self.request.id} completed: {results['sent']} sent, {results['failed']} failed")
        return result
        
    except Exception as e:
        logger.error(f"Error in bulk email task {self.request.id}: {e}")
        try:
            raise self.retry(exc=e)
        except self.MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for bulk email task {self.request.id}")
            raise e


@app.task(bind=True, max_retries=3, default_retry_delay=30, queue='email')
def send_notification_email_task(
    self,
    user_id: str,
    notification_type: str,
    data: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Send notification email based on type.
    
    Args:
        user_id: User's ID
        notification_type: Type of notification
        data: Notification data
        
    Returns:
        Task result
    """
    try:
        logger.info(f"Processing notification email task {self.request.id} for user {user_id}, type: {notification_type}")
        
        # Get user from database
        with get_database_session() as session:
            user_repo = SQLUserRepository(session)
            user = user_repo.get_by_id_or_raise(user_id)
        
        email_service = SMTPEmailService()
        
        # Define notification templates and subjects
        notification_config = {
            'account_locked': {
                'template': 'account_locked.html',
                'subject': 'Your account has been locked - Security Alert',
            },
            'password_changed': {
                'template': 'password_changed.html',
                'subject': 'Your password has been changed successfully',
            },
            'login_alert': {
                'template': 'login_alert.html',
                'subject': 'New login to your account',
            },
            'profile_updated': {
                'template': 'profile_updated.html',
                'subject': 'Your profile has been updated',
            },
            'security_alert': {
                'template': 'security_alert.html',
                'subject': 'Security Alert - Action Required',
            },
        }
        
        config = notification_config.get(notification_type)
        if not config:
            raise ValueError(f"Unknown notification type: {notification_type}")
        
        # Add user information to context
        context = {
            'user_name': user.full_name,
            'user_email': user.email.value,
            'app_name': settings.app_name,
            'support_email': settings.email.from_email,
            **data
        }
        
        success = email_service.send_template_email(
            template_name=config['template'],
            to=user.email.value,
            subject=config['subject'],
            context=context,
        )
        
        result = {
            'success': success,
            'user_id': user_id,
            'email': user.email.value,
            'notification_type': notification_type,
            'task_id': self.request.id,
            'sent_at': datetime.utcnow().isoformat(),
        }
        
        logger.info(f"Notification email task {self.request.id} completed for user {user_id}")
        return result
        
    except Exception as e:
        logger.error(f"Error in notification email task {self.request.id}: {e}")
        try:
            raise self.retry(exc=e)
        except self.MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for notification email task {self.request.id}")
            raise e


@app.task(bind=True, max_retries=1, queue='email')
def test_email_connection_task(self) -> Dict[str, Any]:
    """
    Test email service connection.
    
    Returns:
        Connection test result
    """
    try:
        logger.info(f"Testing email connection in task {self.request.id}")
        
        email_service = SMTPEmailService()
        connection_ok = email_service.test_connection()
        
        result = {
            'connection_ok': connection_ok,
            'service_info': email_service.get_connection_info(),
            'task_id': self.request.id,
            'tested_at': datetime.utcnow().isoformat(),
        }
        
        logger.info(f"Email connection test task {self.request.id} completed: {connection_ok}")
        return result
        
    except Exception as e:
        logger.error(f"Email connection test task {self.request.id} failed: {e}")
        raise


@app.task(bind=True, queue='email')
def send_scheduled_digest_email_task(self) -> Dict[str, Any]:
    """
    Send scheduled digest emails to active users.
    
    This is typically called by Celery Beat on a schedule.
    
    Returns:
        Task result with digest statistics
    """
    try:
        logger.info(f"Processing scheduled digest email task {self.request.id}")
        
        # Get active users from database
        with get_database_session() as session:
            user_repo = SQLUserRepository(session)
            # Get users who want digest emails (you'd add this field to user model)
            active_users = user_repo.find_active_users(limit=1000)
        
        email_service = SMTPEmailService()
        sent_count = 0
        failed_count = 0
        
        for user in active_users:
            try:
                # Prepare digest data (you'd implement this based on your app's needs)
                digest_data = {
                    'user_name': user.full_name,
                    'digest_date': datetime.utcnow().strftime('%B %d, %Y'),
                    'app_name': settings.app_name,
                    'support_email': settings.email.from_email,
                    # Add more digest content here
                }
                
                success = email_service.send_template_email(
                    template_name='digest.html',
                    to=user.email.value,
                    subject=f'Your {settings.app_name} Weekly Digest',
                    context=digest_data,
                )
                
                if success:
                    sent_count += 1
                else:
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"Failed to send digest to {user.email}: {e}")
                failed_count += 1
        
        result = {
            'total_users': len(active_users),
            'sent': sent_count,
            'failed': failed_count,
            'task_id': self.request.id,
            'completed_at': datetime.utcnow().isoformat(),
        }
        
        logger.info(f"Digest email task {self.request.id} completed: {sent_count} sent, {failed_count} failed")
        return result
        
    except Exception as e:
        logger.error(f"Error in digest email task {self.request.id}: {e}")
        raise


# Utility functions for calling tasks asynchronously
def send_email_async(
    to: str,
    subject: str,
    body: str,
    **kwargs
) -> str:
    """
    Send email asynchronously and return task ID.
    
    Args:
        to: Recipient email address
        subject: Email subject
        body: Email body
        **kwargs: Additional email parameters
        
    Returns:
        Task ID
    """
    task = send_email_task.delay(
        to=to,
        subject=subject,
        body=body,
        **kwargs
    )
    return task.id


def send_welcome_email_async(
    user_id: str,
    verification_url: Optional[str] = None,
) -> str:
    """Send welcome email asynchronously."""
    task = send_welcome_email_task.delay(
        user_id=user_id,
        verification_url=verification_url,
    )
    return task.id


def send_password_reset_email_async(
    user_id: str,
    reset_url: str,
    expires_in_hours: int = 1,
) -> str:
    """Send password reset email asynchronously."""
    task = send_password_reset_email_task.delay(
        user_id=user_id,
        reset_url=reset_url,
        expires_in_hours=expires_in_hours,
    )
    return task.id


def send_notification_email_async(
    user_id: str,
    notification_type: str,
    data: Dict[str, Any],
) -> str:
    """Send notification email asynchronously."""
    task = send_notification_email_task.delay(
        user_id=user_id,
        notification_type=notification_type,
        data=data,
    )
    return task.id


def schedule_email(
    email_data: Dict[str, Any],
    eta,  # datetime or timedelta
) -> str:
    """
    Schedule email for future delivery.
    
    Args:
        email_data: Email data
        eta: When to send the email
        
    Returns:
        Task ID
    """
    task = send_email_task.apply_async(
        kwargs=email_data,
        eta=eta,
        queue='email',
    )
    return task.id