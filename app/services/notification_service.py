"""
Notification service for sending alerts, reports, and system notifications.
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from app.services.base import BaseService
from app.core.exceptions import NotificationError, ServiceError
from app.core.enums import NotificationType, NotificationChannel, NotificationStatus
from app.utils.date_utils import get_current_timestamp
from app.core.config import settings


class NotificationService(BaseService):
    """Service for managing system notifications and alerts."""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session)
        self.smtp_server = settings.SMTP_SERVER
        self.smtp_port = settings.SMTP_PORT
        self.smtp_username = settings.SMTP_USERNAME
        self.smtp_password = settings.SMTP_PASSWORD
        self.from_email = settings.FROM_EMAIL
    
    def get_service_name(self) -> str:
        return "NotificationService"
    
    async def send_job_completion_notification(self, execution_id: int, recipients: List[str]) -> Dict[str, Any]:
        """Send job completion notification."""
        try:
            self.log_operation("send_job_completion_notification", {
                "execution_id": execution_id,
                "recipients_count": len(recipients)
            })
            
            # Get execution details
            execution = await self._get_execution_details(execution_id)
            if not execution:
                raise NotificationError("Execution not found")
            
            # Prepare notification content
            subject = f"ETL Job Completed: {execution.get('job_name', 'Unknown Job')}"
            
            if execution.get("status") == "SUCCESS":
                template = "job_success"
                priority = "NORMAL"
            else:
                template = "job_failure"
                priority = "HIGH"
            
            # Send notifications
            notification_results = []
            for recipient in recipients:
                try:
                    # Send email notification
                    email_result = await self._send_email_notification(
                        recipient=recipient,
                        subject=subject,
                        template=template,
                        context=execution
                    )
                    
                    # Log notification
                    notification_log = await self._log_notification({
                        "notification_type": NotificationType.JOB_COMPLETION.value,
                        "channel": NotificationChannel.EMAIL.value,
                        "recipient": recipient,
                        "subject": subject,
                        "status": NotificationStatus.SENT.value if email_result else NotificationStatus.FAILED.value,
                        "execution_id": execution_id,
                        "sent_at": get_current_timestamp()
                    })
                    
                    notification_results.append({
                        "recipient": recipient,
                        "status": "sent" if email_result else "failed",
                        "notification_id": notification_log.notification_id
                    })
                    
                except Exception as recipient_error:
                    self.logger.error(f"Failed to send notification to {recipient}: {recipient_error}")
                    notification_results.append({
                        "recipient": recipient,
                        "status": "failed",
                        "error": str(recipient_error)
                    })
            
            return {
                "execution_id": execution_id,
                "total_recipients": len(recipients),
                "successful_sends": len([r for r in notification_results if r["status"] == "sent"]),
                "failed_sends": len([r for r in notification_results if r["status"] == "failed"]),
                "notification_results": notification_results
            }
            
        except Exception as e:
            self.handle_error(e, "send_job_completion_notification")
    
    async def send_data_quality_alert(self, quality_check_id: int, recipients: List[str]) -> Dict[str, Any]:
        """Send data quality alert notification."""
        try:
            self.log_operation("send_data_quality_alert", {
                "quality_check_id": quality_check_id,
                "recipients_count": len(recipients)
            })
            
            # Get quality check details
            quality_check = await self._get_quality_check_details(quality_check_id)
            if not quality_check:
                raise NotificationError("Quality check not found")
            
            # Prepare notification content
            subject = f"Data Quality Alert: {quality_check.get('rule_name')}"
            template = "data_quality_alert"
            
            # Send notifications
            notification_results = []
            for recipient in recipients:
                try:
                    # Send email notification
                    email_result = await self._send_email_notification(
                        recipient=recipient,
                        subject=subject,
                        template=template,
                        context=quality_check
                    )
                    
                    # Log notification
                    notification_log = await self._log_notification({
                        "notification_type": NotificationType.DATA_QUALITY_ALERT.value,
                        "channel": NotificationChannel.EMAIL.value,
                        "recipient": recipient,
                        "subject": subject,
                        "status": NotificationStatus.SENT.value if email_result else NotificationStatus.FAILED.value,
                        "quality_check_id": quality_check_id,
                        "sent_at": get_current_timestamp()
                    })
                    
                    notification_results.append({
                        "recipient": recipient,
                        "status": "sent" if email_result else "failed",
                        "notification_id": notification_log.notification_id
                    })
                    
                except Exception as recipient_error:
                    self.logger.error(f"Failed to send quality alert to {recipient}: {recipient_error}")
                    notification_results.append({
                        "recipient": recipient,
                        "status": "failed",
                        "error": str(recipient_error)
                    })
            
            return {
                "quality_check_id": quality_check_id,
                "total_recipients": len(recipients),
                "successful_sends": len([r for r in notification_results if r["status"] == "sent"]),
                "failed_sends": len([r for r in notification_results if r["status"] == "failed"]),
                "notification_results": notification_results
            }
            
        except Exception as e:
            self.handle_error(e, "send_data_quality_alert")
    
    async def send_system_alert(self, alert_data: Dict[str, Any], recipients: List[str]) -> Dict[str, Any]:
        """Send system alert notification."""
        try:
            self.validate_input(alert_data, ["alert_type", "severity", "title", "message"])
            self.log_operation("send_system_alert", {
                "alert_type": alert_data["alert_type"],
                "severity": alert_data["severity"],
                "recipients_count": len(recipients)
            })
            
            # Prepare notification content
            subject = f"System Alert [{alert_data['severity']}]: {alert_data['title']}"
            template = "system_alert"
            
            # Determine priority based on severity
            priority_mapping = {
                "CRITICAL": "HIGH",
                "HIGH": "HIGH",
                "MEDIUM": "NORMAL",
                "LOW": "LOW"
            }
            priority = priority_mapping.get(alert_data["severity"], "NORMAL")
            
            # Send notifications
            notification_results = []
            for recipient in recipients:
                try:
                    # Send email notification
                    email_result = await self._send_email_notification(
                        recipient=recipient,
                        subject=subject,
                        template=template,
                        context=alert_data,
                        priority=priority
                    )
                    
                    # Log notification
                    notification_log = await self._log_notification({
                        "notification_type": NotificationType.SYSTEM_ALERT.value,
                        "channel": NotificationChannel.EMAIL.value,
                        "recipient": recipient,
                        "subject": subject,
                        "status": NotificationStatus.SENT.value if email_result else NotificationStatus.FAILED.value,
                        "alert_data": alert_data,
                        "sent_at": get_current_timestamp()
                    })
                    
                    notification_results.append({
                        "recipient": recipient,
                        "status": "sent" if email_result else "failed",
                        "notification_id": notification_log.notification_id
                    })
                    
                except Exception as recipient_error:
                    self.logger.error(f"Failed to send system alert to {recipient}: {recipient_error}")
                    notification_results.append({
                        "recipient": recipient,
                        "status": "failed",
                        "error": str(recipient_error)
                    })
            
            return {
                "alert_type": alert_data["alert_type"],
                "severity": alert_data["severity"],
                "total_recipients": len(recipients),
                "successful_sends": len([r for r in notification_results if r["status"] == "sent"]),
                "failed_sends": len([r for r in notification_results if r["status"] == "failed"]),
                "notification_results": notification_results
            }
            
        except Exception as e:
            self.handle_error(e, "send_system_alert")
    
    async def send_daily_report(self, report_data: Dict[str, Any], recipients: List[str]) -> Dict[str, Any]:
        """Send daily ETL report."""
        try:
            self.log_operation("send_daily_report", {"recipients_count": len(recipients)})
            
            # Prepare report content
            subject = f"Daily ETL Report - {datetime.utcnow().strftime('%Y-%m-%d')}"
            template = "daily_report"
            
            # Generate report attachment if needed
            report_attachment = None
            if report_data.get("include_attachment"):
                report_attachment = await self._generate_report_attachment(report_data)
            
            # Send notifications
            notification_results = []
            for recipient in recipients:
                try:
                    # Send email notification
                    email_result = await self._send_email_notification(
                        recipient=recipient,
                        subject=subject,
                        template=template,
                        context=report_data,
                        attachment=report_attachment
                    )
                    
                    # Log notification
                    notification_log = await self._log_notification({
                        "notification_type": NotificationType.DAILY_REPORT.value,
                        "channel": NotificationChannel.EMAIL.value,
                        "recipient": recipient,
                        "subject": subject,
                        "status": NotificationStatus.SENT.value if email_result else NotificationStatus.FAILED.value,
                        "sent_at": get_current_timestamp()
                    })
                    
                    notification_results.append({
                        "recipient": recipient,
                        "status": "sent" if email_result else "failed",
                        "notification_id": notification_log.notification_id
                    })
                    
                except Exception as recipient_error:
                    self.logger.error(f"Failed to send daily report to {recipient}: {recipient_error}")
                    notification_results.append({
                        "recipient": recipient,
                        "status": "failed",
                        "error": str(recipient_error)
                    })
            
            return {
                "report_date": datetime.utcnow().strftime('%Y-%m-%d'),
                "total_recipients": len(recipients),
                "successful_sends": len([r for r in notification_results if r["status"] == "sent"]),
                "failed_sends": len([r for r in notification_results if r["status"] == "failed"]),
                "notification_results": notification_results
            }
            
        except Exception as e:
            self.handle_error(e, "send_daily_report")
    
    async def send_custom_notification(self, notification_data: Dict[str, Any]) -> Dict[str, Any]:
        """Send custom notification."""
        try:
            self.validate_input(notification_data, ["recipients", "subject", "message"])
            self.log_operation("send_custom_notification", {
                "recipients_count": len(notification_data["recipients"])
            })
            
            recipients = notification_data["recipients"]
            subject = notification_data["subject"]
            message = notification_data["message"]
            template = notification_data.get("template", "custom")
            
            # Send notifications
            notification_results = []
            for recipient in recipients:
                try:
                    # Send email notification
                    email_result = await self._send_email_notification(
                        recipient=recipient,
                        subject=subject,
                        template=template,
                        context={"message": message, **notification_data.get("context", {})}
                    )
                    
                    # Log notification
                    notification_log = await self._log_notification({
                        "notification_type": NotificationType.CUSTOM.value,
                        "channel": NotificationChannel.EMAIL.value,
                        "recipient": recipient,
                        "subject": subject,
                        "status": NotificationStatus.SENT.value if email_result else NotificationStatus.FAILED.value,
                        "sent_at": get_current_timestamp()
                    })
                    
                    notification_results.append({
                        "recipient": recipient,
                        "status": "sent" if email_result else "failed",
                        "notification_id": notification_log.notification_id
                    })
                    
                except Exception as recipient_error:
                    self.logger.error(f"Failed to send custom notification to {recipient}: {recipient_error}")
                    notification_results.append({
                        "recipient": recipient,
                        "status": "failed",
                        "error": str(recipient_error)
                    })
            
            return {
                "subject": subject,
                "total_recipients": len(recipients),
                "successful_sends": len([r for r in notification_results if r["status"] == "sent"]),
                "failed_sends": len([r for r in notification_results if r["status"] == "failed"]),
                "notification_results": notification_results
            }
            
        except Exception as e:
            self.handle_error(e, "send_custom_notification")
    
    async def get_notification_history(self, notification_type: str = None, days: int = 7) -> List[Dict[str, Any]]:
        """Get notification history with optional filtering."""
        try:
            self.log_operation("get_notification_history", {
                "notification_type": notification_type,
                "days": days
            })
            
            notifications = await self._get_notification_history(notification_type, days)
            
            return [{
                "notification_id": notification.notification_id,
                "notification_type": notification.notification_type,
                "channel": notification.channel,
                "recipient": notification.recipient,
                "subject": notification.subject,
                "status": notification.status,
                "sent_at": notification.sent_at,
                "delivered_at": notification.delivered_at,
                "error_message": notification.error_message
            } for notification in notifications]
            
        except Exception as e:
            self.handle_error(e, "get_notification_history")
    
    async def get_notification_statistics(self, days: int = 30) -> Dict[str, Any]:
        """Get notification statistics."""
        try:
            self.log_operation("get_notification_statistics", {"days": days})
            
            stats = await self._get_notification_statistics(days)
            
            return {
                "period_days": days,
                "total_notifications": stats.get("total_notifications", 0),
                "successful_notifications": stats.get("successful_notifications", 0),
                "failed_notifications": stats.get("failed_notifications", 0),
                "delivery_rate": stats.get("delivery_rate", 0),
                "notification_by_type": stats.get("by_type", {}),
                "notification_by_channel": stats.get("by_channel", {}),
                "daily_trend": stats.get("daily_trend", [])
            }
            
        except Exception as e:
            self.handle_error(e, "get_notification_statistics")
    
    async def setup_notification_preferences(self, user_id: int, preferences: Dict[str, Any]) -> Dict[str, Any]:
        """Setup user notification preferences."""
        try:
            self.log_operation("setup_notification_preferences", {"user_id": user_id})
            
            # Validate preferences
            self.validate_input(preferences, ["email_notifications", "notification_types"])
            
            # Save preferences
            prefs = await self._save_notification_preferences(user_id, preferences)
            
            return {
                "user_id": user_id,
                "preferences": prefs,
                "status": "saved"
            }
            
        except Exception as e:
            self.handle_error(e, "setup_notification_preferences")
    
    async def test_notification_settings(self, test_email: str) -> Dict[str, Any]:
        """Test notification settings by sending a test email."""
        try:
            self.log_operation("test_notification_settings", {"test_email": test_email})
            
            # Send test email
            test_result = await self._send_email_notification(
                recipient=test_email,
                subject="ETL System - Test Notification",
                template="test_notification",
                context={
                    "test_time": get_current_timestamp(),
                    "system_name": "ETL System"
                }
            )
            
            return {
                "test_email": test_email,
                "status": "sent" if test_result else "failed",
                "timestamp": get_current_timestamp()
            }
            
        except Exception as e:
            self.handle_error(e, "test_notification_settings")
    
    async def send_webhook_notification(self, webhook_url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Send notification via webhook."""
        try:
            import aiohttp
            
            self.log_operation("send_webhook_notification", {"webhook_url": webhook_url})
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    success = response.status == 200
                    response_text = await response.text()
                    
                    # Log webhook notification
                    notification_log = await self._log_notification({
                        "notification_type": NotificationType.WEBHOOK.value,
                        "channel": NotificationChannel.WEBHOOK.value,
                        "recipient": webhook_url,
                        "subject": payload.get("subject", "Webhook Notification"),
                        "status": NotificationStatus.SENT.value if success else NotificationStatus.FAILED.value,
                        "payload": payload,
                        "response": response_text,
                        "sent_at": get_current_timestamp()
                    })
                    
                    return {
                        "webhook_url": webhook_url,
                        "status": "sent" if success else "failed",
                        "response_status": response.status,
                        "response_text": response_text,
                        "notification_id": notification_log.notification_id
                    }
                    
        except Exception as e:
            self.handle_error(e, "send_webhook_notification")
    
    async def send_slack_notification(self, webhook_url: str, message: str, channel: str = None, 
                                    username: str = "ETL Bot", attachments: List[Dict] = None) -> Dict[str, Any]:
        """Send notification to Slack."""
        try:
            self.log_operation("send_slack_notification", {"channel": channel})
            
            payload = {
                "text": message,
                "username": username,
                "icon_emoji": ":robot_face:"
            }
            
            if channel:
                payload["channel"] = channel
                
            if attachments:
                payload["attachments"] = attachments
            
            result = await self.send_webhook_notification(webhook_url, payload)
            
            return {
                "channel": channel,
                "message": message,
                "status": result.get("status"),
                "notification_id": result.get("notification_id")
            }
            
        except Exception as e:
            self.handle_error(e, "send_slack_notification")
    
    async def send_teams_notification(self, webhook_url: str, title: str, message: str, 
                                    color: str = "0078D4") -> Dict[str, Any]:
        """Send notification to Microsoft Teams."""
        try:
            self.log_operation("send_teams_notification", {"title": title})
            
            payload = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": color,
                "summary": title,
                "sections": [{
                    "activityTitle": title,
                    "activitySubtitle": f"ETL System Notification - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}",
                    "text": message,
                    "markdown": True
                }]
            }
            
            result = await self.send_webhook_notification(webhook_url, payload)
            
            return {
                "title": title,
                "message": message,
                "status": result.get("status"),
                "notification_id": result.get("notification_id")
            }
            
        except Exception as e:
            self.handle_error(e, "send_teams_notification")
    
    async def send_sms_notification(self, phone_number: str, message: str) -> Dict[str, Any]:
        """Send SMS notification (requires SMS service integration)."""
        try:
            self.log_operation("send_sms_notification", {"phone_number": phone_number})
            
            # This would integrate with SMS services like Twilio, AWS SNS, etc.
            # For now, it's a placeholder implementation
            
            # Example with Twilio:
            # from twilio.rest import Client
            # client = Client(settings.TWILIO_ACCOUNT_SID, settings.TWILIO_AUTH_TOKEN)
            # message = client.messages.create(
            #     body=message,
            #     from_=settings.TWILIO_PHONE_NUMBER,
            #     to=phone_number
            # )
            
            # Simulated success for placeholder
            success = True  # Replace with actual SMS sending logic
            
            # Log SMS notification
            notification_log = await self._log_notification({
                "notification_type": NotificationType.SMS.value,
                "channel": NotificationChannel.SMS.value,
                "recipient": phone_number,
                "subject": "SMS Notification",
                "message": message,
                "status": NotificationStatus.SENT.value if success else NotificationStatus.FAILED.value,
                "sent_at": get_current_timestamp()
            })
            
            return {
                "phone_number": phone_number,
                "message": message,
                "status": "sent" if success else "failed",
                "notification_id": notification_log.notification_id
            }
            
        except Exception as e:
            self.handle_error(e, "send_sms_notification")
    
    async def send_batch_notifications(self, notifications: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Send multiple notifications in batch."""
        try:
            self.log_operation("send_batch_notifications", {"batch_size": len(notifications)})
            
            results = []
            successful_count = 0
            failed_count = 0
            
            for notification in notifications:
                try:
                    notification_type = notification.get("type", "email")
                    
                    if notification_type == "email":
                        result = await self._send_email_notification(
                            recipient=notification["recipient"],
                            subject=notification["subject"],
                            template=notification.get("template", "custom"),
                            context=notification.get("context", {}),
                            priority=notification.get("priority", "NORMAL")
                        )
                    
                    elif notification_type == "webhook":
                        result = await self.send_webhook_notification(
                            webhook_url=notification["webhook_url"],
                            payload=notification["payload"]
                        )
                        result = result.get("status") == "sent"
                    
                    elif notification_type == "slack":
                        result = await self.send_slack_notification(
                            webhook_url=notification["webhook_url"],
                            message=notification["message"],
                            channel=notification.get("channel"),
                            attachments=notification.get("attachments")
                        )
                        result = result.get("status") == "sent"
                    
                    elif notification_type == "teams":
                        result = await self.send_teams_notification(
                            webhook_url=notification["webhook_url"],
                            title=notification["title"],
                            message=notification["message"],
                            color=notification.get("color", "0078D4")
                        )
                        result = result.get("status") == "sent"
                    
                    elif notification_type == "sms":
                        result = await self.send_sms_notification(
                            phone_number=notification["phone_number"],
                            message=notification["message"]
                        )
                        result = result.get("status") == "sent"
                    
                    else:
                        result = False
                    
                    if result:
                        successful_count += 1
                        results.append({"index": len(results), "status": "sent"})
                    else:
                        failed_count += 1
                        results.append({"index": len(results), "status": "failed"})
                        
                except Exception as notification_error:
                    failed_count += 1
                    results.append({
                        "index": len(results),
                        "status": "failed",
                        "error": str(notification_error)
                    })
            
            return {
                "total_notifications": len(notifications),
                "successful_count": successful_count,
                "failed_count": failed_count,
                "success_rate": (successful_count / len(notifications) * 100) if notifications else 0,
                "results": results
            }
            
        except Exception as e:
            self.handle_error(e, "send_batch_notifications")
    
    async def schedule_notification(self, notification_data: Dict[str, Any], 
                                  scheduled_time: datetime) -> Dict[str, Any]:
        """Schedule a notification to be sent later."""
        try:
            self.log_operation("schedule_notification", {"scheduled_time": scheduled_time})
            
            # Save scheduled notification
            scheduled_notification = await self._save_scheduled_notification({
                "notification_type": notification_data.get("type", "email"),
                "recipient": notification_data["recipient"],
                "subject": notification_data.get("subject"),
                "message": notification_data.get("message"),
                "template": notification_data.get("template"),
                "context": notification_data.get("context", {}),
                "scheduled_time": scheduled_time,
                "status": "SCHEDULED",
                "created_at": get_current_timestamp()
            })
            
            return {
                "scheduled_notification_id": scheduled_notification.id,
                "scheduled_time": scheduled_time,
                "status": "scheduled"
            }
            
        except Exception as e:
            self.handle_error(e, "schedule_notification")
    
    async def cancel_scheduled_notification(self, scheduled_notification_id: int) -> bool:
        """Cancel a scheduled notification."""
        try:
            self.log_operation("cancel_scheduled_notification", {
                "scheduled_notification_id": scheduled_notification_id
            })
            
            # Update scheduled notification status
            success = await self._cancel_scheduled_notification(scheduled_notification_id)
            
            return success
            
        except Exception as e:
            self.handle_error(e, "cancel_scheduled_notification")
    
    async def process_scheduled_notifications(self) -> Dict[str, Any]:
        """Process all due scheduled notifications."""
        try:
            self.log_operation("process_scheduled_notifications")
            
            # Get due notifications
            due_notifications = await self._get_due_scheduled_notifications()
            
            processed_count = 0
            failed_count = 0
            
            for notification in due_notifications:
                try:
                    # Send notification based on type
                    if notification.notification_type == "email":
                        result = await self._send_email_notification(
                            recipient=notification.recipient,
                            subject=notification.subject,
                            template=notification.template or "custom",
                            context=notification.context or {}
                        )
                    
                    # Add other notification types as needed
                    
                    # Update notification status
                    if result:
                        await self._update_scheduled_notification_status(
                            notification.id, 
                            "SENT", 
                            get_current_timestamp()
                        )
                        processed_count += 1
                    else:
                        await self._update_scheduled_notification_status(
                            notification.id, 
                            "FAILED", 
                            get_current_timestamp()
                        )
                        failed_count += 1
                        
                except Exception as notification_error:
                    self.logger.error(f"Failed to process scheduled notification {notification.id}: {notification_error}")
                    await self._update_scheduled_notification_status(
                        notification.id, 
                        "FAILED", 
                        get_current_timestamp(),
                        str(notification_error)
                    )
                    failed_count += 1
            
            return {
                "total_due": len(due_notifications),
                "processed_count": processed_count,
                "failed_count": failed_count,
                "timestamp": get_current_timestamp()
            }
            
        except Exception as e:
            self.handle_error(e, "process_scheduled_notifications")
    
    async def create_notification_template(self, template_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a custom notification template."""
        try:
            self.validate_input(template_data, ["name", "type", "subject", "content"])
            self.log_operation("create_notification_template", {"name": template_data["name"]})
            
            template = await self._save_notification_template({
                "name": template_data["name"],
                "type": template_data["type"],  # email, slack, teams, etc.
                "subject": template_data["subject"],
                "content": template_data["content"],
                "variables": template_data.get("variables", []),
                "is_active": template_data.get("is_active", True),
                "created_at": get_current_timestamp()
            })
            
            return {
                "template_id": template.id,
                "name": template.name,
                "type": template.type,
                "status": "created"
            }
            
        except Exception as e:
            self.handle_error(e, "create_notification_template")
    
    async def get_notification_templates(self, template_type: str = None) -> List[Dict[str, Any]]:
        """Get notification templates with optional filtering."""
        try:
            self.log_operation("get_notification_templates", {"template_type": template_type})
            
            templates = await self._get_notification_templates(template_type)
            
            return [{
                "template_id": template.id,
                "name": template.name,
                "type": template.type,
                "subject": template.subject,
                "variables": template.variables,
                "is_active": template.is_active,
                "created_at": template.created_at,
                "usage_count": await self._get_template_usage_count(template.id)
            } for template in templates]
            
        except Exception as e:
            self.handle_error(e, "get_notification_templates")
    
    async def send_notification_with_template(self, template_id: int, recipients: List[str], 
                                            variables: Dict[str, Any]) -> Dict[str, Any]:
        """Send notification using a custom template."""
        try:
            self.log_operation("send_notification_with_template", {
                "template_id": template_id,
                "recipients_count": len(recipients)
            })
            
            # Get template
            template = await self._get_notification_template_by_id(template_id)
            if not template:
                raise NotificationError("Template not found")
            
            # Render template with variables
            rendered_content = await self._render_template(template.content, variables)
            rendered_subject = await self._render_template(template.subject, variables)
            
            # Send notifications
            notification_results = []
            for recipient in recipients:
                try:
                    if template.type == "email":
                        result = await self._send_email_notification(
                            recipient=recipient,
                            subject=rendered_subject,
                            template="custom",
                            context={"message": rendered_content}
                        )
                    
                    # Add other template types as needed
                    
                    notification_results.append({
                        "recipient": recipient,
                        "status": "sent" if result else "failed"
                    })
                    
                except Exception as recipient_error:
                    notification_results.append({
                        "recipient": recipient,
                        "status": "failed",
                        "error": str(recipient_error)
                    })
            
            return {
                "template_id": template_id,
                "template_name": template.name,
                "total_recipients": len(recipients),
                "successful_sends": len([r for r in notification_results if r["status"] == "sent"]),
                "failed_sends": len([r for r in notification_results if r["status"] == "failed"]),
                "notification_results": notification_results
            }
            
        except Exception as e:
            self.handle_error(e, "send_notification_with_template")
    
    async def get_notification_delivery_status(self, notification_id: int) -> Dict[str, Any]:
        """Get delivery status of a notification."""
        try:
            self.log_operation("get_notification_delivery_status", {"notification_id": notification_id})
            
            notification = await self._get_notification_by_id(notification_id)
            if not notification:
                raise NotificationError("Notification not found")
            
            # Check delivery status (if supported by the channel)
            delivery_info = await self._check_delivery_status(notification)
            
            return {
                "notification_id": notification_id,
                "status": notification.status,
                "sent_at": notification.sent_at,
                "delivered_at": notification.delivered_at,
                "delivery_info": delivery_info,
                "recipient": notification.recipient,
                "channel": notification.channel
            }
            
        except Exception as e:
            self.handle_error(e, "get_notification_delivery_status")
    
    async def setup_notification_escalation(self, escalation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Setup notification escalation rules."""
        try:
            self.validate_input(escalation_data, ["name", "trigger_conditions", "escalation_levels"])
            self.log_operation("setup_notification_escalation", {"name": escalation_data["name"]})
            
            escalation = await self._save_notification_escalation({
                "name": escalation_data["name"],
                "description": escalation_data.get("description"),
                "trigger_conditions": escalation_data["trigger_conditions"],
                "escalation_levels": escalation_data["escalation_levels"],
                "is_active": escalation_data.get("is_active", True),
                "created_at": get_current_timestamp()
            })
            
            return {
                "escalation_id": escalation.id,
                "name": escalation.name,
                "levels": len(escalation_data["escalation_levels"]),
                "status": "created"
            }
            
        except Exception as e:
            self.handle_error(e, "setup_notification_escalation")
    
    async def trigger_escalation(self, escalation_id: int, context: Dict[str, Any]) -> Dict[str, Any]:
        """Trigger notification escalation."""
        try:
            self.log_operation("trigger_escalation", {"escalation_id": escalation_id})
            
            escalation = await self._get_notification_escalation_by_id(escalation_id)
            if not escalation:
                raise NotificationError("Escalation rule not found")
            
            # Process escalation levels
            escalation_results = []
            for level in escalation.escalation_levels:
                try:
                    # Send notifications for this level
                    level_result = await self._process_escalation_level(level, context)
                    escalation_results.append(level_result)
                    
                    # Check if we should continue to next level based on conditions
                    if not level.get("continue_on_success", True) and level_result.get("success"):
                        break
                        
                except Exception as level_error:
                    self.logger.error(f"Failed to process escalation level: {level_error}")
                    escalation_results.append({
                        "level": level.get("level", 0),
                        "success": False,
                        "error": str(level_error)
                    })
            
            return {
                "escalation_id": escalation_id,
                "escalation_name": escalation.name,
                "levels_processed": len(escalation_results),
                "results": escalation_results
            }
            
        except Exception as e:
            self.handle_error(e, "trigger_escalation")
    
    async def send_notification_digest(self, user_id: int, digest_type: str = "daily") -> Dict[str, Any]:
        """Send notification digest (daily/weekly summary)."""
        try:
            self.log_operation("send_notification_digest", {"user_id": user_id, "digest_type": digest_type})
            
            # Get user preferences
            user_prefs = await self._get_user_notification_preferences(user_id)
            if not user_prefs or not user_prefs.get("enable_digest"):
                return {"status": "skipped", "reason": "digest disabled"}
            
            # Get digest data based on type
            if digest_type == "daily":
                digest_data = await self._get_daily_digest_data(user_id)
                subject = f"Daily ETL Digest - {datetime.utcnow().strftime('%Y-%m-%d')}"
            elif digest_type == "weekly":
                digest_data = await self._get_weekly_digest_data(user_id)
                subject = f"Weekly ETL Digest - Week of {datetime.utcnow().strftime('%Y-%m-%d')}"
            else:
                raise NotificationError(f"Unknown digest type: {digest_type}")
            
            # Send digest
            result = await self._send_email_notification(
                recipient=user_prefs["email"],
                subject=subject,
                template="notification_digest",
                context={
                    "digest_type": digest_type,
                    "user_name": user_prefs.get("name", "User"),
                    **digest_data
                }
            )
            
            return {
                "user_id": user_id,
                "digest_type": digest_type,
                "status": "sent" if result else "failed"
            }
            
        except Exception as e:
            self.handle_error(e, "send_notification_digest")
    
    async def manage_notification_subscriptions(self, user_id: int, subscriptions: Dict[str, Any]) -> Dict[str, Any]:
        """Manage user notification subscriptions."""
        try:
            self.log_operation("manage_notification_subscriptions", {"user_id": user_id})
            
            # Save subscription preferences
            saved_subscriptions = await self._save_notification_subscriptions(user_id, subscriptions)
            
            return {
                "user_id": user_id,
                "subscriptions": saved_subscriptions,
                "status": "updated"
            }
            
        except Exception as e:
            self.handle_error(e, "manage_notification_subscriptions")
    
    async def get_notification_metrics(self, days: int = 30) -> Dict[str, Any]:
        """Get comprehensive notification metrics."""
        try:
            self.log_operation("get_notification_metrics", {"days": days})
            
            metrics = await self._calculate_notification_metrics(days)
            
            return {
                "period_days": days,
                "total_notifications": metrics.get("total", 0),
                "delivery_metrics": {
                    "delivered": metrics.get("delivered", 0),
                    "failed": metrics.get("failed", 0),
                    "pending": metrics.get("pending", 0),
                    "delivery_rate": metrics.get("delivery_rate", 0)
                },
                "channel_metrics": metrics.get("channel_breakdown", {}),
                "type_metrics": metrics.get("type_breakdown", {}),
                "performance_metrics": {
                    "avg_delivery_time": metrics.get("avg_delivery_time", 0),
                    "peak_volume_hour": metrics.get("peak_hour", 0),
                    "error_rate": metrics.get("error_rate", 0)
                },
                "trend_data": metrics.get("daily_trend", [])
            }
            
        except Exception as e:
            self.handle_error(e, "get_notification_metrics")
    
    # Private helper methods
    async def _send_email_notification(self, recipient: str, subject: str, template: str, 
                                     context: Dict[str, Any], priority: str = "NORMAL", 
                                     attachment: Dict[str, Any] = None) -> bool:
        """Send email notification using SMTP."""
        try:
            # Load email template
            email_content = await self._load_email_template(template, context)
            
            # Create message
            msg = MIMEMultipart('alternative')
            msg['From'] = self.from_email
            msg['To'] = recipient
            msg['Subject'] = subject
            
            # Add priority header
            if priority == "HIGH":
                msg['X-Priority'] = '1'
                msg['X-MSMail-Priority'] = 'High'
            elif priority == "LOW":
                msg['X-Priority'] = '5'
                msg['X-MSMail-Priority'] = 'Low'
            
            # Add HTML content
            html_part = MIMEText(email_content["html"], 'html')
            msg.attach(html_part)
            
            # Add text content if available
            if email_content.get("text"):
                text_part = MIMEText(email_content["text"], 'plain')
                msg.attach(text_part)
            
            # Add attachment if provided
            if attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment["content"])
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename= {attachment["filename"]}'
                )
                msg.attach(part)
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                if self.smtp_username and self.smtp_password:
                    server.starttls()
                    server.login(self.smtp_username, self.smtp_password)
                
                server.send_message(msg)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send email to {recipient}: {e}")
            return False
    
    async def _load_email_template(self, template: str, context: Dict[str, Any]) -> Dict[str, str]:
        """Load and render email template."""
        # This is a simplified version - in production, use a proper template engine like Jinja2
        templates = {
            "job_success": {
                "html": f"""
                <html>
                <body>
                    <h2>ETL Job Completed Successfully</h2>
                    <p>Job: <strong>{context.get('job_name', 'Unknown')}</strong></p>
                    <p>Execution ID: {context.get('execution_id', 'N/A')}</p>
                    <p>Status: <span style="color: green;">SUCCESS</span></p>
                    <p>Records Processed: {context.get('records_processed', 0)}</p>
                    <p>Duration: {context.get('duration_seconds', 0)} seconds</p>
                    <p>Completed at: {context.get('end_time', 'N/A')}</p>
                </body>
                </html>
                """,
                "text": f"""
                ETL Job Completed Successfully
                
                Job: {context.get('job_name', 'Unknown')}
                Execution ID: {context.get('execution_id', 'N/A')}
                Status: SUCCESS
                Records Processed: {context.get('records_processed', 0)}
                Duration: {context.get('duration_seconds', 0)} seconds
                Completed at: {context.get('end_time', 'N/A')}
                """
            },
            "job_failure": {
                "html": f"""
                <html>
                <body>
                    <h2>ETL Job Failed</h2>
                    <p>Job: <strong>{context.get('job_name', 'Unknown')}</strong></p>
                    <p>Execution ID: {context.get('execution_id', 'N/A')}</p>
                    <p>Status: <span style="color: red;">FAILED</span></p>
                    <p>Error: {context.get('error_details', {}).get('message', 'Unknown error')}</p>
                    <p>Failed at: {context.get('end_time', 'N/A')}</p>
                    <p>Please check the system logs for more details.</p>
                </body>
                </html>
                """,
                "text": f"""
                ETL Job Failed
                
                Job: {context.get('job_name', 'Unknown')}
                Execution ID: {context.get('execution_id', 'N/A')}
                Status: FAILED
                Error: {context.get('error_details', {}).get('message', 'Unknown error')}
                Failed at: {context.get('end_time', 'N/A')}
                
                Please check the system logs for more details.
                """
            },
            "data_quality_alert": {
                "html": f"""
                <html>
                <body>
                    <h2>Data Quality Alert</h2>
                    <p>Rule: <strong>{context.get('rule_name', 'Unknown')}</strong></p>
                    <p>Rule Type: {context.get('rule_type', 'N/A')}</p>
                    <p>Entity Type: {context.get('entity_type', 'N/A')}</p>
                    <p>Check Result: <span style="color: red;">{context.get('check_result', 'FAILED')}</span></p>
                    <p>Records Checked: {context.get('records_checked', 0)}</p>
                    <p>Records Failed: {context.get('records_failed', 0)}</p>
                    <p>Pass Rate: {context.get('pass_rate', 0)}%</p>
                    <p>Please review the data quality issues and take appropriate action.</p>
                </body>
                </html>
                """,
                "text": f"""
                Data Quality Alert
                
                Rule: {context.get('rule_name', 'Unknown')}
                Rule Type: {context.get('rule_type', 'N/A')}
                Entity Type: {context.get('entity_type', 'N/A')}
                Check Result: {context.get('check_result', 'FAILED')}
                Records Checked: {context.get('records_checked', 0)}
                Records Failed: {context.get('records_failed', 0)}
                Pass Rate: {context.get('pass_rate', 0)}%
                
                Please review the data quality issues and take appropriate action.
                """
            },
            "system_alert": {
                "html": f"""
                <html>
                <body>
                    <h2>System Alert</h2>
                    <p>Alert Type: <strong>{context.get('alert_type', 'Unknown')}</strong></p>
                    <p>Severity: <span style="color: {'red' if context.get('severity') == 'CRITICAL' else 'orange'};">{context.get('severity', 'N/A')}</span></p>
                    <p>Title: {context.get('title', 'N/A')}</p>
                    <p>Message: {context.get('message', 'N/A')}</p>
                    <p>Source: {context.get('source', 'SYSTEM')}</p>
                    <p>Time: {context.get('created_at', get_current_timestamp())}</p>
                </body>
                </html>
                """,
                "text": f"""
                System Alert
                
                Alert Type: {context.get('alert_type', 'Unknown')}
                Severity: {context.get('severity', 'N/A')}
                Title: {context.get('title', 'N/A')}
                Message: {context.get('message', 'N/A')}
                Source: {context.get('source', 'SYSTEM')}
                Time: {context.get('created_at', get_current_timestamp())}
                """
            },
            "daily_report": {
                "html": f"""
                <html>
                <body>
                    <h2>Daily ETL Report</h2>
                    <h3>Summary</h3>
                    <ul>
                        <li>Total Jobs Executed: {context.get('total_jobs', 0)}</li>
                        <li>Successful Jobs: {context.get('successful_jobs', 0)}</li>
                        <li>Failed Jobs: {context.get('failed_jobs', 0)}</li>
                        <li>Success Rate: {context.get('success_rate', 0)}%</li>
                        <li>Total Records Processed: {context.get('total_records', 0)}</li>
                        <li>Files Processed: {context.get('files_processed', 0)}</li>
                    </ul>
                    <h3>Data Quality</h3>
                    <ul>
                        <li>Quality Checks: {context.get('quality_checks', 0)}</li>
                        <li>Quality Pass Rate: {context.get('quality_pass_rate', 0)}%</li>
                    </ul>
                    <p>For detailed information, please check the ETL dashboard.</p>
                </body>
                </html>
                """,
                "text": f"""
                Daily ETL Report
                
                Summary:
                - Total Jobs Executed: {context.get('total_jobs', 0)}
                - Successful Jobs: {context.get('successful_jobs', 0)}
                - Failed Jobs: {context.get('failed_jobs', 0)}
                - Success Rate: {context.get('success_rate', 0)}%
                - Total Records Processed: {context.get('total_records', 0)}
                - Files Processed: {context.get('files_processed', 0)}
                
                Data Quality:
                - Quality Checks: {context.get('quality_checks', 0)}
                - Quality Pass Rate: {context.get('quality_pass_rate', 0)}%
                
                For detailed information, please check the ETL dashboard.
                """
            },
            "test_notification": {
                "html": f"""
                <html>
                <body>
                    <h2>Test Notification</h2>
                    <p>This is a test notification from the ETL System.</p>
                    <p>System: {context.get('system_name', 'ETL System')}</p>
                    <p>Test Time: {context.get('test_time', 'N/A')}</p>
                    <p>If you received this email, your notification settings are working correctly.</p>
                </body>
                </html>
                """,
                "text": f"""
                Test Notification
                
                This is a test notification from the ETL System.
                System: {context.get('system_name', 'ETL System')}
                Test Time: {context.get('test_time', 'N/A')}
                
                If you received this email, your notification settings are working correctly.
                """
            },
            "notification_digest": {
                "html": f"""
                <html>
                <body>
                    <h2>{context.get('digest_type', 'Daily').title()} ETL Digest</h2>
                    <p>Hello {context.get('user_name', 'User')},</p>
                    <p>Here's your {context.get('digest_type', 'daily')} summary:</p>
                    
                    <h3>Job Activity</h3>
                    <ul>
                        <li>Jobs Executed: {context.get('jobs_executed', 0)}</li>
                        <li>Success Rate: {context.get('success_rate', 0)}%</li>
                        <li>Records Processed: {context.get('records_processed', 0)}</li>
                    </ul>
                    
                    <h3>Data Quality</h3>
                    <ul>
                        <li>Quality Checks: {context.get('quality_checks', 0)}</li>
                        <li>Issues Found: {context.get('quality_issues', 0)}</li>
                    </ul>
                    
                    <h3>Alerts</h3>
                    <ul>
                        <li>New Alerts: {context.get('new_alerts', 0)}</li>
                        <li>Critical Alerts: {context.get('critical_alerts', 0)}</li>
                    </ul>
                    
                    <p>For more details, visit your ETL dashboard.</p>
                </body>
                </html>
                """,
                "text": f"""
                {context.get('digest_type', 'Daily').title()} ETL Digest
                
                Hello {context.get('user_name', 'User')},
                
                Here's your {context.get('digest_type', 'daily')} summary:
                
                Job Activity:
                - Jobs Executed: {context.get('jobs_executed', 0)}
                - Success Rate: {context.get('success_rate', 0)}%
                - Records Processed: {context.get('records_processed', 0)}
                
                Data Quality:
                - Quality Checks: {context.get('quality_checks', 0)}
                - Issues Found: {context.get('quality_issues', 0)}
                
                Alerts:
                - New Alerts: {context.get('new_alerts', 0)}
                - Critical Alerts: {context.get('critical_alerts', 0)}
                
                For more details, visit your ETL dashboard.
                """
            },
            "custom": {
                "html": f"""
                <html>
                <body>
                    <h2>Custom Notification</h2>
                    <div>{context.get('message', 'No message provided')}</div>
                </body>
                </html>
                """,
                "text": f"""
                Custom Notification
                
                {context.get('message', 'No message provided')}
                """
            }
        }
        
        return templates.get(template, templates["custom"])
    
    async def _generate_report_attachment(self, report_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate report attachment (CSV, PDF, etc.)."""
        try:
            import csv
            import io
            
            # Generate CSV report
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write header
            writer.writerow(['Metric', 'Value'])
            
            # Write data
            for key, value in report_data.items():
                if isinstance(value, (str, int, float)):
                    writer.writerow([key, value])
            
            content = output.getvalue().encode('utf-8')
            output.close()
            
            return {
                "filename": f"etl_report_{datetime.utcnow().strftime('%Y%m%d')}.csv",
                "content": content,
                "content_type": "text/csv"
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate report attachment: {e}")
            return None
    
    async def _render_template(self, template_content: str, variables: Dict[str, Any]) -> str:
        """Render template with variables."""
        try:
            # Simple template rendering - in production, use Jinja2 or similar
            rendered_content = template_content
            for key, value in variables.items():
                rendered_content = rendered_content.replace(f"{{{key}}}", str(value))
            return rendered_content
        except Exception as e:
            self.logger.error(f"Failed to render template: {e}")
            return template_content
    
    # Database helper methods (implement based on your models)
    async def _get_execution_details(self, execution_id: int):
        """Get execution details from database."""
        # Implement database query


        pass
    
    async def _get_quality_check_details(self, quality_check_id: int):
        """Get quality check details from database."""
        # Implement database query
        pass
    
    async def _log_notification(self, notification_data: Dict[str, Any]):
        """Log notification in database."""
        # Implement database insert
        pass
    
    async def _get_notification_history(self, notification_type: str, days: int):
        """Get notification history from database."""
        # Implement database query
        pass
    
    async def _get_notification_statistics(self, days: int):
        """Get notification statistics from database."""
        # Implement database query
        pass
    
    async def _save_notification_preferences(self, user_id: int, preferences: Dict[str, Any]):
        """Save user notification preferences."""
        # Implement database insert/update
        pass
    
    async def _save_scheduled_notification(self, notification_data: Dict[str, Any]):
        """Save scheduled notification to database."""
        # Implement database insert
        pass
    
    async def _cancel_scheduled_notification(self, scheduled_notification_id: int):
        """Cancel scheduled notification in database."""
        # Implement database update
        pass
    
    async def _get_due_scheduled_notifications(self):
        """Get due scheduled notifications from database."""
        # Implement database query
        pass
    
    async def _update_scheduled_notification_status(self, notification_id: int, status: str, 
                                                   processed_at: datetime, error_message: str = None):
        """Update scheduled notification status."""
        # Implement database update
        pass
    
    async def _save_notification_template(self, template_data: Dict[str, Any]):
        """Save notification template to database."""
        # Implement database insert
        pass
    
    async def _get_notification_templates(self, template_type: str = None):
        """Get notification templates from database."""
        # Implement database query
        pass
    
    async def _get_notification_template_by_id(self, template_id: int):
        """Get notification template by ID."""
        # Implement database query
        pass
    
    async def _get_template_usage_count(self, template_id: int):
        """Get template usage count."""
        # Implement database query
        pass
    
    async def _get_notification_by_id(self, notification_id: int):
        """Get notification by ID."""
        # Implement database query
        pass
    
    async def _check_delivery_status(self, notification):
        """Check delivery status of notification."""
        # Implement delivery status checking
        pass
    
    async def _save_notification_escalation(self, escalation_data: Dict[str, Any]):
        """Save notification escalation to database."""
        # Implement database insert
        pass
    
    async def _get_notification_escalation_by_id(self, escalation_id: int):
        """Get notification escalation by ID."""
        # Implement database query
        pass
    
    async def _process_escalation_level(self, level: Dict[str, Any], context: Dict[str, Any]):
        """Process single escalation level."""
        # Implement escalation level processing
        pass
    
    async def _get_user_notification_preferences(self, user_id: int):
        """Get user notification preferences."""
        # Implement database query
        pass
    
    async def _get_daily_digest_data(self, user_id: int):
        """Get daily digest data for user."""
        # Implement data aggregation for daily digest
        pass
    
    async def _get_weekly_digest_data(self, user_id: int):
        """Get weekly digest data for user."""
        # Implement data aggregation for weekly digest
        pass
    
    async def _save_notification_subscriptions(self, user_id: int, subscriptions: Dict[str, Any]):
        """Save notification subscriptions."""
        # Implement database insert/update
        pass
    
    async def _calculate_notification_metrics(self, days: int):
        """Calculate comprehensive notification metrics."""
        # Implement metrics calculation
        pass
