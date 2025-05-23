import smtplib
import logging
from email.message import EmailMessage
from typing import List, Optional

from app.core.config import settings


class EmailService:
    """
    Service to send emails using SMTP.
    """

    def __init__(self):
        self.smtp_host = settings.smtp_host
        self.smtp_port = settings.smtp_port
        self.smtp_user = settings.smtp_user
        self.smtp_password = settings.smtp_password
        self.from_email = settings.smtp_from_email

    def send_email(
        self,
        subject: str,
        recipients: List[str],
        body: str,
        html_body: Optional[str] = None,
    ):
        """
        Send an email synchronously.

        :param subject: Email subject
        :param recipients: List of recipient email addresses
        :param body: Plain text body content
        :param html_body: Optional HTML body content
        """
        message = EmailMessage()
        message["From"] = self.from_email
        message["To"] = ", ".join(recipients)
        message["Subject"] = subject
        message.set_content(body)
        if html_body:
            message.add_alternative(html_body, subtype="html")

        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(message)
            logging.info(f"Email sent to {recipients}")
        except Exception as e:
            logging.error(f"Failed to send email: {e}")
            raise
