from app.services.base import BaseService

class NotificationService(BaseService):
    def send_alert(self, message: str):
        print(f"[ALERT] {message}")
