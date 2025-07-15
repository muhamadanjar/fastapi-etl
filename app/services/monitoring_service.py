from app.services.base import BaseService

class MonitoringService(BaseService):
    def generate_report(self):
        print("System health report generated.")