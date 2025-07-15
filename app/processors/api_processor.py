from app.processors.base_processor import BaseProcessor
import requests

class APIProcessor(BaseProcessor):
    def extract(self):
        response = requests.get(self.file_path)
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else [data]

    def detect_structure(self):
        records = self.extract()
        if not records:
            return []
        return [{"column_name": k, "data_type": type(v).__name__} for k, v in records[0].items()]
