import json
from app.processors.base_processor import BaseProcessor

class JSONProcessor(BaseProcessor):
    def extract(self):
        with open(self.file_path, encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            return [data]

    def detect_structure(self):
        records = self.extract()
        if not records:
            return []
        return [{"column_name": k, "data_type": type(v).__name__} for k, v in records[0].items()]
