import xml.etree.ElementTree as ET
from app.processors.base_processor import BaseProcessor

class XMLProcessor(BaseProcessor):
    def extract(self):
        tree = ET.parse(self.file_path)
        root = tree.getroot()
        records = []
        for elem in root:
            records.append({child.tag: child.text for child in elem})
        return records

    def detect_structure(self):
        records = self.extract()
        if not records:
            return []
        return [{"column_name": k, "data_type": "STRING"} for k in records[0].keys()]
