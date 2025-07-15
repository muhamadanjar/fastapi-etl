import csv
from app.processors.base_processor import BaseProcessor

class CSVProcessor(BaseProcessor):
    def extract(self):
        with open(self.file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            return [row for row in reader]

    def detect_structure(self):
        with open(self.file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)
            return [{"column_name": col, "data_type": "STRING"} for col in header]