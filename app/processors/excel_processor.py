import pandas as pd
from app.processors.base_processor import BaseProcessor

class ExcelProcessor(BaseProcessor):
    def extract(self):
        df = pd.read_excel(self.file_path, sheet_name=None)
        records = []
        for sheet_name, sheet_df in df.items():
            for index, row in sheet_df.iterrows():
                records.append({"sheet": sheet_name, **row.to_dict()})
        return records

    def detect_structure(self):
        df = pd.read_excel(self.file_path, nrows=1)
        return [{"column_name": col, "data_type": str(df[col].dtype)} for col in df.columns]
