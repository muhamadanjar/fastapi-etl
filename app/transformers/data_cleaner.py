from app.transformers.base_transformer import BaseTransformer

class DataCleaner(BaseTransformer):
    def transform(self, record):
        return {k: (v.strip() if isinstance(v, str) else v) for k, v in record.items()}
    