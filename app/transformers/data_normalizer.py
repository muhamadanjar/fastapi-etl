from app.transformers.base_transformer import BaseTransformer

class DataNormalizer(BaseTransformer):
    def transform(self, record):
        normalized = {}
        for key, value in record.items():
            if isinstance(value, str):
                normalized[key] = value.lower().strip()
            else:
                normalized[key] = value
        return normalized