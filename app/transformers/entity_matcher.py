from app.transformers.base_transformer import BaseTransformer

class EntityMatcher(BaseTransformer):
    def transform(self, record):
        # Dummy matcher example
        record["entity_key"] = record.get("email") or record.get("phone")
        return record