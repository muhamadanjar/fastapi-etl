from app.transformers.base_transformer import BaseTransformer

class DataValidator(BaseTransformer):
    def __init__(self, required_fields: list[str]):
        self.required_fields = required_fields

    def transform(self, record):
        errors = []
        for field in self.required_fields:
            if not record.get(field):
                errors.append(f"Missing field: {field}")
        record["_validation_errors"] = errors
        record["_is_valid"] = len(errors) == 0
        return record