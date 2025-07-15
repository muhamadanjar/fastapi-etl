from typing import Dict

def validate_required_fields(data: Dict, required_fields: List[str]) -> bool:
    return all(field in data and data[field] is not None for field in required_fields)

