import hashlib
import json

def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()

def dict_hash(data: dict) -> str:
    encoded = json.dumps(data, sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()