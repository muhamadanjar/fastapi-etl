from collections import defaultdict
from typing import List, Dict

class Aggregator:
    def aggregate(self, records: List[Dict], key: str, field: str, method: str = "sum") -> Dict[str, float]:
        result = defaultdict(float)
        for record in records:
            group = record.get(key)
            value = record.get(field, 0)
            try:
                result[group] += float(value)
            except (ValueError, TypeError):
                continue
        return dict(result)