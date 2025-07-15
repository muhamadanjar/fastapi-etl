from datetime import datetime

def get_current_timestamp() -> str:
    return datetime.utcnow().isoformat()


def format_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

