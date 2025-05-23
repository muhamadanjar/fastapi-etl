import logging
import sys
import os
from pythonjsonlogger import jsonlogger

def setup_logging():
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_format = os.getenv("LOG_FORMAT", "plain")  # "json" or "plain"

    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)

    if log_format == "json":
        formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s"
        )
    else:
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)
