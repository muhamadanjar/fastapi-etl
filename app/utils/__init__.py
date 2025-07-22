"""
Utilities module for ETL project.
Contains common utility functions and helpers.
"""

from .logger import get_logger, setup_logging
from .security import (
    hash_password, 
    verify_password, 
    create_access_token, 
    decode_access_token,
    generate_random_token
)
from .file_utils import (
    get_file_extension,
    sanitize_filename,
    get_file_size,
    create_directory,
    delete_file_safely,
    calculate_file_hash
)
from .date_utils import (
    get_current_timestamp,
    format_datetime,
    parse_datetime,
    get_date_range,
    calculate_duration
)
from .validation_utils import (
    validate_email,
    validate_phone,
    validate_json,
    validate_csv_headers,
    sanitize_input
)
# from .hash_utils import (
#     generate_hash,
#     verify_hash,
#     generate_uuid,
#     hash_data
# )
# from .exception_utils import (
#     handle_exception,
#     format_error_message,
#     log_exception
# )

__all__ = [
    "get_logger",
    "setup_logging",
    "hash_password",
    "verify_password", 
    "create_access_token",
    "decode_access_token",
    "generate_random_token",
    "get_file_extension",
    "sanitize_filename",
    "get_file_size",
    "create_directory",
    "delete_file_safely",
    "calculate_file_hash",
    "get_current_timestamp",
    "format_datetime",
    "parse_datetime",
    "get_date_range",
    "calculate_duration",
    "validate_email",
    "validate_phone",
    "validate_json",
    "validate_csv_headers",
    "sanitize_input",
    # "generate_hash",
    # "verify_hash",
    # "generate_uuid",
    # "hash_data",
    # "handle_exception",
    # "format_error_message",
    # "log_exception"
]