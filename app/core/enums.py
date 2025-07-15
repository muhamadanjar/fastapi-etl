from enum import Enum

class FileTypeEnum(str, Enum):
    CSV = "CSV"
    EXCEL = "EXCEL"
    JSON = "JSON"
    XML = "XML"
    API = "API"

class JobStatusEnum(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class ValidationStatusEnum(str, Enum):
    UNVALIDATED = "UNVALIDATED"
    VALID = "VALID"
    INVALID = "INVALID"

class QualityResultEnum(str, Enum):
    PASS_ = "PASS"
    FAIL = "FAIL"
    WARNING = "WARNING"
    