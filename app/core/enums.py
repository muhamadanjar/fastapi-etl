from enum import Enum

class FileTypeEnum(str, Enum):
    CSV = "CSV"
    EXCEL = "EXCEL"
    JSON = "JSON"
    XML = "XML"
    API = "API"

class ProcessingStatus(str, Enum):
    """File processing status"""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class DataType(str, Enum):
    """Detected data types for columns"""
    STRING = "STRING"
    NUMBER = "NUMBER"
    DATE = "DATE"
    BOOLEAN = "BOOLEAN"
    EMAIL = "EMAIL"
    PHONE = "PHONE"
    URL = "URL"
    JSON = "JSON"
    UNKNOWN = "UNKNOWN"


class ValidationStatus(str, Enum):
    """Validation status for raw records"""
    UNVALIDATED = "UNVALIDATED"
    VALID = "VALID"
    INVALID = "INVALID"
    NEEDS_REVIEW = "NEEDS_REVIEW"

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
    


class EntityType(str, Enum):
    """Entity types."""
    PERSON = "PERSON"
    PRODUCT = "PRODUCT"
    TRANSACTION = "TRANSACTION"
    EVENT = "EVENT"
    ORGANIZATION = "ORGANIZATION"
    LOCATION = "LOCATION"
    DOCUMENT = "DOCUMENT"
    CUSTOM = "CUSTOM"


class RelationshipType(str, Enum):
    """Relationship types between entities."""
    PARENT_CHILD = "PARENT_CHILD"
    RELATED_TO = "RELATED_TO"
    DEPENDS_ON = "DEPENDS_ON"
    BELONGS_TO = "BELONGS_TO"
    CONTAINS = "CONTAINS"
    REFERENCES = "REFERENCES"
    DERIVED_FROM = "DERIVED_FROM"
    SIMILAR_TO = "SIMILAR_TO"