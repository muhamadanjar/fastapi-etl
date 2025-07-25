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
    
class JobStatus(str, Enum):
    """Job status for ETL jobs."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

class JobType(str, Enum):
    """Job types for ETL jobs."""
    EXTRACT = "EXTRACT" 


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



class QualityRuleType(str, Enum):
    """Enum untuk rule type"""
    COMPLETENESS = "COMPLETENESS"
    UNIQUENESS = "UNIQUENESS"
    VALIDITY = "VALIDITY"
    CONSISTENCY = "CONSISTENCY"
    ACCURACY = "ACCURACY"
    TIMELINESS = "TIMELINESS"
    REFERENTIAL_INTEGRITY = "REFERENTIAL_INTEGRITY"


class QualityCheckResult(str, Enum):
    """Enum untuk check result"""
    PASS = "PASS"
    FAIL = "FAIL"
    WARNING = "WARNING"
    SKIP = "SKIP"
    ERROR = "ERROR"

class TransformationType(str, Enum):
    """Enum for transformation types."""
    MAPPING = "MAPPING"
    CALCULATION = "CALCULATION"
    VALIDATION = "VALIDATION"
    ENRICHMENT = "ENRICHMENT"

class MappingType(str, Enum):
    """Enum for mapping types."""
    DIRECT = "DIRECT"
    CALCULATED = "CALCULATED"
    LOOKUP = "LOOKUP"

class NotificationType(str, Enum):
    """Enum for notification types."""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    SUCCESS = "SUCCESS"

class NotificationChannel(str, Enum):
    """Enum for notification channels."""
    EMAIL = "EMAIL"
    SMS = "SMS"
    SLACK = "SLACK"
    WEBHOOK = "WEBHOOK"
    PUSH_NOTIFICATION = "PUSH_NOTIFICATION"

class NotificationStatus(str, Enum):
    """Enum for notification statuses."""
    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"
    DELIVERED = "DELIVERED"
    READ = "READ"