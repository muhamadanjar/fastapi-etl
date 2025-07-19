# ==============================================
# app/processors/__init__.py
# ==============================================
from .base_processor import BaseProcessor
from .csv_processor import CSVProcessor
from .excel_processor import ExcelProcessor
from .json_processor import JSONProcessor
from .xml_processor import XMLProcessor
from .api_processor import APIProcessor

# Processor registry for dynamic instantiation
PROCESSOR_REGISTRY = {
    'csv': CSVProcessor,
    'text/csv': CSVProcessor,
    'application/csv': CSVProcessor,
    
    'excel': ExcelProcessor,
    'xlsx': ExcelProcessor,
    'xls': ExcelProcessor,
    'application/vnd.ms-excel': ExcelProcessor,
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ExcelProcessor,
    
    'json': JSONProcessor,
    'application/json': JSONProcessor,
    'text/json': JSONProcessor,
    
    'xml': XMLProcessor,
    'application/xml': XMLProcessor,
    'text/xml': XMLProcessor,
    
    'api': APIProcessor,
    'rest_api': APIProcessor,
    'web_api': APIProcessor,
}

def get_processor(file_type: str, **kwargs):
    """
    Factory function to get appropriate processor based on file type
    
    Args:
        file_type: File type or MIME type
        **kwargs: Additional arguments to pass to processor
    
    Returns:
        Processor instance
    
    Raises:
        ValueError: If file type is not supported
    """
    processor_class = PROCESSOR_REGISTRY.get(file_type.lower())
    
    if not processor_class:
        # Try to match partial MIME types
        for mime_type, proc_class in PROCESSOR_REGISTRY.items():
            if mime_type in file_type.lower():
                processor_class = proc_class
                break
    
    if not processor_class:
        supported_types = list(PROCESSOR_REGISTRY.keys())
        raise ValueError(
            f"Unsupported file type: {file_type}. "
            f"Supported types: {supported_types}"
        )
    
    return processor_class(**kwargs)

def get_supported_types():
    """Get list of all supported file types"""
    return list(PROCESSOR_REGISTRY.keys())

def is_supported_type(file_type: str) -> bool:
    """Check if file type is supported"""
    return file_type.lower() in PROCESSOR_REGISTRY

__all__ = [
    "BaseProcessor",
    "CSVProcessor", 
    "ExcelProcessor",
    "JSONProcessor",
    "XMLProcessor",
    "APIProcessor",
    "get_processor",
    "get_supported_types",
    "is_supported_type",
    "PROCESSOR_REGISTRY"
]