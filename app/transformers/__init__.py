# ==============================================
# app/transformers/__init__.py
# ==============================================
from .base_transformer import BaseTransformer
from .data_cleaner import DataCleaner
from .data_normalizer import DataNormalizer
from .data_validator import DataValidator
from .entity_matcher import EntityMatcher
from .aggregator import Aggregator

# Transformer registry for pipeline building
TRANSFORMER_REGISTRY = {
    'cleaner': DataCleaner,
    'data_cleaner': DataCleaner,
    'clean': DataCleaner,
    
    'normalizer': DataNormalizer,
    'data_normalizer': DataNormalizer,
    'normalize': DataNormalizer,
    
    'validator': DataValidator,
    'data_validator': DataValidator,
    'validate': DataValidator,
    
    'matcher': EntityMatcher,
    'entity_matcher': EntityMatcher,
    'match': EntityMatcher,
    
    'aggregator': Aggregator,
    'aggregate': Aggregator,
    'agg': Aggregator,
}

# Transformation pipeline stages
TRANSFORMATION_STAGES = [
    'clean',      # Data cleaning and formatting
    'validate',   # Data validation and quality checks
    'normalize',  # Data normalization and standardization
    'match',      # Entity matching and deduplication
    'aggregate'   # Data aggregation and summarization
]

def get_transformer(transformer_type: str, **kwargs):
    """
    Factory function to get appropriate transformer based on type
    
    Args:
        transformer_type: Type of transformer to create
        **kwargs: Additional arguments to pass to transformer
    
    Returns:
        Transformer instance
    
    Raises:
        ValueError: If transformer type is not supported
    """
    transformer_class = TRANSFORMER_REGISTRY.get(transformer_type.lower())
    
    if not transformer_class:
        supported_types = list(TRANSFORMER_REGISTRY.keys())
        raise ValueError(
            f"Unsupported transformer type: {transformer_type}. "
            f"Supported types: {supported_types}"
        )
    
    return transformer_class(**kwargs)

def create_transformation_pipeline(stages: list, **kwargs):
    """
    Create a transformation pipeline with specified stages
    
    Args:
        stages: List of transformation stages
        **kwargs: Configuration for each stage
    
    Returns:
        List of transformer instances
    """
    pipeline = []
    
    for stage in stages:
        if stage not in TRANSFORMATION_STAGES:
            raise ValueError(f"Unknown transformation stage: {stage}")
        
        # Get stage-specific configuration
        stage_config = kwargs.get(f"{stage}_config", {})
        
        # Create transformer instance
        transformer = get_transformer(stage, **stage_config)
        pipeline.append(transformer)
    
    return pipeline

def get_supported_transformers():
    """Get list of all supported transformer types"""
    return list(TRANSFORMER_REGISTRY.keys())

def is_supported_transformer(transformer_type: str) -> bool:
    """Check if transformer type is supported"""
    return transformer_type.lower() in TRANSFORMER_REGISTRY

def get_transformation_stages():
    """Get list of standard transformation stages"""
    return TRANSFORMATION_STAGES.copy()

__all__ = [
    "BaseTransformer",
    "DataCleaner",
    "DataNormalizer", 
    "DataValidator",
    "EntityMatcher",
    "Aggregator",
    "get_transformer",
    "create_transformation_pipeline",
    "get_supported_transformers",
    "is_supported_transformer",
    "get_transformation_stages",
    "TRANSFORMER_REGISTRY",
    "TRANSFORMATION_STAGES"
]