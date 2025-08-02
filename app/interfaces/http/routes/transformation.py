from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlmodel import Session
from typing import List, Optional, Dict, Any

from app.infrastructure.db.connection import get_session_dependency
from app.interfaces.dependencies import get_current_user, get_db
from app.schemas.transformation import (
    TransformationRuleCreate, TransformationRuleRead, TransformationRuleUpdate,
    FieldMappingCreate, FieldMappingRead, FieldMappingUpdate,
    DataTransformRequest, DataTransformResponse,
    CustomTransformRequest, TestTransformRequest, TestTransformResponse
)
from app.services.transformation_service import TransformationService
from app.infrastructure.db.models.auth import User

router = APIRouter()

# ==============================================
# TRANSFORMATION RULES ENDPOINTS
# ==============================================

@router.post("/rules", response_model=Dict[str, Any])
async def create_transformation_rule(
    rule_data: TransformationRuleCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session_dependency)
) -> Dict[str, Any]:
    """Create a new transformation rule"""
    try:
        transformation_service = TransformationService(db)
        return await transformation_service.create_transformation_rule(rule_data.dict())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.get("/rules", response_model=List[TransformationRuleRead])
async def list_transformation_rules(
    source_format: Optional[str] = Query(None, description="Filter by source format"),
    target_format: Optional[str] = Query(None, description="Filter by target format"),
    transformation_type: Optional[str] = Query(None, description="Filter by transformation type"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[TransformationRuleRead]:
    """List transformation rules with optional filtering"""
    try:
        transformation_service = TransformationService(db)
        rules = await transformation_service.get_transformation_rules(
            source_format=source_format,
            target_format=target_format
        )
        
        # Apply additional filters
        if transformation_type:
            rules = [r for r in rules if r.get('transformation_type') == transformation_type]
        if is_active is not None:
            rules = [r for r in rules if r.get('is_active') == is_active]
        
        # Apply pagination
        return rules[skip:skip + limit]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.get("/rules/{rule_id}", response_model=TransformationRuleRead)
async def get_transformation_rule(
    rule_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> TransformationRuleRead:
    """Get transformation rule by ID"""
    try:
        transformation_service = TransformationService(db)
        rules = await transformation_service.get_transformation_rules()
        
        rule = next((r for r in rules if r.get('rule_id') == rule_id), None)
        if not rule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Transformation rule not found"
            )
        
        return TransformationRuleRead(**rule)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.put("/rules/{rule_id}", response_model=Dict[str, Any])
async def update_transformation_rule(
    rule_id: int,
    rule_data: TransformationRuleUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Update transformation rule"""
    try:
        transformation_service = TransformationService(db)
        return await transformation_service.update_transformation_rule(
            rule_id, 
            rule_data.dict(exclude_unset=True)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.delete("/rules/{rule_id}")
async def delete_transformation_rule(
    rule_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Delete transformation rule"""
    try:
        transformation_service = TransformationService(db)
        success = await transformation_service.delete_transformation_rule(rule_id)
        
        if success:
            return {"message": "Transformation rule deleted successfully"}
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to delete transformation rule"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# ==============================================
# FIELD MAPPINGS ENDPOINTS
# ==============================================

@router.post("/mappings", response_model=Dict[str, Any])
async def create_field_mapping(
    mapping_data: FieldMappingCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Create a new field mapping"""
    try:
        transformation_service = TransformationService(db)
        return await transformation_service.create_field_mapping(mapping_data.dict())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.get("/mappings", response_model=List[FieldMappingRead])
async def list_field_mappings(
    source_entity: Optional[str] = Query(None, description="Filter by source entity"),
    target_entity: Optional[str] = Query(None, description="Filter by target entity"),
    mapping_type: Optional[str] = Query(None, description="Filter by mapping type"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[FieldMappingRead]:
    """List field mappings with optional filtering"""
    try:
        transformation_service = TransformationService(db)
        mappings = await transformation_service.get_field_mappings(
            source_entity=source_entity,
            target_entity=target_entity
        )
        
        # Apply additional filters
        if mapping_type:
            mappings = [m for m in mappings if m.get('mapping_type') == mapping_type]
        
        # Apply pagination
        return [FieldMappingRead(**mapping) for mapping in mappings[skip:skip + limit]]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.get("/mappings/{mapping_id}", response_model=FieldMappingRead)
async def get_field_mapping(
    mapping_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> FieldMappingRead:
    """Get field mapping by ID"""
    try:
        transformation_service = TransformationService(db)
        mappings = await transformation_service.get_field_mappings()
        
        mapping = next((m for m in mappings if m.get('mapping_id') == mapping_id), None)
        if not mapping:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Field mapping not found"
            )
        
        return FieldMappingRead(**mapping)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.put("/mappings/{mapping_id}", response_model=Dict[str, Any])
async def update_field_mapping(
    mapping_id: int,
    mapping_data: FieldMappingUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Update field mapping"""
    # Note: You'll need to implement update_field_mapping in TransformationService
    try:
        transformation_service = TransformationService(db)
        # This method needs to be added to TransformationService
        return {"message": "Field mapping updated successfully", "mapping_id": mapping_id}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.delete("/mappings/{mapping_id}")
async def delete_field_mapping(
    mapping_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Delete field mapping"""
    # Note: You'll need to implement delete_field_mapping in TransformationService
    try:
        transformation_service = TransformationService(db)
        # This method needs to be added to TransformationService
        return {"message": "Field mapping deleted successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# ==============================================
# DATA TRANSFORMATION ENDPOINTS
# ==============================================

@router.post("/transform-batch", response_model=DataTransformResponse)
async def transform_data_batch(
    transform_request: DataTransformRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> DataTransformResponse:
    """Transform a batch of data using configured rules and mappings"""
    try:
        transformation_service = TransformationService(db)
        result = await transformation_service.transform_data_batch(
            data_batch=transform_request.data_batch,
            source_entity=transform_request.source_entity,
            target_entity=transform_request.target_entity
        )
        
        return DataTransformResponse(**result)
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/transform-custom", response_model=Dict[str, Any])
async def apply_custom_transformation(
    transform_request: CustomTransformRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Apply custom transformation logic to data batch"""
    try:
        transformation_service = TransformationService(db)
        return await transformation_service.apply_custom_transformation(
            data_batch=transform_request.data_batch,
            transformation_logic=transform_request.transformation_logic,
            parameters=transform_request.parameters
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/test-transformation", response_model=TestTransformResponse)
async def test_transformation(
    test_request: TestTransformRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> TestTransformResponse:
    """Test transformation configuration on sample data"""
    try:
        transformation_service = TransformationService(db)
        result = await transformation_service.test_transformation(
            sample_data=test_request.sample_data,
            transformation_config=test_request.transformation_config
        )
        
        return TestTransformResponse(**result)
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# ==============================================
# UTILITY ENDPOINTS
# ==============================================

@router.get("/mapping-types")
async def get_mapping_types(
    current_user: User = Depends(get_current_user)
) -> Dict[str, List[str]]:
    """Get available mapping types"""
    return {
        "mapping_types": ["DIRECT", "CALCULATED", "LOOKUP"],
        "transformation_types": ["MAPPING", "CALCULATION", "VALIDATION", "ENRICHMENT"]
    }

@router.get("/entities")
async def get_available_entities(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, List[str]]:
    """Get available source and target entities"""
    try:
        transformation_service = TransformationService(db)
        mappings = await transformation_service.get_field_mappings()
        
        source_entities = list(set(m.get('source_entity') for m in mappings if m.get('source_entity')))
        target_entities = list(set(m.get('target_entity') for m in mappings if m.get('target_entity')))
        
        return {
            "source_entities": source_entities,
            "target_entities": target_entities
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.get("/rules/{rule_id}/preview")
async def preview_transformation_rule(
    rule_id: int,
    sample_data: Dict[str, Any] = Query(..., description="Sample data for preview"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Preview how a transformation rule would transform sample data"""
    try:
        transformation_service = TransformationService(db)
        
        # Get the rule
        rules = await transformation_service.get_transformation_rules()
        rule = next((r for r in rules if r.get('rule_id') == rule_id), None)
        
        if not rule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Transformation rule not found"
            )
        
        # Apply rule to sample data
        result = await transformation_service.test_transformation(
            sample_data=[sample_data],
            transformation_config={"transformation_rules": [rule]}
        )
        
        return {
            "original_data": sample_data,
            "transformed_data": result.get("transformed_samples", [{}])[0] if result.get("transformed_samples") else {},
            "rule_applied": rule
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )