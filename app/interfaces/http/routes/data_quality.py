from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlmodel import Session
from typing import List, Optional, Dict, Any
from uuid import UUID

from app.dependencies import get_db, get_current_user
from app.schemas.quality_schemas import (
    QualityRuleCreate, QualityRuleUpdate, QualityRuleResponse,
    QualityCheckResult, QualityReport
)
from app.services.data_quality_service import DataQualityService
from app.models.base import User

router = APIRouter()

@router.post("/rules", response_model=QualityRuleResponse)
async def create_quality_rule(
    rule_data: QualityRuleCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> QualityRuleResponse:
    """Create a new data quality rule"""
    quality_service = DataQualityService(db)
    rule = await quality_service.create_quality_rule(rule_data, current_user.id)
    return QualityRuleResponse.from_orm(rule)

@router.get("/rules", response_model=List[QualityRuleResponse])
async def list_quality_rules(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    rule_type: Optional[str] = Query(None, description="Filter by rule type"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[QualityRuleResponse]:
    """List all data quality rules"""
    quality_service = DataQualityService(db)
    return await quality_service.list_quality_rules(skip, limit, rule_type, entity_type, is_active)

@router.get("/rules/{rule_id}", response_model=QualityRuleResponse)
async def get_quality_rule(
    rule_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> QualityRuleResponse:
    """Get specific quality rule"""
    quality_service = DataQualityService(db)
    rule = await quality_service.get_quality_rule(rule_id)
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Quality rule not found"
        )
    return QualityRuleResponse.from_orm(rule)

@router.put("/rules/{rule_id}", response_model=QualityRuleResponse)
async def update_quality_rule(
    rule_id: UUID,
    rule_data: QualityRuleUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> QualityRuleResponse:
    """Update quality rule"""
    quality_service = DataQualityService(db)
    rule = await quality_service.update_quality_rule(rule_id, rule_data, current_user.id)
    return QualityRuleResponse.from_orm(rule)

@router.delete("/rules/{rule_id}")
async def delete_quality_rule(
    rule_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Delete quality rule"""
    quality_service = DataQualityService(db)
    await quality_service.delete_quality_rule(rule_id, current_user.id)
    return {"message": "Quality rule deleted successfully"}

@router.post("/rules/{rule_id}/execute")
async def execute_quality_rule(
    rule_id: UUID,
    target_data: Optional[str] = Query(None, description="Specific data to check"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> QualityCheckResult:
    """Execute a specific quality rule"""
    quality_service = DataQualityService(db)
    return await quality_service.execute_quality_rule(rule_id, target_data, current_user.id)

@router.post("/check")
async def run_quality_check(
    entity_type: str = Query(..., description="Entity type to check"),
    rules: Optional[List[UUID]] = Query(None, description="Specific rules to run"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[QualityCheckResult]:
    """Run quality checks on specific entity type"""
    quality_service = DataQualityService(db)
    return await quality_service.run_quality_check(entity_type, rules, current_user.id)

@router.get("/results")
async def get_quality_check_results(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    rule_id: Optional[UUID] = Query(None, description="Filter by rule"),
    check_result: Optional[str] = Query(None, regex="^(PASS|FAIL|WARNING)$"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[QualityCheckResult]:
    """Get quality check results with pagination"""
    quality_service = DataQualityService(db)
    return await quality_service.get_quality_check_results(
        skip, limit, rule_id, check_result, entity_type
    )

@router.get("/report")
async def generate_quality_report(
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    period: str = Query("30d", regex="^(7d|30d|90d)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> QualityReport:
    """Generate comprehensive data quality report"""
    quality_service = DataQualityService(db)
    return await quality_service.generate_quality_report(entity_type, period)

@router.get("/score")
async def get_quality_score(
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Get overall data quality score"""
    quality_service = DataQualityService(db)
    return await quality_service.get_quality_score(entity_type)
