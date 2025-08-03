from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlmodel import Session
from typing import List, Optional, Dict, Any

from app.interfaces.dependencies import get_current_user, get_db
from app.schemas.data_quality_schema import (
    QualityRuleCreate, QualityRuleRead, QualityRuleUpdate,
    QualityCheckRequest, QualityCheckResponse,
    ValidationRequest, ValidationResponse,
    QualityReportRequest, QualityReportResponse
)
from app.services.data_quality_service import DataQualityService
from app.infrastructure.db.models.auth import User

router = APIRouter()

# ==============================================
# QUALITY RULES MANAGEMENT
# ==============================================

@router.post("/rules", response_model=Dict[str, Any])
async def create_quality_rule(
    rule_data: QualityRuleCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Create a new data quality rule"""
    try:
        data_quality_service = DataQualityService(db)
        return await data_quality_service.create_quality_rule(rule_data.dict())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.get("/rules", response_model=List[QualityRuleRead])
async def list_quality_rules(
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    rule_type: Optional[str] = Query(None, description="Filter by rule type"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[QualityRuleRead]:
    """List data quality rules with filtering"""
    try:
        data_quality_service = DataQualityService(db)
        return await data_quality_service.get_quality_rules(
            entity_type=entity_type,
            rule_type=rule_type,
            is_active=is_active,
            skip=skip,
            limit=limit
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.get("/rules/{rule_id}", response_model=QualityRuleRead)
async def get_quality_rule(
    rule_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> QualityRuleRead:
    """Get quality rule by ID"""
    try:
        data_quality_service = DataQualityService(db)
        rule = await data_quality_service.get_quality_rule_by_id(rule_id)
        
        if not rule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Quality rule not found"
            )
        
        return rule
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.put("/rules/{rule_id}", response_model=Dict[str, Any])
async def update_quality_rule(
    rule_id: int,
    rule_data: QualityRuleUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Update quality rule"""
    try:
        data_quality_service = DataQualityService(db)
        return await data_quality_service.update_quality_rule(
            rule_id, 
            rule_data.dict(exclude_unset=True)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.delete("/rules/{rule_id}")
async def delete_quality_rule(
    rule_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Delete quality rule"""
    try:
        data_quality_service = DataQualityService(db)
        success = await data_quality_service.delete_quality_rule(rule_id)
        
        if success:
            return {"message": "Quality rule deleted successfully"}
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to delete quality rule"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# ==============================================
# QUALITY CHECKS EXECUTION
# ==============================================

@router.post("/check", response_model=QualityCheckResponse)
async def run_quality_check(
    check_request: QualityCheckRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> QualityCheckResponse:
    """Run quality check on data"""
    try:
        data_quality_service = DataQualityService(db)
        return await data_quality_service.run_quality_check(
            data_batch=check_request.data_batch,
            entity_type=check_request.entity_type,
            rule_ids=check_request.rule_ids,
            check_config=check_request.check_config
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/validate", response_model=ValidationResponse)
async def validate_data(
    validation_request: ValidationRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> ValidationResponse:
    """Validate data against quality rules"""
    try:
        data_quality_service = DataQualityService(db)
        return await data_quality_service.validate_data(
            data_batch=validation_request.data_batch,
            validation_rules=validation_request.validation_rules,
            validation_config=validation_request.validation_config
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/check-entity/{entity_type}")
async def check_entity_quality(
    entity_type: str,
    entity_ids: Optional[List[int]] = None,
    quality_config: Optional[Dict[str, Any]] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> QualityCheckResponse:
    """Run quality check on specific entity type"""
    try:
        data_quality_service = DataQualityService(db)
        return await data_quality_service.check_entity_quality(
            entity_type=entity_type,
            entity_ids=entity_ids,
            quality_config=quality_config or {}
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/check-file/{file_id}")
async def check_file_quality(
    file_id: int,
    validation_rules: Optional[List[Dict[str, Any]]] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> QualityCheckResponse:
    """Run quality check on processed file data"""
    try:
        data_quality_service = DataQualityService(db)
        return await data_quality_service.check_file_quality(
            file_id=file_id,
            validation_rules=validation_rules or []
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/check-job/{job_id}")
async def check_job_quality(
    job_id: int,
    execution_id: Optional[int] = None,
    quality_config: Optional[Dict[str, Any]] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> QualityCheckResponse:
    """Run quality check on ETL job results"""
    try:
        data_quality_service = DataQualityService(db)
        return await data_quality_service.check_job_quality(
            job_id=job_id,
            execution_id=execution_id,
            quality_config=quality_config or {}
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# ==============================================
# QUALITY REPORTS
# ==============================================

@router.post("/report", response_model=QualityReportResponse)
async def generate_quality_report(
    report_request: QualityReportRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> QualityReportResponse:
    """Generate comprehensive quality report"""
    try:
        data_quality_service = DataQualityService(db)
        return await data_quality_service.generate_quality_report(
            entity_type=report_request.entity_type,
            date_range=report_request.date_range,
            report_config=report_request.report_config
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.get("/report/summary")
async def get_quality_summary(
    entity_type: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Get quality summary statistics"""
    try:
        data_quality_service = DataQualityService(db)
        return await data_quality_service.get_quality_summary(
            entity_type=entity_type,
            date_from=date_from,
            date_to=date_to
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.get("/report/trends")
async def get_quality_trends(
    entity_type: Optional[str] = Query(None),
    period: str = Query("week", description="Period: day, week, month"),
    limit: int = Query(30, ge=1, le=365),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Get quality trends over time"""
    try:
        data_quality_service = DataQualityService(db)
        return await data_quality_service.get_quality_trends(
            entity_type=entity_type,
            period=period,
            limit=limit
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# ==============================================
# QUALITY MONITORING
# ==============================================

@router.get("/monitor/alerts")
async def get_quality_alerts(
    severity: Optional[str] = Query(None, description="Filter by severity: low, medium, high, critical"),
    entity_type: Optional[str] = Query(None),
    is_resolved: Optional[bool] = Query(False),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> List[Dict[str, Any]]:
    """Get data quality alerts"""
    try:
        data_quality_service = DataQualityService(db)
        return await data_quality_service.get_quality_alerts(
            severity=severity,
            entity_type=entity_type,
            is_resolved=is_resolved,
            skip=skip,
            limit=limit
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/monitor/alerts/{alert_id}/resolve")
async def resolve_quality_alert(
    alert_id: int,
    resolution_notes: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Resolve quality alert"""
    try:
        data_quality_service = DataQualityService(db)
        success = await data_quality_service.resolve_quality_alert(
            alert_id=alert_id,
            resolution_notes=resolution_notes,
            resolved_by=current_user.id
        )
        
        if success:
            return {"message": "Quality alert resolved successfully"}
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to resolve quality alert"
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

# ==============================================
# UTILITY ENDPOINTS
# ==============================================

@router.get("/rule-types")
async def get_quality_rule_types(
    current_user: User = Depends(get_current_user)
) -> Dict[str, List[str]]:
    """Get available quality rule types"""
    return {
        "rule_types": [
            "COMPLETENESS",    # Check for null/empty values
            "UNIQUENESS",      # Check for duplicates
            "VALIDITY",        # Check data format/pattern
            "CONSISTENCY",     # Check data consistency
            "ACCURACY",        # Check data accuracy
            "INTEGRITY",       # Check referential integrity
            "TIMELINESS",      # Check data freshness
            "CUSTOM"           # Custom validation logic
        ],
        "severity_levels": ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
        "validation_actions": ["WARN", "FAIL", "SKIP", "CORRECT"]
    }

@router.get("/metrics")
async def get_quality_metrics(
    entity_type: Optional[str] = Query(None),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Get quality metrics dashboard data"""
    try:
        data_quality_service = DataQualityService(db)
        return await data_quality_service.get_quality_metrics(entity_type=entity_type)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/schedule-check")
async def schedule_quality_check(
    entity_type: str,
    schedule_config: Dict[str, Any],
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> Dict[str, str]:
    """Schedule recurring quality checks"""
    try:
        data_quality_service = DataQualityService(db)
        job_id = await data_quality_service.schedule_quality_check(
            entity_type=entity_type,
            schedule_config=schedule_config,
            scheduled_by=current_user.id
        )
        
        return {
            "message": "Quality check scheduled successfully",
            "job_id": str(job_id)
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )