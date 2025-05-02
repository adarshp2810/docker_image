
from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List
from app.services.query_service import RiskDataModelService

router = APIRouter(prefix="/api", tags=["query"])
risk_model = RiskDataModelService(data_folder="./Sample_Bank_Data")

@router.get("/distinct_values")
def get_distinct_values(
    column: str = Query(..., description="Field name like 'staging', 'date', 'cust_name'")
):
    try:
        return {column: risk_model.get_distinct_values(column)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/sum_by_dimension")
def get_sum_by_dimension(
    fact_fields: str = Query(..., description="Comma-separated list of fact fields"),
    group_by_fields: Optional[str] = Query(None),
    date_filter: Optional[str] = Query(None),
    dimension_filter_field: Optional[str] = Query(None),
    dimension_filter_value: Optional[str] = Query(None),
):
    try:
        facts = [f.strip() for f in fact_fields.split(",")]
        groups = [g.strip() for g in group_by_fields.split(",")] if group_by_fields else None
        return risk_model.get_sum_by_dimension(
            fact_fields=facts,
            group_by_fields=groups,
            date_filter=date_filter,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
