from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import datetime
from app.services.breaches_service import (
    calculate_breaches, BrechLevel as BreachLevel, BreachesResponse
)

router = APIRouter(tags=["breaches"])

@router.get("/breaches", response_model=BreachesResponse)
def get_breaches(
    date: str = Query(..., description="DD/MM/YYYY"),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1),
    level: Optional[BreachLevel] = Query(None),
):
    try:
        req_date = datetime.strptime(date, "%d/%m/%Y").date()
    except ValueError:
        raise HTTPException(400, "Date must be DD/MM/YYYY")
    full = calculate_breaches(req_date, page, size)
    if level is None:
        return BreachesResponse(
            customer_level=full["customer"],
            sector_level=full["sector"],
            group_level=full["group"],
        )
    resp = BreachesResponse()
    setattr(resp, f"{level}_level", full[level.value])
    return resp
