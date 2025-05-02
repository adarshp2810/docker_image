
import os
import re
import calendar
from enum import Enum
from datetime import datetime, date
from typing import List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ─── Models ────────────────────────────────────────────────────────────────────
class CustomerItem(BaseModel):
    customer_name: str
    exposure: float
    rating: int
    hc_collateral: float
    provision: float
    exposure_limit: float
    excess_exposure: float

class SectorItem(BaseModel):
    sector: str
    avg_rating: float
    exposure: float
    hc_collateral: float
    provision: float
    exposure_limit: float
    excess_exposure: float

class GroupItem(BaseModel):
    group_id: int
    avg_rating: float
    exposure: float
    hc_collateral: float
    provision: float
    exposure_limit: float
    excess_exposure: float

class PagedResponse(BaseModel):
    page: int
    page_size: int
    total: int
    total_exposure: float  # sum of excess exposures
    items: List

class BreachLevel(str, Enum):
    customer = "customer"
    sector   = "sector"
    group    = "group"

class BreachesResponse(BaseModel):
    customer_level: Optional[PagedResponse] = None
    sector_level:   Optional[PagedResponse] = None
    group_level:    Optional[PagedResponse] = None

# ─── Helpers ───────────────────────────────────────────────────────────────────
def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to snake_case."""
    def _clean(c):
        if not isinstance(c, str):
            return c
        s = re.sub(r'[^0-9A-Za-z]+', '_', c.strip())
        return s.strip('_').lower()
    return df.rename(columns=_clean)

def safe_float(x) -> float:
    """Coerce NaN/inf into JSON-safe floats."""
    try:
        f = float(x)
    except Exception:
        return 0.0
    if pd.isna(f) or f in (float('inf'), float('-inf')):
        return 0.0
    return f


def parse_effective_date(filename: str) -> date:
    """From 'DEC 2023.xlsx' derive date(2023,12,31)."""
    name = os.path.splitext(os.path.basename(filename))[0]
    parts = name.split()
    if len(parts) != 2:
        raise ValueError(f"Filename '{filename}' not in 'MON YYYY.xlsx' format")
    mon_abbr, year_str = parts[0].capitalize(), parts[1]
    month_map = {abbr: idx for idx, abbr in enumerate(calendar.month_abbr) if abbr}
    if mon_abbr not in month_map:
        raise ValueError(f"Unknown month '{mon_abbr}' in '{filename}'")
    year, month = int(year_str), month_map[mon_abbr]
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, last_day)


def load_data(folder: str):
    """
    Read all .xlsx in folder, return:
      - customer_df
      - fact_df
      - rl_df
    """
    cust_list, fr_list, rl_list = [], [], []
    for fname in os.listdir(folder):
        if not fname.lower().endswith(".xlsx"):
            continue
        path = os.path.join(folder, fname)
        eff_date = parse_effective_date(path)

        # CUSTOMER
        c = pd.read_excel(path, sheet_name="CUSTOMER")
        cust_list.append(clean_column_names(c))

        # fact risk
        fr = pd.read_excel(path, sheet_name="fact risk")
        fr = clean_column_names(fr)
        fr['date'] = pd.to_datetime(fr['date'], dayfirst=True).dt.date
        fr_list.append(fr)

        # Risk Limit
        rl = pd.read_excel(path, sheet_name="Risk Limit")
        rl = clean_column_names(rl)
        rl['effective_date'] = eff_date
        rl_list.append(rl)

    customer_df = pd.concat(cust_list, ignore_index=True).drop_duplicates(subset=['cust_id'])
    fact_df     = pd.concat(fr_list, ignore_index=True)
    rl_df       = pd.concat(rl_list, ignore_index=True)
    return customer_df, fact_df, rl_df


def calculate_breaches(requested_date: date, page: int, size: int):
    cust_df, fact_df, rl_df = load_data(r"D:\FAST API\Sample_Bank_Data")

    # 1) exposures at date
    exposures = fact_df[fact_df['date'] == requested_date].copy()
    if exposures.empty:
        raise HTTPException(404, "No exposures for that date")

    # 2) limits at date
    limits = rl_df[rl_df['effective_date'] == requested_date]
    if limits.empty:
        raise HTTPException(404, "No risk limits for that date")

    # 3) build lookup tables
    cust_limits = (
        limits[['internal_risk_rating','customer_level_limit']]
        .dropna(subset=['internal_risk_rating'])
        .drop_duplicates()
        .rename(columns={
            'internal_risk_rating':'rating',
            'customer_level_limit':'exposure_limit'
        })
    )
    sector_limits = (
        limits[['sector','sector_limit']]
        .dropna(subset=['sector'])
        .drop_duplicates()
    )
    group_limits = (
        limits[['group_name','group_limit']]
        .dropna(subset=['group_name'])
        .drop_duplicates()
        .rename(columns={
            'group_name':'group_id',
            'group_limit':'exposure_limit'
        })
    )

    # 4) enrich exposures with customer mapping
    exposures = exposures.drop(columns=['cust_name','group'], errors='ignore')
    exposures = exposures.merge(
        cust_df[['cust_id','cust_name','sector','group_id']],
        on='cust_id', how='left'
    )

    # ─── Customer-level ─────────────────────────────────────────────────────────
    cust = exposures.merge(cust_limits, on='rating', how='left')
    cust['excess_exposure'] = cust['exposure'] - cust['exposure_limit']
    cust_breach = cust[cust['exposure'] > cust['exposure_limit']]

    start, end = (page-1)*size, page*size
    cust_page = cust_breach.iloc[start:end]

    cust_resp = PagedResponse(
        page=page,
        page_size=size,
        total=len(cust_breach),
        total_exposure=safe_float(cust_breach['excess_exposure'].sum()),
        items=[
            CustomerItem(
                customer_name  = row['cust_name'] or "",
                exposure       = safe_float(row['exposure']),
                rating         = int(row['rating']),
                hc_collateral  = safe_float(row.get('total_hc_collateral')),
                provision      = safe_float(row['provision']),
                exposure_limit = safe_float(row['exposure_limit']),
                excess_exposure= safe_float(row['excess_exposure']),
            )
            for _, row in cust_page.iterrows()
        ]
    )

    # ─── Sector-level with weighted avg rating ─────────────────────────────────
    sector_agg = cust_breach.groupby('sector').apply(
        lambda df: pd.Series({
            'exposure': df['exposure'].sum(),
            'hc_collateral': df['total_hc_collateral'].sum(),
            'provision': df['provision'].sum(),
            'avg_rating': (df['rating'] * df['exposure']).sum() / df['exposure'].sum()
        })
    ).reset_index()
    sector = sector_agg.merge(sector_limits, on='sector', how='left')
    sector['excess_exposure'] = sector['exposure'] - sector['sector_limit']
    sector_breach = sector[sector['exposure'] > sector['sector_limit']]

    sec_resp = PagedResponse(
        page=page,
        page_size=size,
        total=len(sector_breach),
        total_exposure=safe_float(sector_breach['excess_exposure'].sum()),
        items=[
            SectorItem(
                sector         = row['sector'] or "",
                avg_rating     = safe_float(row['avg_rating']),
                exposure       = safe_float(row['exposure']),
                hc_collateral  = safe_float(row['hc_collateral']),
                provision      = safe_float(row['provision']),
                exposure_limit = safe_float(row['sector_limit']),
                excess_exposure= safe_float(row['excess_exposure']),
            )
            for _, row in sector_breach.iloc[start:end].iterrows()
        ]
    )

    # ─── Group-level with weighted avg rating ──────────────────────────────────
    group_agg = cust_breach.groupby('group_id').apply(
        lambda df: pd.Series({
            'exposure': df['exposure'].sum(),
            'hc_collateral': df['total_hc_collateral'].sum(),
            'provision': df['provision'].sum(),
            'avg_rating': (df['rating'] * df['exposure']).sum() / df['exposure'].sum()
        })
    ).reset_index()
    grp = group_agg.merge(group_limits, on='group_id', how='left')
    grp['excess_exposure'] = grp['exposure'] - grp['exposure_limit']
    grp_breach = grp[grp['exposure'] > grp['exposure_limit']]

    grp_resp = PagedResponse(
        page=page,
        page_size=size,
        total=len(grp_breach),
        total_exposure=safe_float(grp_breach['excess_exposure'].sum()),
        items=[
            GroupItem(
                group_id       = int(row['group_id']),
                avg_rating     = safe_float(row['avg_rating']),
                exposure       = safe_float(row['exposure']),
                hc_collateral  = safe_float(row['hc_collateral']),
                provision      = safe_float(row['provision']),
                exposure_limit = safe_float(row['exposure_limit']),
                excess_exposure= safe_float(row['excess_exposure']),
            )
            for _, row in grp_breach.iloc[start:end].iterrows()
        ]
    )

    return {
        "customer": cust_resp,
        "sector":   sec_resp,
        "group":    grp_resp
    }

# ─── FastAPI App ──────────────────────────────────────────────────────────────
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/breaches", response_model=BreachesResponse)
def get_breaches(
    date: str = Query(..., description="DD/MM/YYYY"),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1),
    level: Optional[BreachLevel] = Query(
        None, description="Filter by breach level: customer, sector, or group"
    )
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
            group_level=full["group"]
        )

    resp = BreachesResponse()
    if level == BreachLevel.customer:
        resp.customer_level = full["customer"]
    elif level == BreachLevel.sector:
        resp.sector_level = full["sector"]
    else:
        resp.group_level = full["group"]
    return resp

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", port=8000, reload=True)