import os, re, calendar
from enum import Enum
from datetime import datetime, date
from typing import List, Optional

import pandas as pd
from fastapi import HTTPException
from pydantic import BaseModel


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    def _clean(c):
        if not isinstance(c, str):
            return c
        s = re.sub(r'[^0-9A-Za-z]+', '_', c.strip())
        return s.strip('_').lower()
    return df.rename(columns=_clean)


def parse_effective_date(filename: str) -> date:
    name = os.path.splitext(os.path.basename(filename))[0]
    mon_abbr, year_str = name.split()
    month_map = {abbr: idx for idx, abbr in enumerate(calendar.month_abbr) if abbr}
    month = month_map[mon_abbr.capitalize()]
    last = calendar.monthrange(int(year_str), month)[1]
    return date(int(year_str), month, last)


def load_data(folder: str):
    custs, facts, rls = [], [], []
    for fname in os.listdir(folder):
        if not fname.lower().endswith(".xlsx"):
            continue
        path = os.path.join(folder, fname)
        eff = parse_effective_date(path)
        c = pd.read_excel(path, sheet_name="CUSTOMER")
        custs.append(clean_column_names(c))

        fr = pd.read_excel(path, sheet_name="fact risk")
        fr = clean_column_names(fr)
        fr['date'] = pd.to_datetime(fr['date'], dayfirst=True).dt.date
        facts.append(fr)

        rl = pd.read_excel(path, sheet_name="Risk Limit")
        rl = clean_column_names(rl)
        rl['effective_date'] = eff
        rls.append(rl)

    customer_df = pd.concat(custs, ignore_index=True).drop_duplicates(['cust_id'])
    fact_df = pd.concat(facts, ignore_index=True)
    rl_df = pd.concat(rls, ignore_index=True)
    return customer_df, fact_df, rl_df


def safe_float(x):
    try:
        f = float(x)
    except:
        return 0.0
    if pd.isna(f) or f in (float('inf'), float('-inf')):
        return 0.0
    return f


# -----------------------------
# Pydantic Models
# -----------------------------

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
    total_exposure: float
    items: List


class BreachLevel(str, Enum):
    customer = "customer"
    sector = "sector"
    group = "group"


class BreachesResponse(BaseModel):
    customer_level: Optional[PagedResponse] = None
    sector_level: Optional[PagedResponse] = None
    group_level: Optional[PagedResponse] = None


# -----------------------------
# Breach Calculation Logic
# -----------------------------

def calculate_breaches(requested_date: date, page: int, size: int):
    cust_df, fact_df, rl_df = load_data("./Sample_Bank_Data")

    exposures = fact_df[fact_df['date'] == requested_date].copy()
    if exposures.empty:
        raise HTTPException(404, "No exposures for that date")

    limits = rl_df[rl_df['effective_date'] == requested_date]
    if limits.empty:
        raise HTTPException(404, "No risk limits for that date")

    cust_limits = (
        limits[['internal_risk_rating','customer_level_limit']]
        .dropna(subset=['internal_risk_rating'])
        .drop_duplicates()
        .rename(columns={'internal_risk_rating': 'rating', 'customer_level_limit': 'exposure_limit'})
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
        .rename(columns={'group_name': 'group_id', 'group_limit': 'exposure_limit'})
    )

    exposures = exposures.drop(columns=['cust_name', 'group'], errors='ignore')
    exposures = exposures.merge(
        cust_df[['cust_id', 'cust_name', 'sector', 'group_id']],
        on='cust_id', how='left'
    )

    # Customer Level
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
                customer_name=row['cust_name'] or "",
                exposure=safe_float(row['exposure']),
                rating=int(row['rating']),
                hc_collateral=safe_float(row.get('total_hc_collateral')),
                provision=safe_float(row['provision']),
                exposure_limit=safe_float(row['exposure_limit']),
                excess_exposure=safe_float(row['excess_exposure']),
            )
            for _, row in cust_page.iterrows()
        ]
    )

    # Sector Level
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
                sector=row['sector'] or "",
                avg_rating=safe_float(row['avg_rating']),
                exposure=safe_float(row['exposure']),
                hc_collateral=safe_float(row['hc_collateral']),
                provision=safe_float(row['provision']),
                exposure_limit=safe_float(row['sector_limit']),
                excess_exposure=safe_float(row['excess_exposure']),
            )
            for _, row in sector_breach.iloc[start:end].iterrows()
        ]
    )

    # Group Level
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
                group_id=int(row['group_id']),
                avg_rating=safe_float(row['avg_rating']),
                exposure=safe_float(row['exposure']),
                hc_collateral=safe_float(row['hc_collateral']),
                provision=safe_float(row['provision']),
                exposure_limit=safe_float(row['exposure_limit']),
                excess_exposure=safe_float(row['excess_exposure']),
            )
            for _, row in grp_breach.iloc[start:end].iterrows()
        ]
    )

    return {
        "customer": cust_resp,
        "sector": sec_resp,
        "group": grp_resp
    }
