import os, re, calendar
from enum import Enum
from datetime import datetime, date
from typing import List, Optional

import pandas as pd
from fastapi import HTTPException
from pydantic import BaseModel

# File: app/services/breaches_service.py

def calculate_breaches(requested_date: date, page: int, size: int):
    # your full logic here...
    return {
        "customer": cust_resp,
        "sector": sec_resp,
        "group": grp_resp
    }


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





# Pydantic models
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
