import os
import re
import calendar
import logging
from enum import Enum
from datetime import datetime, date
from typing import List, Optional, Union, Dict, Literal, Any

import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel, Field, RootModel
from dateutil.relativedelta import relativedelta
from natsort import natsorted
from urllib.parse import unquote

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CustomerItem(BaseModel):
    customer_name: str = Query(..., description="The name or identifier of the customer, e.g., 'SABIC' or 'QNB Group'")
    exposure: float = Query(..., description="The total financial exposure for the customer, representing the amount at risk, e.g., 101610694")
    rating: int = Query(..., description="The risk rating assigned to the customer, indicating creditworthiness or risk level, e.g., 1 for high rating")
    hc_collateral: float = Query(..., description="The value of held collateral after applying haircuts to account for risk, e.g., 105120194.5")
    provision: float = Query(..., description="The amount set aside to cover potential losses for the customer, e.g., 215184.64184")
    exposure_limit: float = Query(..., description="The maximum allowable exposure for the customer based on their risk rating, e.g., 120000000")
    excess_exposure: float = Query(..., description="The amount by which the customer's exposure exceeds their exposure limit, positive if exposure > limit")

class SectorItem(BaseModel):
    sector: str = Query(..., description="The name of the industry sector, e.g., 'Financials' or 'Telecommunications'")
    avg_rating: float = Query(..., description="The average risk rating across customers in the sector, weighted by exposure, calculated as (sum(rating * exposure) / sum(exposure))")
    exposure: float = Query(..., description="The total financial exposure for all customers in the sector, sum of customer exposures")
    hc_collateral: float = Query(..., description="The total value of haircut collateral for all customers in the sector, sum of total_hc_collateral")
    provision: float = Query(..., description="The total provision amount set aside for potential losses in the sector, sum of customer provisions")
    exposure_limit: float = Query(..., description="The maximum allowable exposure for the sector, as defined in risk limits, e.g., 3836000000 for Financials")
    excess_exposure: float = Query(..., description="The amount by which the sector's total exposure exceeds its exposure limit, positive if exposure > limit")

class GroupItem(BaseModel):
    group_id: int = Query(..., description="The unique identifier for the customer group, e.g., 1 for SABIC and Almarai group")
    avg_rating: float = Query(..., description="The average risk rating across customers in the group, weighted by exposure, calculated as (sum(rating * exposure) / sum(exposure))")
    exposure: float = Query(..., description="The total financial exposure for all customers in the group, sum of customer exposures")
    hc_collateral: float = Query(..., description="The total value of haircut collateral for all customers in the group, sum of total_hc_collateral")
    provision: float = Query(..., description="The total provision amount set aside for potential losses in the group, sum of customer provisions")
    exposure_limit: float = Query(..., description="The maximum allowable exposure for the group, as defined in risk limits, e.g., 540000000 for group 1")
    excess_exposure: float = Query(..., description="The amount by which the group's total exposure exceeds its exposure limit, positive if exposure > limit")

class PagedResponse(BaseModel):
    page: int = Query(..., description="The current page number of the paginated response, e.g., 1 for the first page")
    page_size: int = Query(..., description="The number of items per page, e.g., 10 for default page size")
    total: int = Query(..., description="The total number of items across all pages for the query, e.g., 50 if 50 breaches exist")
    total_exposure: float = Query(..., description="The sum of excess exposure for all items in the query, not just the current page")
    items: List = Query(..., description="A list of items for the current page, containing CustomerItem, SectorItem, or GroupItem instances, e.g., 10 CustomerItem objects")

class BreachLevel(str, Enum):
    customer = "customer"
    sector = "sector"
    group = "group"

class BreachesResponse(BaseModel):
    customer_level: Optional[PagedResponse] = Query(None, description="Paginated response for customer-level breaches, list of CustomerItem, null if not requested or no breaches")
    sector_level: Optional[PagedResponse] = Query(None, description="Paginated response for sector-level breaches, list of SectorItem, null if not requested or no breaches")
    group_level: Optional[PagedResponse] = Query(None, description="Paginated response for group-level breaches, list of GroupItem, null if not requested or no breaches")
 
class CollateralDistributionResponse(RootModel[Dict[str, List[Dict[str, Union[str, int, float]]]]]):
    pass

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    def _clean(c):
        if not isinstance(c, str):
            return c
        s = re.sub(r'[^0-9A-Za-z]+', '_', c.strip())
        return s.strip('_').lower()
    return df.rename(columns=_clean)

def safe_float(x) -> float:
    try:
        f = float(x)
    except Exception:
        return 0.0
    if pd.isna(f) or f in (float('inf'), float('-inf')):
        return 0.0
    return f

def parse_effective_date(filename: str) -> date:
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

def parse_custom_ecl_param(param_str: str) -> dict:
        try:
            return {
                int(k.strip()): int(v.strip())
                for k, v in (pair.split(":") for pair in param_str.split(",") if ":" in pair)
            }
        except Exception:
            raise ValueError("Invalid format for 'customize_additional_provision_percent'. Use format like '1:2,3:5'")
        
def load_data(folder: str, normalize_cols=True):
    if not os.path.exists(folder):
        raise FileNotFoundError(f"Data folder '{folder}' does not exist. Please ensure 'Sample_Bank_Data' is in the repository root.")
    
    all_data = {"fact_risk": [], "customer": [], "risk_limit": [], "rating": [], "fact_restr":[], "written_off":[],"collateral": []}

    for filename in os.listdir(folder):
        if filename.endswith(".xlsx") and not filename.startswith("~$"):
            file_path = os.path.join(folder, filename)
            try:
                eff_date = parse_effective_date(file_path)
                xls = pd.ExcelFile(file_path)
                logger.info(f"Processing file: {filename} with effective date {eff_date}")

                if "fact risk" in xls.sheet_names:
                    df_fact = xls.parse("fact risk")
                    if normalize_cols:
                        df_fact.columns = [str(c).strip().lower().replace(" ", "_") for c in df_fact.columns]
                    if "date" in df_fact.columns:
                        df_fact["date"] = pd.to_datetime(df_fact["date"], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y')
                    for col in df_fact.columns:
                        if not pd.api.types.is_numeric_dtype(df_fact[col]):
                            df_fact[col] = df_fact[col].astype("object")
                    df_fact["source_file"] = filename
                    all_data["fact_risk"].append(df_fact)

                if "CUSTOMER" in xls.sheet_names:
                    df_cust = xls.parse("CUSTOMER")
                    if normalize_cols:
                        df_cust.columns = [str(c).strip().lower().replace(" ", "_") for c in df_cust.columns]
                    df_cust["source_file"] = filename
                    all_data["customer"].append(df_cust)

                if "Risk Limit" in xls.sheet_names:
                    rl = xls.parse("Risk Limit")
                    if normalize_cols:
                        rl = clean_column_names(rl)
                    rl['effective_date'] = eff_date.strftime('%d/%m/%Y')
                    all_data["risk_limit"].append(rl)

                if "PD" in xls.sheet_names:
                    df_rating = xls.parse("PD")
                    if normalize_cols:
                        df_rating.columns = [str(c).strip().lower().replace(" ", "_") for c in df_rating.columns]
                    df_rating["source_file"] = filename
                    all_data["rating"].append(df_rating)

                if "fact restructred" in xls.sheet_names:
                    df_restr = xls.parse("fact restructred", header=1)
                    if normalize_cols:
                        df_restr.columns = [str(c).strip().lower().replace(" ", "_") for c in df_restr.columns]

                    for col_to_drop in ["cust_name", "group"]:
                        if col_to_drop in df_restr.columns:
                            df_restr = df_restr.drop(columns=[col_to_drop])
                            
                    for date_col in ["restrcuturing_date", "1st_repayment_date"]: 
                        if date_col in df_restr.columns:df_restr[date_col] = pd.to_datetime(df_restr[date_col], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y')

                    df_restr["source_file"] = filename
                    all_data["fact_restr"].append(df_restr)

                if "Fact writeen-off" in xls.sheet_names:
                    df_restr = xls.parse("Fact writeen-off", header=1)
                    if normalize_cols:
                        df_restr.columns = [str(c).strip().lower().replace(" ", "_") for c in df_restr.columns]
                    if "date" in df_fact.columns:
                        df_fact["date"] = pd.to_datetime(df_fact["date"], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y')
                    df_restr["source_file"] = filename
                    all_data["written_off"].append(df_restr)

                if "Collateral Details" in xls.sheet_names:
                    df_coll = xls.parse("Collateral Details")
                    if normalize_cols:
                        df_coll.columns = [str(c).strip().lower().replace(" ", "_") for c in df_coll.columns]
                    logger.info(f"[{filename}] Collateral columns: {df_coll.columns.tolist()}")
                    df_coll["date"] = pd.to_datetime(df_coll["date"], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y')
                    all_data["collateral"].append(df_coll)

            except Exception as e:
                logger.error(f"Error loading {filename}: {e}")

    merged_data = {
        "fact_risk": pd.concat(all_data["fact_risk"], ignore_index=True) if all_data["fact_risk"] else None,
        "customer": pd.concat(all_data["customer"], ignore_index=True).drop_duplicates(subset=['cust_id']),
        "risk_limit": pd.concat(all_data["risk_limit"], ignore_index=True) if all_data["risk_limit"] else None,
        "rating": pd.concat(all_data["rating"], ignore_index=True) if all_data["rating"] else None,
        "fact_restr": pd.concat(all_data["fact_restr"], ignore_index=True) if all_data["fact_restr"] else None,
        "written_off": pd.concat(all_data["written_off"], ignore_index=True) if all_data["written_off"] else None,
        "collateral": pd.concat(all_data["collateral"], ignore_index=True) if all_data["collateral"] else None
    }

    if merged_data["fact_risk"] is not None:
        logger.info(f"Fact risk data loaded with {len(merged_data['fact_risk'])} rows, dates: {merged_data['fact_risk']['date'].unique()}")
    if merged_data["risk_limit"] is not None:
        logger.info(f"Risk limit data loaded with {len(merged_data['risk_limit'])} rows, dates: {merged_data['risk_limit']['effective_date'].unique()}")
    
    return (
        merged_data["customer"],
        merged_data["fact_risk"],
        merged_data["risk_limit"],
        merged_data["rating"],
        merged_data["fact_restr"],
        merged_data["written_off"],
        merged_data["collateral"]
    )

class RiskDataModel:
    def __init__(self, customer_df, fact_df, rl_df, rating_df, factrestr_df, written_df,collateral_df):
        self.df_fact_risk = fact_df
        self.df_customer = customer_df
        self.rl_df = rl_df
        self.df_rating = rating_df
        self.df_restr=factrestr_df
        self.df_written=written_df
        self.df_collateral = collateral_df
        self.data_folder = DATA_FOLDER  # wherever you pass it from main


        if self.df_customer is not None and "cust_name" in self.df_customer.columns:
            self.df_customer = self.df_customer.drop(columns=["cust_name"])

        self._join_data()
        self._join_collateral()
        self.valid_collateral_types = []
        self.valid_categories = []
        self.valid_subcategories = []
        self._set_valid_values()
        
    def _set_valid_values(self):
        if self.df_collateral_joined is not None:
            cols = self.df_collateral_joined.columns
            self.valid_collateral_types = (
                sorted(self.df_collateral_joined["collateral_type"].dropna().unique())
                if "collateral_type" in cols else []
            )
            self.valid_categories = (
                sorted(self.df_collateral_joined["collateral_category"].dropna().unique())
                if "collateral_category" in cols else []
            )
            self.valid_subcategories = (
                sorted(self.df_collateral_joined["collateral_sub-category"].dropna().unique())
                if "collateral_sub-category" in cols else []
            )


    def _join_data(self):
        if self.df_fact_risk is None or self.df_customer is None:
            self.df_joined = None
            return

        self.df_joined = pd.merge(
            self.df_fact_risk,
            self.df_customer,
            how="left",
            on="cust_id"
        )

        if "cust_name_x" in self.df_joined.columns and "cust_name_y" in self.df_joined.columns:
            self.df_joined = self.df_joined.drop(columns=["cust_name_y"])
            self.df_joined = self.df_joined.rename(columns={"cust_name_x": "cust_name"})

        self.df_joined["date"] = pd.to_datetime(self.df_joined["date"], errors="coerce", dayfirst=True)
        self.df_joined["year"] = self.df_joined["date"].dt.year

        if self.df_rating is not None:
            self.df_rating["year"] = pd.to_numeric(self.df_rating["year"], errors="coerce")
            self.df_rating = self.df_rating.drop_duplicates(subset=["internal_rating", "year"])

            self.df_joined = pd.merge(
                self.df_joined,
                self.df_rating,
                how="left",
                left_on=["rating", "year"],
                right_on=["internal_rating", "year"]
            )
        self.df_joined["coverage_ratio"] = round(self.df_joined["coverage_ratio"]*100, 2)
        self.df_joined["ecl"] = round(self.df_joined["ecl"]*100, 2)
        self.df_joined = self.df_joined.rename(columns={col: f'additional_provision_at_{int(float(col)*100)}_percent'
                   for col in self.df_joined.columns
                   if col.replace('.', '', 1).isdigit()})
        
        new_cols = {}
        for pct in range(1, 101):
            col_name = f"additional_provision_at_{pct}_percent"
            if col_name not in self.df_joined.columns:
                new_cols[col_name] = (
                    (self.df_joined["exposure"] * (pct / 100) - self.df_joined["provision"])
                    .apply(lambda x: x if x > 0 else 0)
                )

        if new_cols:
            self.df_joined = pd.concat([self.df_joined, pd.DataFrame(new_cols)], axis=1)
        
        if self.df_restr is not None:
            if "cust_id" in self.df_restr.columns and  "date" in self.df_restr.columns and  "restrcuturing_date" in self.df_restr.columns:
                self.df_restr["date"] = pd.to_datetime(self.df_restr["date"], errors="coerce", dayfirst=True)
                self.df_restr["restrcuturing_date"] = pd.to_datetime(self.df_restr["restrcuturing_date"], errors="coerce", dayfirst=True)
                self.df_restr["1st_repayment_date"] = pd.to_datetime(self.df_restr["1st_repayment_date"], errors="coerce", dayfirst=True)
                self.df_joined["date"] = pd.to_datetime(self.df_joined["date"], errors="coerce", dayfirst=True)
                self.df_restr["loan_restr_flag"] = self.df_restr["restrcuturing_date"].apply(lambda x: "yes" if pd.notnull(x) else "no")

                self.df_restr = self.df_restr[["cust_id", "date", "restrcuturing_date", "1st_repayment_date", "loan_restr_flag"]].drop_duplicates()
                self.df_joined = pd.merge(self.df_joined,self.df_restr,how="left",on=["cust_id", "date"])
                self.df_joined["loan_restr_flag"] = self.df_joined["loan_restr_flag"].fillna("no")

    def _join_collateral(self):
        if self.df_collateral is None or self.df_customer is None:
            self.df_collateral_joined = None
            return

        self.df_collateral_joined = pd.merge(
            self.df_collateral,
            self.df_customer,
            how="left",
            left_on="customer_id",
            right_on="cust_id"
        )

        self.df_collateral_joined["date"] = pd.to_datetime(self.df_collateral_joined["date"], errors="coerce", dayfirst=True)
        self.df_collateral_joined["year"] = self.df_collateral_joined["date"].dt.year

        if self.df_rating is not None:
            self.df_rating["year"] = pd.to_numeric(self.df_rating["year"], errors="coerce")
            self.df_rating = self.df_rating.drop_duplicates(subset=["internal_rating", "year"])

            self.df_collateral_joined = pd.merge(
                self.df_collateral_joined,
                self.df_rating,
                how="left",
                left_on=["year"],
                right_on=["year"]
            )

    def get_distinct_values(self, column_name):
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()

        if column_name not in df.columns:
            raise ValueError(f"Field '{column_name}' is not found.")

        series = df[column_name].dropna()
        
        if pd.api.types.is_datetime64_any_dtype(series):
            parsed = pd.to_datetime(series, errors='coerce', dayfirst=True).dropna().unique()
            parsed_sorted = sorted(parsed.tolist())  # Sort datetime objects first (chronological order)
            return [dt.strftime('%d-%m-%Y') for dt in parsed_sorted]  # convert to strings
            
        elif pd.api.types.is_numeric_dtype(series):
            return sorted(series.unique())
        elif pd.api.types.is_object_dtype(series):
            # Clean strings only if they're string-like
            cleaned_series = pd.Series(series).apply(
                lambda x: str(x).strip() if isinstance(x, str) else str(x))
            try:
                return natsorted(cleaned_series.unique())
            except Exception:
                raise ValueError("Inconsistent values found; cannot sort reliably.")
        #complex type,bool and other (list)
        else:
            try:
                return natsorted(pd.Series(series).astype(str).unique())
            except Exception:
                raise ValueError("Unsupported column type for sorting.")

    def get_sum_by_dimension(self,
    fact_field: str,
    group_by_field: Optional[str] = None,
    date_filter: Optional[str] = None,
    dimension_filter_field: Optional[str] = None,
    dimension_filter_value: Optional[str] = None):
        
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
        
        fields = {"group", "group_id", "cust_id","rating","internal_rating"}
        if  fact_field:
            if fact_field not in df.columns:
                raise ValueError(f"Fact field '{fact_field}' is not found.")
            
    
            if (not pd.api.types.is_numeric_dtype(df[fact_field]) or fact_field in fields):
                raise ValueError(f"Fact field must be valid numeric field.")
            
        if group_by_field:
            if group_by_field not in df.columns:
                raise ValueError(f"Group by field '{group_by_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[group_by_field]) and group_by_field not in fields:
                raise ValueError(f"Numeric field '{group_by_field}' is not allowed as a group by field.")

        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        if date_filter:
            try:
                date_obj = pd.to_datetime(date_filter, dayfirst=True)
                df = df[df["date"] == date_obj]
            except Exception:
                raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy' format.")
            
            
        if dimension_filter_field:
            if dimension_filter_field not in df.columns:
                raise ValueError(f"Dimension filter '{dimension_filter_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[dimension_filter_field]) and dimension_filter_field not in fields:
                raise ValueError(f"Numeric field '{dimension_filter_field}' is not allowed as a dimension filter.")

            if dimension_filter_value:
                is_valid = ((dimension_filter_field in fields  and
                 int(dimension_filter_value) in df[dimension_filter_field].dropna().unique()) or
                (str(dimension_filter_value) in df[dimension_filter_field].astype(str).unique()))
                if not is_valid:
                    raise ValueError(f"Dimension value '{dimension_filter_value}' is not found in the dimension filter '{dimension_filter_field}'.")

                if dimension_filter_field in fields:
                    df = df[df[dimension_filter_field] == int(dimension_filter_value)]
                else:
                    df = df[df[dimension_filter_field].astype(str) == str(dimension_filter_value)]
            else:
                raise ValueError("Dimension filter value is missing for the given dimension filter.")
             
        if group_by_field:
            agg_df = df.groupby(group_by_field)[fact_field].sum().reset_index()
            agg_df[fact_field] = agg_df[fact_field].round(0)
            result = agg_df.to_dict(orient="records")

            if dimension_filter_field and dimension_filter_value:
                result.insert(0, {dimension_filter_field: dimension_filter_value})
            return result
        else:
            total = df[fact_field].sum()
            result = {fact_field: int(round(total))}
            if dimension_filter_field and dimension_filter_value:
                return {dimension_filter_field: dimension_filter_value, **result}
            return result
        
    def writtenoff_sum_by_dimension(self, fact_field, date_filter=None, dimension_filters=None):
        if self.df_written is None:
            raise FileNotFoundError("Source data is not found.")
        
        df = self.df_written.copy()
        if fact_field not in df.columns:
            raise ValueError(f"Fact field '{fact_field}' is not found.")
        
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        if date_filter:
            try:
                date_val = pd.to_datetime(date_filter, dayfirst=True)
                df = df[df["date"] == date_val]
            except Exception:
                raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy' format.")
            
        filters_dict = {}
        if dimension_filters:
            for pair in dimension_filters.split(','):
                if ':' in pair:
                    field, value = [p.strip() for p in pair.split(':', 1)]
                    if field not in df.columns:
                        raise ValueError(f"Dimension filter field '{field}' is not found in the dataset.")
                    filters_dict[field] = value
                else:
                    raise ValueError(f"Invalid dimension filter format: '{pair}'. Expected 'field:value'.")

        for field, value in filters_dict.items():
            if pd.api.types.is_numeric_dtype(df[field]):
                df = df[df[field] == pd.to_numeric(value, errors='coerce')]
            else:
                df = df[df[field].astype(str) == str(value)]
        

        if df.empty:
            raise FileNotFoundError("No data found after applying filters.")
              
        total = df[fact_field].sum()
        result = {fact_field: float(round(total, 2))}
        result.update(filters_dict)
        return result
        

    def get_avg_by_dimension(
        self,
        fact_fields: List[str],
        group_by_fields: Optional[List[str]] = None,
        date_filter: Optional[str] = None,
        dimension_filter_field: Optional[str] = None,
        dimension_filter_value: Optional[str] = None):
        
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
        fields = {"group", "group_id", "cust_id"}
        # Validate fact_fields
        if fact_fields:
            for field in fact_fields:
                
                if field not in df.columns:
                    raise ValueError(f"Fact field '{field}' is not found.")

                if not pd.api.types.is_numeric_dtype(df[field]) or field in fields:
                    raise ValueError(f"Fact field must be a valid numeric field.")
         
        if group_by_fields:
            for group_field in group_by_fields:
                if group_field not in df.columns:
                    raise ValueError(f"Group by field '{group_field}'is not found.")
                
                if pd.api.types.is_numeric_dtype(df[group_field]) and group_field not in fields and group_field != "rating":
                    raise ValueError(f"Numeric field '{group_field}' is not allowed as a group by field.")

        # date filter
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        if date_filter:
            try:
                date_obj = pd.to_datetime(date_filter, dayfirst=True)
                df = df[df["date"] == date_obj]
            except Exception:
                raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy' format.")

        # dimension filter
        if dimension_filter_field:
            if dimension_filter_field not in df.columns:
                raise ValueError(f"Dimension filter '{dimension_filter_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[dimension_filter_field]) and dimension_filter_field not in fields:
                raise ValueError(f"Numeric field '{dimension_filter_field}' is not allowed as a dimension filter.")

            if dimension_filter_value:
                is_valid = ((dimension_filter_field in fields  and
                 int(dimension_filter_value) in df[dimension_filter_field].dropna().unique()) or
                (str(dimension_filter_value) in df[dimension_filter_field].astype(str).unique()))
                if not is_valid:
                    raise ValueError(f"Dimension value '{dimension_filter_value}' is not found in the dimension filter '{dimension_filter_field}'.")

                if dimension_filter_field in fields:
                    df = df[df[dimension_filter_field] == int(dimension_filter_value)]
                else:
                    df = df[df[dimension_filter_field].astype(str) == str(dimension_filter_value)]
            else:
                raise ValueError("Dimension filter value is missing for the given dimension filter.")


        result = []
        if group_by_fields:
            df[group_by_fields] = df[group_by_fields].fillna("NA")
            agg_df = df.groupby(group_by_fields)[fact_fields].mean().reset_index()

            for field in fact_fields:
                agg_df[field] = agg_df[field].fillna(0).replace([float("inf"), -float("inf")], 0).round(0).astype(int)

            result = agg_df.to_dict(orient="records")

            if dimension_filter_field and dimension_filter_value:
                result.insert(0, {dimension_filter_field: dimension_filter_value})

            return result
        else:
            avg_series = df[fact_fields].mean()
            avg_series = avg_series.fillna(0).replace([float("inf"), -float("inf")], 0).round(0).astype(int)
            result = avg_series.to_dict()

            if dimension_filter_field and dimension_filter_value:
                result = {dimension_filter_field: dimension_filter_value, **result}
            return result

    def count_distinct(
    self,
    dimension,
    date_filter=None,
    compare_date: Optional[str] = None,
    dimension_filters: Optional[str] = None,
    groupby_field=None):
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")
        df = self.df_joined.copy()
        if dimension not in df.columns:
            raise ValueError(f"Fact field '{dimension}' is not found.")
        
        if groupby_field and groupby_field not in df.columns:
            raise ValueError(f"Group by field '{groupby_field}' is not found.")
        
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        filters_dict = {}
        if dimension_filters:
            for pair in dimension_filters.split(','):
                if ':' in pair:
                    field, value = [p.strip() for p in pair.split(':', 1)]
                    if field not in df.columns:
                        raise ValueError(f"Dimension filter field '{field}' is not found in the dataset.")
                    filters_dict[field] = value
                else:
                    raise ValueError(f"Invalid dimension filter format: '{pair}'. Expected 'field:value'.")
        for field, value in filters_dict.items():
            if pd.api.types.is_numeric_dtype(df[field]):
                df = df[df[field] == pd.to_numeric(value, errors='coerce')]
            else:
                df = df[df[field].astype(str) == str(value)]

        if df.empty:
           raise FileNotFoundError("No data found after applying filters.")

        result = {}
        
        all_groups = None
        if groupby_field:
            if groupby_field == "rating":
                all_groups = sorted(self.df_rating["internal_rating"].dropna().astype(int).unique())
            elif groupby_field == "group":
                all_groups = sorted(self.df_customer["group_id"].dropna().astype(int).unique())
            elif groupby_field == "sector":
                all_groups = sorted(self.df_customer["sector"].dropna().unique())
            else:
                all_groups = sorted(df[groupby_field].dropna().unique())
                
        # Loop through both dates if given
        for label, date_str in [("Base_count", date_filter), ("Other_count", compare_date)]:
            if not date_str:
                continue

            try:
                date_val = pd.to_datetime(date_str, dayfirst=True)
            except Exception:
                raise ValueError(f"Invalid date format: '{date_str}'. Please use 'dd/mm/yyyy' format.")

            df_filtered = df[df["date"] == date_val]

            if df_filtered.empty:
                if groupby_field:
                    result.update({str(group): 0 for group in all_groups})
                else:
                    result[label] = 0
                continue

            if groupby_field:
                grouped = df_filtered.groupby(groupby_field)[dimension].nunique().reset_index()
                grouped_dict = {str(row[groupby_field]): int(row[dimension]) for _, row in grouped.iterrows()}
                result.update({str(group): grouped_dict.get(str(group), 0) for group in all_groups})
            else:
                result[label] = df_filtered[dimension].dropna().nunique()
        if not date_filter and not compare_date:
            if groupby_field:
                grouped = df.groupby(groupby_field)[dimension].nunique().reset_index()
                grouped_dict = {str(row[groupby_field]): int(row[dimension]) for _, row in grouped.iterrows()}
                for group in all_groups:
                    result[str(group)] = grouped_dict.get(str(group), 0)
            else:
                result["count"] = df[dimension].dropna().nunique()

        result.update(filters_dict)
        return result
    
    def writtenoff_count_distinct(
    self,
    dimension,
    date_filter=None,
    dimension_filters: Optional[str] = None):
        
        if self.df_written is None:
            raise FileNotFoundError("Source data is not found.")
        
        df = self.df_written.copy()
        if dimension not in df.columns:
            raise ValueError(f"Fact field '{dimension}' is not found.")
        
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        if date_filter:
            try:
                date_val = pd.to_datetime(date_filter, dayfirst=True)
                df = df[df["date"] == date_val]
            except Exception:
                raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy' format.")


        filters_dict = {}
        if dimension_filters:
            for pair in dimension_filters.split(','):
                if ':' in pair:
                    field, value = [p.strip() for p in pair.split(':', 1)]
                    if field not in df.columns:
                        raise ValueError(f"Dimension filter field '{field}' is not found in the dataset.")
                    filters_dict[field] = value
                else:
                    raise ValueError(f"Invalid dimension filter format: '{pair}'. Expected 'field:value'.")

        for field, value in filters_dict.items():
            if pd.api.types.is_numeric_dtype(df[field]):
                df = df[df[field] == pd.to_numeric(value, errors='coerce')]
            else:
                df = df[df[field].astype(str) == str(value)]
    

        if df.empty:
            raise FileNotFoundError("No data found after applying filters.")
        
        
        result = {"count": df[dimension].dropna().nunique()}
        result.update(filters_dict)
        return result


    def get_concentration(self, fact_fields, group_by_fields=None, date_filter=None, top_n=10, dimension_filter_field=None, dimension_filter_value=None):
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
        fields = {"group", "group_id", "cust_id","rating"}
        # Validate fact_fields
        if fact_fields:
            if not (1 <= len(fact_fields) <= 2):
                   raise ValueError(f"Exactly one or two fact fields must be provided")
            for field in fact_fields:
                if field not in df.columns:
                    raise ValueError(f"Fact field '{field}' is not found.")

                if not pd.api.types.is_numeric_dtype(df[field]) or field in fields:
                    raise ValueError(f"Fact field must be a valid numeric field.")
         
        if group_by_fields:
            for group_field in group_by_fields:
                if group_field not in df.columns:
                    raise ValueError(f"Group by field '{group_field}'is not found.")
                
                if pd.api.types.is_numeric_dtype(df[group_field]) and group_field not in fields:
                    raise ValueError(f"Numeric field '{group_field}' is not allowed as a group by field.")

        # date filter
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        if date_filter:
            try:
                date_obj = pd.to_datetime(date_filter, dayfirst=True)
                df = df[df["date"] == date_obj]
            except Exception:
                raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy' format.")
            
        if not isinstance(top_n, int) or top_n <= 0:
            raise ValueError("top_n must be a positive integer.")

        # dimension filter
        if dimension_filter_field:
            if dimension_filter_field not in df.columns:
                raise ValueError(f"Dimension filter '{dimension_filter_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[dimension_filter_field]) and dimension_filter_field not in fields:
                raise ValueError(f"Numeric field '{dimension_filter_field}' is not allowed as a dimension filter.")

            if dimension_filter_value:
                is_valid = ((dimension_filter_field in fields  and
                 int(dimension_filter_value) in df[dimension_filter_field].dropna().unique()) or
                (str(dimension_filter_value) in df[dimension_filter_field].astype(str).unique()))
                if not is_valid:
                    raise ValueError(f"Dimension value '{dimension_filter_value}' is not found in the dimension filter '{dimension_filter_field}'.")

                if dimension_filter_field in fields:
                    df = df[df[dimension_filter_field] == int(dimension_filter_value)]
                else:
                    df = df[df[dimension_filter_field].astype(str) == str(dimension_filter_value)]
            else:
                raise ValueError("Dimension filter value is missing for the given dimension filter.")

        # If top_n is provided and group_by_fields exists, calculate top N
        if group_by_fields:
            # Handle multiple fact fields (first fact field as dividend, second as divisor)
            fact_field_1 = fact_fields[0]
            fact_field_2 = fact_fields[1] if len(fact_fields) > 1 else fact_fields[0]

            # Top N calculation
            top_n_df = df.groupby(group_by_fields).agg({fact_field_1: "sum", fact_field_2: "sum"}).reset_index()
            top_n_df = top_n_df.sort_values(fact_field_1, ascending=False).head(top_n)

            top_n_value_1 = top_n_df[fact_field_1].sum()  
            top_n_value_2 = df[fact_field_2].sum()  

        else:
            # If no group_by_fields, calculate total fact_field_1
            fact_field_1 = fact_fields[0]
            fact_field_2 = fact_fields[1] if len(fact_fields) > 1 else fact_fields[0]

            total_value_1 = df[fact_field_1].sum()  
            total_value_2 = df[fact_field_2].sum()  

            # No grouping, just total values
            top_n_value_1 = total_value_1
            top_n_value_2 = total_value_2

        # Calculate concentration as a percentage
        concentration = (top_n_value_1 / top_n_value_2) * 100 if top_n_value_2 > 0 else 0

        result = {
        fact_field_1: round(float(top_n_value_1), 0),
        "concentration_percentage": f"{round(concentration, 0)}%"
        }

        if dimension_filter_field and dimension_filter_value:
            result = {dimension_filter_field: dimension_filter_value, **result}

        return result

    def get_portfolio_trend_summary(self, fact_fields, date_filter, period_type="M", lookback: Optional[int] = 5,compare_date: Optional[str] = None,dimension_filters: Optional[str] = None):
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
        fields = {"group", "group_id", "rating"}
        selected_date = None
        cmp_date=None
        cmp =False
        # Validate fact_fields
        if fact_fields:
            for field in fact_fields:
                
                if field not in df.columns:
                    raise ValueError(f"Fact field '{field}' is not found.")

                if not pd.api.types.is_numeric_dtype(df[field]) or field in fields:
                    raise ValueError(f"Fact field must be a valid numeric field.")
        
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)

        if date_filter:
            try:
                selected_date = pd.to_datetime(date_filter, dayfirst=True)
            except Exception:
                raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy' format.")
        else:
            selected_date = df["date"].max() 
        
        if compare_date:
            try:
                cmp_date = pd.to_datetime(compare_date, dayfirst=True)
                cmp=True
            except Exception:
                raise ValueError(f"Invalid date format: '{compare_date}'. Please use 'dd/mm/yyyy' format.")
        
            
        if period_type not in ("M", "Q"):
            raise ValueError(f"Unexpected value;Only 'M' (monthly) or 'Q' (quarterly) are allowed.")

        if not isinstance(lookback, int) or lookback <= 0:
            raise ValueError("Invalid lookback value. It must be a positive integer greater than 0.")
        
        filters_dict = {}
        if dimension_filters:
            for pair in dimension_filters.split(','):
                if ':' in pair:
                    field, value = [p.strip() for p in pair.split(':', 1)]
                    if field not in df.columns:
                        raise ValueError(f"Dimension filter field '{field}' is not found in the dataset.")
                    filters_dict[field] = value
                else:
                    raise ValueError(f"Invalid dimension filter format: '{pair}'. Expected 'field:value'.")
        
        # Setup periods

        df["period"] = df["date"].dt.to_period(period_type)
        df["period_str"] = df["period"].dt.strftime('%b, %Y')
        if compare_date and cmp ==True :
            compare_period = cmp_date.to_period(period_type)
            filter_period = selected_date.to_period(period_type)
            period_list = sorted([compare_period, filter_period ])       
        else:
            period_list = [
                (selected_date - relativedelta(months=i if period_type == "M" else i * 3)).to_period(period_type)
                for i in range(lookback + 1)]
            

        #period_list = [(selected_date - relativedelta(months=i if period_type == "M" else i * 3)).to_period(period_type) for i in range(lookback + 1)]
        df = df[df["period"].isin(period_list)]
        results = []
        for p in period_list:
            p_str = p.strftime('%b, %Y')
            df_p = df[df["period"] == p]
            for field, value in filters_dict.items():
                if pd.api.types.is_numeric_dtype(df_p[field]):
                    df_p = df_p[df_p[field] == pd.to_numeric(value, errors='coerce')]
                else:
                    df_p = df_p[df_p[field].astype(str) == str(value)]

            # handle numeric fact fields
            row = {"period": p_str}
            for field in fact_fields:
                    if field in df_p.columns:
                        row[field] = round(df_p[field].sum(), 0)
                    else:
                        row[field] = None
                # Average rating 
            if "rating" in df_p.columns:
                avg_rating = df_p["rating"].mean()
                row["avg_rating_score"] = round(avg_rating, 1) if pd.notna(avg_rating) else None
            else:
                row["avg_rating_score"] = None

            # Total unique customers
            if "cust_id" in df_p.columns: 
                row["total_customers"] = df_p["cust_id"].nunique()

            if "provision" in fact_fields and "exposure" in fact_fields:
                if (df_p["exposure"].sum() != 0):
                    row["provision_percentage"] = round((df_p["provision"].sum() / df_p["exposure"].sum())*100, 2)
                else:
                    row["provision_percentage"] = None

            results.append(row)
            results.sort(key=lambda x: datetime.strptime(x["period"], "%b, %Y"))
            for row in results:
                for key, val in row.items():
                    if isinstance(val, (np.integer, np.floating)):
                        row[key] = val.item()
            

        return results

    def get_segment_distribution(
        self,
        fact_field: str,
        dimension_field: str,
        date_filter: Optional[str] = None,
        start: int = 1,
        end: Optional[int] = 20,
        interval: Optional[int] = 10,
        others: bool = True,
        dimension_filter_field: Optional[str] = None,
        dimension_filter_value: Optional[str] = None):

        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
        
        fields = {"group", "group_id", "cust_id","rating"}
        if  fact_field:
            if fact_field not in df.columns:
                raise ValueError(f"Fact field '{fact_field}' is not found.")
            
            if (not pd.api.types.is_numeric_dtype(df[fact_field]) or fact_field in fields):
                raise ValueError(f"Fact field must be valid numeric field.")
            
        if dimension_field:
            if dimension_field not in df.columns:
                raise ValueError(f"dimension_field '{dimension_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[dimension_field]) and dimension_field not in fields:
                raise ValueError(f"Numeric field '{dimension_field}' is not allowed as a group by field.")

        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        if date_filter:
            try:
                date_obj = pd.to_datetime(date_filter, dayfirst=True)
                df = df[df["date"] == date_obj]
            except Exception:
                raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy' format.")
            
        if not isinstance(start, int) or start < 1:
            raise ValueError(f"'start' must be a positive integer (>= 1). Got: {start}.")

        if end is not None:
            if not isinstance(end, int) or end <= start:
                raise ValueError(f"'end' must be an integer greater than 'start'. Got start={start}, end={end}.")
            max_interval = end - start + 1 
        else:
            # If end is None, interval range check is skipped
            max_interval = None  

        if interval is not None:
            if not isinstance(interval, int) or interval <= 0:
                raise ValueError(f"'interval' must be a positive integer. Got: {interval}.")
            if max_interval is not None and interval > max_interval:
                raise ValueError(f"'interval' must be <= (end - start + 1). "
            f"Got: interval={interval}, start={start}, end={end}, so max allowed is {max_interval}.")

        if not isinstance(others, bool):
            raise ValueError(f"'others' must be a boolean (true/false).")
            
        if dimension_filter_field:
            if dimension_filter_field not in df.columns:
                raise ValueError(f"Dimension filter '{dimension_filter_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[dimension_filter_field]) and dimension_filter_field not in fields:
                raise ValueError(f"Numeric field '{dimension_filter_field}' is not allowed as a dimension filter.")

            if dimension_filter_value:
                is_valid = ((dimension_filter_field in fields  and
                 int(dimension_filter_value) in df[dimension_filter_field].dropna().unique()) or
                (str(dimension_filter_value) in df[dimension_filter_field].astype(str).unique()))
                if not is_valid:
                    raise ValueError(f"Dimension value '{dimension_filter_value}' is not found in the dimension filter '{dimension_filter_field}'.")

                if dimension_filter_field in fields:
                    df = df[df[dimension_filter_field] == int(dimension_filter_value)]
                else:
                    df = df[df[dimension_filter_field].astype(str) == str(dimension_filter_value)]
            else:
                raise ValueError("Dimension filter value is missing for the given dimension filter.")

        # Rank by fact_field (e.g., exposure)
        df_ranked = (df.groupby(dimension_field)[fact_field].sum() .sort_values(ascending=False)
                      .reset_index())
        total_fact_field = df_ranked[fact_field].sum()

        # Apply segmentation logic based on start, end, and interval
        segments = []

        if dimension_filter_field and dimension_filter_value:
            segments.append({dimension_filter_field: dimension_filter_value})
        
        # Handling intervals if provided
        if interval:
            for i in range(start - 1, end, interval):
                upper_limit = i + interval
                segment_name = f"Top {i + 1}-{upper_limit}"
                segment_df = df_ranked.iloc[i:upper_limit]
                segment_total = segment_df[fact_field].sum()
                segment_percentage = (segment_total / total_fact_field) * 100
                segments.append({
                    "segment": segment_name,
                    fact_field: int(segment_total),
                    "percentage": f"{round(segment_percentage, 1)}%"
                })
            
            # Handle the "Others" segment
            if others:
                others_df = df_ranked.iloc[end:]
                others_total = others_df[fact_field].sum()
                others_percentage = (others_total / total_fact_field) * 100
                segments.append({
                    "segment": "Others",
                    fact_field: int(others_total),
                    "percentage": f"{round(others_percentage, 1)}%"
                })

        else:
            # If no interval is provided, just use start and end for the segment
            segment_df = df_ranked.iloc[start - 1:end]
            segment_total = segment_df[fact_field].sum()
            segment_percentage = (segment_total / total_fact_field) * 100
            segments.append({
                "segment": f"Top {start}-{end}",
                fact_field: int(segment_total),
                "percentage": f"{round(segment_percentage, 1)}%"
            })
            
            # Handle the "Others" segment
            if others:
                others_df = df_ranked.iloc[end:]
                others_total = others_df[fact_field].sum()
                others_percentage = (others_total / total_fact_field) * 100
                segments.append({
                    "segment": "Others",
                    fact_field: int(others_total),
                    "percentage": f"{round(others_percentage, 1)}%"
                })

        return segments

    def get_ranked_entities_with_others(
    self,
    fact_field: str,
    dimension_field: str,
    date_filter: Optional[str] = None,
    start: int = 1,
    end: Optional[int] = 10,
    others_option: bool = False,
    dimension_filter_field: Optional[str] = None,
    dimension_filter_value: Optional[str] = None):
        
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
        
        fields = {"group", "group_id", "cust_id","rating"}
        if  fact_field:
            if fact_field not in df.columns:
                raise ValueError(f"Fact field '{fact_field}' is not found.")
            
            if (not pd.api.types.is_numeric_dtype(df[fact_field]) or fact_field in fields):
                raise ValueError(f"Fact field must be valid numeric field.")
            
        if dimension_field:
            if dimension_field not in df.columns:
                raise ValueError(f"dimension_field '{dimension_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[dimension_field]) and dimension_field not in fields:
                raise ValueError(f"Numeric field '{dimension_field}' is not allowed as a group by field.")
        
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        if date_filter:
            try:
                date_obj = pd.to_datetime(date_filter, dayfirst=True)
                df = df[df["date"] == date_obj]
            except Exception:
                raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy' format.")
            
        if not isinstance(start, int) or start < 1:
            raise ValueError(f"'start' must be a positive integer (>= 1). Got: {start}.")

        if end is not None:
            if not isinstance(end, int) or end <= start:
                raise ValueError(f"'end' must be an integer greater than 'start'. Got start={start}, end={end}.")
            max_interval = end - start + 1 
        else:
            # If end is None, interval range check is skipped
            max_interval = None  

        if not isinstance(others_option, bool):
            raise ValueError(f"'others' must be a boolean (true/false).")
            
        if dimension_filter_field:
            if dimension_filter_field not in df.columns:
                raise ValueError(f"Dimension filter '{dimension_filter_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[dimension_filter_field]) and dimension_filter_field not in fields:
                raise ValueError(f"Numeric field '{dimension_filter_field}' is not allowed as a dimension filter.")

            if dimension_filter_value:
                is_valid = ((dimension_filter_field in fields  and
                 int(dimension_filter_value) in df[dimension_filter_field].dropna().unique()) or
                (str(dimension_filter_value) in df[dimension_filter_field].astype(str).unique()))
                if not is_valid:
                    raise ValueError(f"Dimension value '{dimension_filter_value}' is not found in the dimension filter '{dimension_filter_field}'.")

                if dimension_filter_field in fields:
                    df = df[df[dimension_filter_field] == int(dimension_filter_value)]
                else:
                    df = df[df[dimension_filter_field].astype(str) == str(dimension_filter_value)]
            else:
                raise ValueError("Dimension filter value is missing for the given dimension filter.")

        # Rank entities by the provided fact field
        ranked_entities = (df.groupby(dimension_field)[fact_field].sum().sort_values(ascending=False)
            .reset_index())

        # If user provides a start and end rank, we can filter the DataFrame accordingly
        selected_entities = ranked_entities.iloc[start - 1:end]

        # Calculate the total value for the selected top entities
        total_selected_value = selected_entities[fact_field].sum()

        # If the "Others" option is enabled, calculate the total value for entities outside the selected range
        if others_option:
            others = ranked_entities.iloc[end:]
            others_value = others[fact_field].sum()
            others_percentage = round((others_value / others_value) * 100, 1)

            # Return only the "Others" segment as the result
            return [
                {
                    "segment": "Others",
                    fact_field: int(others_value),
                    "percentage": f"{others_percentage}%"
                }
            ]

        result = []

        if dimension_field == "cust_id":
            cust_id_to_name = (
            self.df_joined[["cust_id", "cust_name"]]
            .drop_duplicates()
            .set_index("cust_id")["cust_name"]
            .to_dict()
            )

        # Now we populate the result for the top n entities
        for _, row in selected_entities.iterrows():
            value = row[dimension_field]
            fact_val = round(float(row[fact_field]))
            percent = round((fact_val / total_selected_value) * 100, 1)

            if dimension_field == "cust_id":
                # Map cust_id to cust_name
                cust_name = cust_id_to_name.get(value, f"ID:{value}")
                result.append({
                    "cust_name": cust_name,
                    fact_field: int(fact_val),
                    "percentage": f"{percent}%"
                })
            else:
                result.append({
                    dimension_field: value,
                    fact_field: int(fact_val),
                    "percentage": f"{percent}%"
                })

        return result
    
    def get_ranked_distribution_by_grouping(
    self,
    fact_field: str,
    dimension_field_to_rank: str,
    group_by_field: str,
    start_rank: int = 1,
    end_rank: Optional[int] = None,
    others_option: Optional[bool] = False,
    date_filter: Optional[str] = None,
    dimension_filter_field: Optional[str] = None,
    dimension_filter_value: Optional[str] = None):
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
        
        fields = {"group", "group_id", "cust_id","rating"}
        if  fact_field:
            if fact_field not in df.columns:
                raise ValueError(f"Fact field '{fact_field}' is not found.")
            
            if (not pd.api.types.is_numeric_dtype(df[fact_field]) or fact_field in fields):
                raise ValueError(f"Fact field must be valid numeric field.")
            
        if dimension_field_to_rank:
            if dimension_field_to_rank not in df.columns:
                raise ValueError(f"dimension_field_to_rank '{dimension_field_to_rank}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[dimension_field_to_rank]) and dimension_field_to_rank not in fields:
                raise ValueError(f"Numeric field '{dimension_field_to_rank}' is not allowed as a group by field.")
        
        if group_by_field:
            if group_by_field not in df.columns:
                raise ValueError(f"Group by field '{group_by_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[group_by_field]) and group_by_field not in fields:
                raise ValueError(f"Numeric field '{group_by_field}' is not allowed as a group by field.")
        

        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        if date_filter:
            try:
                date_obj = pd.to_datetime(date_filter, dayfirst=True)
                df = df[df["date"] == date_obj]
            except Exception:
                raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy' format.")
            
        if not isinstance(start_rank, int) or start_rank < 1:
            raise ValueError(f"'start_rank' must be a positive integer (>= 1). Got: {start_rank}.")

        if end_rank is not None:
            if not isinstance(end_rank, int) or end_rank <= start_rank:
                raise ValueError(f"'end' must be an integer greater than 'start'. Got start={start_rank}, end={end_rank}.")
            max_interval = end_rank - start_rank + 1 
        else:
            # If end is None, interval range check is skipped
            max_interval = None  

        if not isinstance(others_option, bool):
            raise ValueError(f"'others' must be a boolean (true/false).")
            
        if dimension_filter_field:
            if dimension_filter_field not in df.columns:
                raise ValueError(f"Dimension filter '{dimension_filter_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[dimension_filter_field]) and dimension_filter_field not in fields:
                raise ValueError(f"Numeric field '{dimension_filter_field}' is not allowed as a dimension filter.")

            if dimension_filter_value:
                is_valid = ((dimension_filter_field in fields  and
                 int(dimension_filter_value) in df[dimension_filter_field].dropna().unique()) or
                (str(dimension_filter_value) in df[dimension_filter_field].astype(str).unique()))
                if not is_valid:
                    raise ValueError(f"Dimension value '{dimension_filter_value}' is not found in the dimension filter '{dimension_filter_field}'.")

                if dimension_filter_field in fields:
                    df = df[df[dimension_filter_field] == int(dimension_filter_value)]
                else:
                    df = df[df[dimension_filter_field].astype(str) == str(dimension_filter_value)]
            else:
                raise ValueError("Dimension filter value is missing for the given dimension filter.")

        # --- Get Top N IDs based on fact_field ---
        ranked = df.groupby(dimension_field_to_rank)[fact_field].sum().sort_values(ascending=False).reset_index()
        ranked["rank"] = ranked[fact_field].rank(method="first", ascending=False).astype(int)

        if end_rank is None:
            end_rank = ranked["rank"].max()
        
        if others_option == False:
            selected_ids = ranked[(ranked["rank"] >= start_rank) & (ranked["rank"] <= end_rank)][dimension_field_to_rank].tolist()
            subset = df[df[dimension_field_to_rank].isin(selected_ids)]

            # --- Group and sum by selected group_by_field ---
            grouped = subset.groupby(group_by_field)[fact_field].sum().reset_index()

            # --- Get all possible values for group_by_field ---
            if group_by_field == "rating":
                all_vals = sorted(self.df_rating["internal_rating"].dropna().unique())
                full_index = pd.DataFrame({group_by_field: all_vals})
            elif group_by_field == "group":
                all_vals = sorted(self.df_customer["group_id"].dropna().unique().astype(int))
                full_index = pd.DataFrame({group_by_field: all_vals})
            elif group_by_field in self.df_customer.columns:
                all_vals = self.df_customer[group_by_field].dropna().unique()
                full_index = pd.DataFrame({group_by_field: all_vals})
            else:
                all_vals = df[group_by_field].dropna().unique()
                full_index = pd.DataFrame({group_by_field: all_vals})

       
            merged = full_index.merge(grouped, on=group_by_field, how="left").fillna(0)
            total = merged[fact_field].sum()
            merged["percentage"] = (merged[fact_field] / total * 100).round(0).astype(str) + "%"
            merged[fact_field] = merged[fact_field].round(0).astype(int)
            result = merged.to_dict(orient="records")

            if dimension_filter_field and dimension_filter_value:
                result = [{dimension_filter_field: dimension_filter_value}] + result
            
            return result

        # Handle "Others" option if selected
        else:
            # Find all rows with rank > end_rank
            other_entities = ranked[ranked["rank"] > end_rank]
            other_ids = other_entities[dimension_field_to_rank].tolist()

            other_subset = df[df[dimension_field_to_rank].isin(other_ids)]

            other_grouped = other_subset.groupby(group_by_field)[fact_field].sum().reset_index()
            
            if group_by_field == "rating":
                all_vals = self.df_rating["internal_rating"].dropna().unique()
                full_index = pd.DataFrame({group_by_field: all_vals})
            elif group_by_field == "group":
                all_vals = self.df_customer["group_id"].dropna().unique()
                full_index = pd.DataFrame({group_by_field: all_vals})
            elif group_by_field in self.df_customer.columns:
                all_vals = self.df_customer[group_by_field].dropna().unique()
                full_index = pd.DataFrame({group_by_field: all_vals})
            elif group_by_field == "staging":
                series = self.df_joined["staging"].dropna()
                cleaned_series = pd.Series(series).apply(lambda x: str(x).strip() if isinstance(x, str) else str(x))
                all_vals = natsorted(cleaned_series.unique())
                full_index = pd.DataFrame({group_by_field: all_vals})
            else:
                all_vals = df[group_by_field].dropna().unique()
                full_index = pd.DataFrame({group_by_field: all_vals})
            
            merged = full_index.merge(other_grouped, on=group_by_field, how="left").fillna(0)
            total = merged[fact_field].sum()
            merged["percentage"] = (merged[fact_field] / total * 100).round(0).astype(str) + "%"
            merged[fact_field] = merged[fact_field].round(0).astype(int)
            result = merged.to_dict(orient="records")

            if dimension_filter_field and dimension_filter_value:
                result = [{dimension_filter_field: dimension_filter_value}] + result

            return result
    
    def get_perc_distribution_by_field(self, fact_field: str, dimension_field: str, date_filter: Optional[str] = None, dimension_filter_field: Optional[str] = None, dimension_filter_value: Optional[str] = None):
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
        
        fields = {"group", "group_id", "cust_id","rating"}
        if  fact_field:
            if fact_field not in df.columns:
                raise ValueError(f"Fact field '{fact_field}' is not found.")
            
            if (not pd.api.types.is_numeric_dtype(df[fact_field]) or fact_field in fields):
                raise ValueError(f"Fact field must be valid numeric field.")
            
        if dimension_field:
            if dimension_field not in df.columns:
                raise ValueError(f"dimension_field '{dimension_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[dimension_field]) and dimension_field not in fields:
                raise ValueError(f"Numeric field '{dimension_field}' is not allowed as a group by field.")
        
        
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        if date_filter:
            try:
                date_obj = pd.to_datetime(date_filter, dayfirst=True)
                df = df[df["date"] == date_obj]
            except Exception:
                raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy' format.")
            
            
        if dimension_filter_field:
            if dimension_filter_field not in df.columns:
                raise ValueError(f"Dimension filter '{dimension_filter_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[dimension_filter_field]) and dimension_filter_field not in fields:
                raise ValueError(f"Numeric field '{dimension_filter_field}' is not allowed as a dimension filter.")

            if dimension_filter_value:
                is_valid = ((dimension_filter_field in fields  and
                 int(dimension_filter_value) in df[dimension_filter_field].dropna().unique()) or
                (str(dimension_filter_value) in df[dimension_filter_field].astype(str).unique()))
                if not is_valid:
                    raise ValueError(f"Dimension value '{dimension_filter_value}' is not found in the dimension filter '{dimension_filter_field}'.")

                if dimension_filter_field in fields:
                    df = df[df[dimension_filter_field] == int(dimension_filter_value)]
                else:
                    df = df[df[dimension_filter_field].astype(str) == str(dimension_filter_value)]
            else:
                raise ValueError("Dimension filter value is missing for the given dimension filter.")
        

        # Group by sector and sum the fact_field
        distribution = df.groupby(dimension_field)[fact_field].sum().reset_index()

        # Calculate total sum of fact_field for percentage calculation
        total = distribution[fact_field].sum()
        distribution["percentage"] = (distribution[fact_field] / total * 100).round(0).astype(str) + "%"

        # Only return the percentage column and the dimension field
        distribution = distribution[[dimension_field, "percentage"]]

        # Add the dimension filter as the first element if provided
        result = distribution.to_dict(orient="records")
        if dimension_filter_field and dimension_filter_value:
            result.insert(0, {dimension_filter_field: dimension_filter_value})

        return result
    
    def get_percentage_trend_by_field(
        self,
        fact_field: str,
        dimension_field: str,
        date: str,
        period_type: str,
        lookback_range: int,
        dimension_filter_field: Optional[str] = None,
        dimension_filter_value: Optional[str] = None):

        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
        fields = {"group", "group_id", "cust_id","rating"}
        # Validate fact_fields
        if fact_field:
            if fact_field not in df.columns:
                raise ValueError(f"Fact field '{fact_field}' is not found.")

            if not pd.api.types.is_numeric_dtype(df[fact_field]) or fact_field in fields:
                raise ValueError(f"Fact field must be a valid numeric field.")
            
        if dimension_field:
            if dimension_field not in df.columns:
                raise ValueError(f"dimension_field '{dimension_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[dimension_field]) and dimension_field not in fields:
                raise ValueError(f"Numeric field '{dimension_field}' is not allowed as a group by field.")
         
        
        user_date = None
        user_month_year = None
        if date:
            try:
                user_date = pd.to_datetime(date, format='%d/%m/%Y')
                user_month_year = user_date.strftime('%b %y') 
            except Exception:
                raise ValueError(f"Invalid date format: '{date}'. Please use 'dd/mm/yyyy' format.")


        if not isinstance(lookback_range, int) or lookback_range <= 0:
            raise ValueError("Invalid lookback value. It must be a positive integer greater than 0.")
        
        if dimension_filter_field:
            if dimension_filter_field not in df.columns:
                raise ValueError(f"Dimension filter '{dimension_filter_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[dimension_filter_field]) and dimension_filter_field not in fields:
                raise ValueError(f"Numeric field '{dimension_filter_field}' is not allowed as a dimension filter.")

            if dimension_filter_value:
                is_valid = ((dimension_filter_field in fields  and
                 int(dimension_filter_value) in df[dimension_filter_field].dropna().unique()) or
                (str(dimension_filter_value) in df[dimension_filter_field].astype(str).unique()))
                if not is_valid:
                    raise ValueError(f"Dimension value '{dimension_filter_value}' is not found in the dimension filter '{dimension_filter_field}'.")

                if dimension_filter_field in fields:
                    df = df[df[dimension_filter_field] == int(dimension_filter_value)]
                else:
                    df = df[df[dimension_filter_field].astype(str) == str(dimension_filter_value)]
            else:
                raise ValueError("Dimension filter value is missing for the given dimension filter.")
        

       
        #user_date = pd.to_datetime(date, format='%d/%m/%Y')
        #user_month_year = user_date.strftime('%b %y')  

        # Adjust lookback to get previous periods
        periods = [user_month_year]
        for i in range(lookback_range):
            if period_type == "M":  
                period_date = user_date - pd.DateOffset(months=i+1)
                period_month_year = period_date.strftime('%b %y')
            elif period_type == "Q":  
                period_date = user_date - pd.DateOffset(months=3 * (i+1))
                period_month_year = period_date.strftime('%b %y')
            periods.append(period_month_year)

        periods = sorted(periods, key=lambda x: pd.to_datetime(x, format='%b %y'))


        if dimension_filter_field and dimension_filter_value:
            if dimension_filter_field not in df.columns:
                return {"error": f"Column {dimension_filter_field} not found in the dataset."}
            if dimension_filter_field == "group":
                df = df[df[dimension_filter_field] == int(dimension_filter_value)]
            else:
                df = df[df[dimension_filter_field] == str(dimension_filter_value)]

        if df.empty or fact_field not in df.columns or dimension_field not in df.columns:
            return {"error": "Invalid input or no data for the given date."}

        result = []

        if dimension_field == "rating":
            all_vals = sorted(self.df_rating["internal_rating"].dropna().unique())
            full_index = pd.DataFrame({dimension_field: all_vals})
        elif dimension_field == "group":
            all_vals = sorted(self.df_customer["group_id"].dropna().unique().astype(int))
            full_index = pd.DataFrame({dimension_field: all_vals})
        elif dimension_field in self.df_customer.columns:
            all_vals = self.df_customer[dimension_field].dropna().unique()
            full_index = pd.DataFrame({dimension_field: all_vals})
        else:
            all_vals = df[dimension_field].dropna().unique()
            full_index = pd.DataFrame({dimension_field: all_vals})

        # Loop through each period to calculate the percentage for each
        for period in periods:
            # Filter the data for the current period
            df_period = df[df['date'].dt.strftime('%b %y') == period]
            
            # Group by the dimension_field and calculate the sum of fact_field
            grouped = df_period.groupby(dimension_field)[fact_field].sum().reset_index()
            merged = full_index.merge(grouped, on=dimension_field, how="left").fillna(0)

            # Calculate the total sum of the fact field for the period
            total = merged[fact_field].sum()
            for index, row in merged.iterrows():
                if row[fact_field] == 0:
                    merged.at[index, "percentage"] = "0%"
                else:
                    percentage = (row[fact_field] / total * 100).round(0)
                    merged.at[index, "percentage"] = f"{percentage}%"

            # Include the period as a field
            merged["period"] = period

            # If cust_id is used as dimension_field, map to cust_name
            if dimension_field == "cust_id":
                merged["cust_name"] = merged[dimension_field].map(self.df_customer.set_index("cust_id")["cust_name"])
            
            period_dict = {"period": period}
            for _, row in merged.iterrows():
                key = str(row[dimension_field])
                period_dict[key] = row["percentage"]
            
            result.append(period_dict)

        if dimension_filter_field and dimension_filter_value:
            dimension_filter_dict = {dimension_filter_field: dimension_filter_value}
            result.insert(0, dimension_filter_dict)

        return result
    
    def _calculate_periods(self, date: str, lookback: int, period_type: str):
        """Generate the lookback periods based on the provided date and period type."""
        date_obj = pd.to_datetime(date, errors="coerce", dayfirst=True)
        periods = []
        for i in range(lookback):
            if period_type == 'M':
                period = (date_obj - pd.DateOffset(months=i)).strftime('%b %y')
            elif period_type == 'Q':
                period = (date_obj - pd.DateOffset(months=3 * i)).strftime('%b %y')
            periods.append(period)
        return periods
     
    def get_ranked_data_by_period(
    self,
    fact_field: str,
    dimension_field_to_rank: str,
    date: str,
    start_rank: Optional[int] = None,
    end_rank: Optional[int] = None,
    period_type: str = 'Q',
    lookback: int = 5,
    dimension_field: str = 'rating',
    dimension_filter_field: Optional[str] = None,
    dimension_filter_value: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()

        # Validate rank range
        if start_rank is not None and start_rank < 1:
            raise ValueError(f"'start_rank' must be a positive integer (>= 1). Got: {start_rank}.")
        if end_rank is not None and start_rank is not None and end_rank < start_rank:
            raise ValueError(f"'end_rank' must be an integer greater than 'start_rank'. Got start_rank={start_rank}, end_rank={end_rank}.")

        # Normalize inputs
        fact_field = fact_field.lower().strip()
        dim_rank = dimension_field_to_rank.lower().strip()
        dim_field = dimension_field.lower().strip()
        filt_field = dimension_filter_field.lower().strip() if dimension_filter_field else None

        # Validate fields exist
        for fld, name in [(fact_field, "fact_field"), (dim_rank, "dimension_field_to_rank"), (dim_field, "dimension_field")]:
            if fld not in df.columns:
                raise ValueError(f"Field '{fld}' not found in dataset.")

        if dim_rank == 'cust id' and 'cust name' not in df.columns:
            raise ValueError("Field 'cust_name' is required when ranking by 'cust_id'.")

        # Apply optional filter
        if filt_field:
            if dimension_filter_value is None:
                raise ValueError("Dimension filter value is missing for the given dimension filter field.")
            if filt_field not in df.columns:
                raise ValueError(f"Dimension filter field '{filt_field}' is not found in dataset.")
            unique_values = df[filt_field].unique().astype(str)
            if str(dimension_filter_value) not in unique_values:
                raise ValueError(f"Dimension value '{dimension_filter_value}' is not found in the filter '{filt_field}'.")
            if filt_field == "group":
                df = df[df[filt_field] == int(dimension_filter_value)]
            else:
                df = df[df[filt_field].astype(str) == str(dimension_filter_value)]
            if df.empty:
                raise FileNotFoundError("No data after applying filter. Check filter values.")

        # Calculate start and end ranks
        if start_rank is None:
            start_rank = 1
        if end_rank is None:
            end_rank = int(df[dim_rank].nunique())

        # Calculate periods
        periods = self._calculate_periods(date, lookback, period_type)
        df['period'] = pd.to_datetime(df['date'], errors="coerce", dayfirst=True).dt.strftime('%b %y')
        df_periods = df[df['period'].isin(periods)]
        if df_periods.empty:
            raise FileNotFoundError(f"No data available for periods: {periods}. Check the date or data availability.")

        # Ensure fact_field is numeric
        df_periods[fact_field] = pd.to_numeric(df_periods[fact_field], errors='coerce')

        # Compute dense rank
        df_periods['rank'] = df_periods.groupby('period')[fact_field].rank(ascending=False, method='dense')

        # Select primary period and rank window
        primary = periods[0]
        df_primary = df_periods[df_periods['period'] == primary]
        df_selected = df_primary[(df_primary['rank'] >= start_rank) & (df_primary['rank'] <= end_rank)]
        if df_selected.empty:
            raise FileNotFoundError(f"No records found in rank range {start_rank}-{end_rank} for {primary}.")

        # Get ordered keys
        order_keys = df_selected.sort_values('rank')[dim_rank].unique().tolist()
        df_full = df_periods[df_periods[dim_rank].isin(order_keys)]

        def to_python(x):
            return x.item() if hasattr(x, 'item') else x

        results: List[Dict[str, Any]] = []
        for key in order_keys:
            grp = df_full[df_full[dim_rank] == key]
            entry: Dict[str, Any] = {
                "Customer ID": to_python(key),
                "Customer Name": grp['cust_name'].iloc[0]
            }
            if filt_field:
                entry[dimension_filter_field] = to_python(grp[filt_field].iloc[0])
            entry["Periods"] = []
            for p in periods:
                sub = grp[grp['period'] == p]
                if not sub.empty:
                    row = sub.iloc[0]
                    entry["Periods"].append({
                        "Period": p,
                        fact_field: to_python(row[fact_field]),
                        "Rank": int(row['rank']),
                        dimension_field: to_python(row[dim_field])
                    })
                else:
                    entry["Periods"].append({
                        "Period": p,
                        fact_field: None,
                        "Rank": None,
                        dimension_field: None
                    })
            results.append(entry)

        return results

    def calculate_weighted_average(
    self,
    fact_field: str,
    weight_field: str,
    date_filter: Optional[str] = None,
    dimension_filter_field: Optional[str] = None,
    dimension_filter_value: Optional[str] = None
    ) -> Dict[str, float]:
        

        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
        fields = {"group", "group_id", "cust_id"}
        # Validate fact_fields
        if fact_field:
            if fact_field not in df.columns:
                raise ValueError(f"Fact field '{fact_field}' is not found.")

            if not pd.api.types.is_numeric_dtype(df[fact_field]) or fact_field in fields:
                raise ValueError(f"Fact field must be a valid numeric field.")
            
        if weight_field:
            if weight_field not in df.columns:
                raise ValueError(f"weight_field '{weight_field}' is not found.")

            if not pd.api.types.is_numeric_dtype(df[weight_field]) or weight_field in fields:
                raise ValueError(f"weight_field must be a valid numeric field.")
    
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        if date_filter:
            try:
                date_obj = pd.to_datetime(date_filter, dayfirst=True)
                df = df[df["date"] == date_obj]
            except Exception:
                raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy' format.") 

        if dimension_filter_field:
            if dimension_filter_field not in df.columns:
                raise ValueError(f"Dimension filter '{dimension_filter_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[dimension_filter_field]) and dimension_filter_field not in fields:
                raise ValueError(f"Numeric field '{dimension_filter_field}' is not allowed as a dimension filter.")

            if dimension_filter_value:
                is_valid = ((dimension_filter_field in fields  and
                 int(dimension_filter_value) in df[dimension_filter_field].dropna().unique()) or
                (str(dimension_filter_value) in df[dimension_filter_field].astype(str).unique()))
                if not is_valid:
                    raise ValueError(f"Dimension value '{dimension_filter_value}' is not found in the dimension filter '{dimension_filter_field}'.")

                if dimension_filter_field in fields:
                    df = df[df[dimension_filter_field] == int(dimension_filter_value)]
                else:
                    df = df[df[dimension_filter_field].astype(str) == str(dimension_filter_value)]
            else:
                raise ValueError("Dimension filter value is missing for the given dimension filter.")
            
        df[fact_field] = pd.to_numeric(df[fact_field], errors='coerce')
        df[weight_field] = pd.to_numeric(df[weight_field], errors='coerce')

        total = df[weight_field].sum()
        if total == 0:
            return {"error": "Total weight is zero; cannot compute weighted average."}

        df["weight"] = df[weight_field] / total
        weighted_sum = (df[fact_field] * df["weight"]).sum()
        weighted_average = weighted_sum / (df["weight"].sum())
        weighted_average = round(weighted_average, 2)

        result = {fact_field: weighted_average}
        if dimension_filter_field and dimension_filter_value:
            result = {dimension_filter_field: dimension_filter_value, **result}

        return result
    
    def _format_label(self, date: pd.Timestamp, freq: str) -> str:
        return date.strftime("%b %Y")  

    def _generate_periods(self, base_date: pd.Timestamp, lookback: int, freq: str) -> List[pd.Timestamp]:
        periods = []
        for i in range(lookback + 1):
            if freq == 'm':
                period = (base_date - pd.DateOffset(months=i)).replace(day=1)
            elif freq == "q":  
                q = (base_date.to_period("Q") - i).to_timestamp(how='end')
                period = q.replace(day=1)
            else:
                raise ValueError("Frequency must be 'm' or 'q'")
            periods.append(period)
        return sorted(set(periods))

    def weighted_avg_trend(
        self,
        fact_fields: List[str],
        weight_field: str,
        date_filter: Optional[str] = None,
        lookback: int = 5,
        frequency: str = "q",
        dimension_filter_field: Optional[str] = None,
        dimension_filter_value: Optional[str] = None
    ) -> Any:
        
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
        fields = {"group", "group_id", "cust_id"}
        # Validate fact_fields
        if fact_fields:
            for field in fact_fields:
                
                if field not in df.columns:
                    raise ValueError(f"Fact field '{field}' is not found.")

                if not pd.api.types.is_numeric_dtype(df[field]) or field in fields:
                    raise ValueError(f"Fact field must be a valid numeric field.")
                
        if weight_field:
            if weight_field not in df.columns:
                raise ValueError(f"weight_field '{weight_field}' is not found.")

            if not pd.api.types.is_numeric_dtype(df[weight_field]) or weight_field in fields:
                raise ValueError(f"weight_field must be a valid numeric field.")
            
        if date_filter:
            try:
                base_date = pd.to_datetime(date_filter, dayfirst=True, errors="raise")
            except Exception:
                raise ValueError(f"Invalid date '{date_filter}'. Use 'DD/MM/YYYY'.")
        else:
            base_date = pd.Timestamp.today()

        if not isinstance(lookback, int) or lookback <= 0:
            raise ValueError("Invalid lookback value. It must be a positive integer greater than 0.")   
        
        if frequency not in ("m", "q"):
            raise ValueError(f"Unexpected value;Only 'm' (monthly) or 'q' (quarterly) are allowed.")

        
        periods = self._generate_periods(base_date, lookback, frequency)
        
        if dimension_filter_field:
            if dimension_filter_field not in df.columns:
                raise ValueError(f"Dimension filter '{dimension_filter_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[dimension_filter_field]) and dimension_filter_field not in fields:
                raise ValueError(f"Numeric field '{dimension_filter_field}' is not allowed as a dimension filter.")

            if dimension_filter_value:
                is_valid = ((dimension_filter_field in fields  and
                 int(dimension_filter_value) in df[dimension_filter_field].dropna().unique()) or
                (str(dimension_filter_value) in df[dimension_filter_field].astype(str).unique()))
                if not is_valid:
                    raise ValueError(f"Dimension value '{dimension_filter_value}' is not found in the dimension filter '{dimension_filter_field}'.")

                if dimension_filter_field in fields:
                    df = df[df[dimension_filter_field] == int(dimension_filter_value)]
                else:
                    df = df[df[dimension_filter_field].astype(str) == str(dimension_filter_value)]
            else:
                raise ValueError("Dimension filter value is missing for the given dimension filter.")
                
       
        # Initialize metrics dictionary
        metrics = {}

        # Calculate metrics for each period
        for p in periods:
            if frequency == 'm':
                subset = df[(df["date"].dt.year == p.year) & (df["date"].dt.month == p.month)]
            else:  # quarterly
                subset = df[df["date"].dt.to_period("Q") == p.to_period("Q")]

            label = self._format_label(p, frequency)
            metrics[label] = {}

            for field in fact_fields:
                if field not in df.columns:
                    metrics[label][field] = 0
                    continue

                total_w = pd.to_numeric(subset[weight_field], errors="coerce").sum()
                if subset.empty or total_w == 0 or pd.isna(total_w):
                    metrics[label][field] = 0
                else:
                    weighted_sum = (
                        pd.to_numeric(subset[field], errors="coerce") *
                        pd.to_numeric(subset[weight_field], errors="coerce")
                    ).sum()
                    avg = weighted_sum / total_w
                    metrics[label][field] = round(avg, 2) if field in ("pd", "rating") else round(avg, 6)

        # Prepare the result
        if dimension_filter_field and dimension_filter_value:
            filter_str = f"{dimension_filter_field}:{dimension_filter_value}"
            result = [filter_str, metrics]
        else:
            result = metrics

        return result
    
    def get_aggregated_metrics_by_field(
    self,
    metrics: str,
    group_by_field: Optional[str] = None,
    date_filter: Optional[str] = None,
    dimension_filters: Optional[str] = None,
    top_n:Optional[int] = 10,
    day_flag :Optional[int] = None,
    additional_field: Optional[str] = None):

        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
    
        #metrics
        valid_aggregations = {"sum", "count", "mean", "weighted_average"}
        metrics_dict = {}
        for metric in metrics.split(','):
            parts = metric.strip().split(':')
            if len(parts) != 2:
                raise ValueError(f"Invalid metric format: '{metric}' (expected 'field:aggregation')")
            field, agg_type = parts
            field = field.strip()
            agg_type = agg_type.strip().lower()
            if agg_type not in valid_aggregations:
                raise ValueError(f"Unsupported aggregation type: '{agg_type}'. Supported types: {valid_aggregations}")
            if field not in df.columns:
                raise ValueError(f"Field '{field}' not found in dataset.")
            
            metrics_dict[field.strip()] = agg_type.strip()
        
        fields = {"group", "group_id","rating","cust_id"}
        if group_by_field:
            if group_by_field not in df.columns:
                raise ValueError(f"Group by field '{group_by_field}' is not found.")
            
            if pd.api.types.is_numeric_dtype(df[group_by_field]) and group_by_field not in fields:
                raise ValueError(f"Numeric field '{group_by_field}' is not allowed as a group by field.")

        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        if date_filter:
            try:
                date_obj = pd.to_datetime(date_filter, dayfirst=True)
                df = df[df["date"] == date_obj]
            except Exception:
                raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy' format.") 
            
        filters_dict = {}
        if dimension_filters:
            for pair in dimension_filters.split(','):
                if ':' in pair:
                    field, value = [p.strip() for p in pair.split(':', 1)]
                    if field not in df.columns:
                        raise ValueError(f"Dimension filter field '{field}' is not found in the dataset.")
                    if pd.api.types.is_numeric_dtype(df[field]) and field not in fields:
                        raise ValueError(f"Numeric field '{field}' is not allowed as a dimension filter.")
                    filters_dict[field] = value
                else:
                    raise ValueError(f"Invalid dimension filter format: '{pair}'. Expected 'field:value'.")

        for field, value in filters_dict.items():
            if pd.api.types.is_numeric_dtype(df[field]):
                df = df[df[field] == pd.to_numeric(value, errors='coerce')]
            else:
                df = df[df[field].astype(str) == str(value)]

        if top_n is not None:
            if not isinstance(top_n, int) or top_n <= 0:
                raise ValueError("Invalid top_n: '{top_n}'.It must be a positive integer.")
        

        if day_flag is not None:
            if not isinstance(day_flag, int) or day_flag not in [90, 180, 360]:
                raise ValueError("day_flag must be one of [90, 180, 360]")
            
            if "dpd" not in df.columns:
                raise ValueError("dpd field is required for day_flag filtering.")
            
            if day_flag == 90:
                df = df[(df["dpd"] >= 61) & (df["dpd"] <= 89)]
            elif day_flag == 180:
                df = df[(df["dpd"] >= 151) & (df["dpd"] <= 179)]
            elif day_flag == 360:
                df = df[(df["dpd"] >= 331) & (df["dpd"] <= 359)]

        # Convert metric fields to numeric
        for field in metrics_dict:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')

        # Grouped Aggregation
        if group_by_field:
            grouped = df.groupby(group_by_field)
            result_list = []

            for group_val, group_df in grouped:
                row = {group_by_field: group_val}

                if group_by_field == "cust_id" and "cust_name" in df.columns:
                    cust_name_val = group_df["cust_name"].dropna().iloc[0] if not group_df["cust_name"].dropna().empty else None
                    row["cust_name"] = cust_name_val
                elif group_by_field == "cust_name" and "cust_id" in df.columns:
                    cust_id_val = group_df["cust_id"].dropna().iloc[0] if not group_df["cust_id"].dropna().empty else None
                    row["cust_id"] = cust_id_val

                for field, agg_type in metrics_dict.items():
                    if field not in group_df.columns:
                        row[field] = None
                        continue

                    if agg_type == "sum":
                        value = group_df[field].sum()
                        col_name = f"{field}_sum"
                    elif agg_type == "count":
                        value = group_df[field].nunique()
                        col_name = f"{field}_count"
                    elif agg_type == "mean":
                        value = group_df[field].mean()
                        col_name = f"{field}_mean"

                    elif agg_type == "weighted_average":
                        weight_field = "exposure"
                        if weight_field not in group_df.columns:
                            value = None
                        else:
                            w_sum = group_df[weight_field].sum()
                            value = (group_df[field] * group_df[weight_field]).sum() / w_sum if w_sum != 0 else 0
                        col_name = field + "_wavg"
                    

                    if isinstance(value, (np.integer, np.int64)):
                        value = int(value)
                    elif isinstance(value, (np.floating, np.float64)):
                        value = round(float(value), 2)
                    row[col_name] = value

                if additional_field and additional_field in group_df.columns:
                    extra = group_df[additional_field].dropna().iloc[0] if not group_df[additional_field].dropna().empty else None
                    if isinstance(extra, (np.integer, np.int64)):
                        extra = int(extra)
                    elif isinstance(extra, (np.floating, np.float64)):
                        extra = float(extra)
                    row[additional_field] = extra

                if "exposure_sum" in row and "provision_sum" in row and row["exposure_sum"] != 0:
                    row["provision_percentage"] = round((row["provision_sum"] / row["exposure_sum"]) * 100, 2)

                result_list.append(row)

            # Build full index of all possible group_by_field values
            if group_by_field == "rating":
                all_vals = sorted(self.df_rating["internal_rating"].dropna().unique())
                full_index = pd.DataFrame({group_by_field: all_vals})
            elif group_by_field == "group":
                all_vals = sorted(self.df_customer["group_id"].dropna().unique().astype(int))
                full_index = pd.DataFrame({group_by_field: all_vals})
            elif group_by_field in self.df_customer.columns:
                all_vals = self.df_customer[group_by_field].dropna().unique()
                full_index = pd.DataFrame({group_by_field: all_vals})
            # elif group_by_field == "staging":
            #     series = self.df_joined["staging"].dropna()
            #     cleaned_series = pd.Series(series).apply(lambda x: str(x).strip() if isinstance(x, str) else str(x))
            #     all_vals = natsorted(cleaned_series.unique())
            #     full_index = pd.DataFrame({group_by_field: all_vals})
            else:
                all_vals = df[group_by_field].dropna().unique()
                full_index = pd.DataFrame({group_by_field: all_vals})

            # Merge full index with result
            result_df = pd.DataFrame(result_list)
            """final_df = pd.merge(full_index, result_df, on=group_by_field, how="left")

            # Fill missing values with 0 for numeric columns
            for col in final_df.columns:
               if col != group_by_field and pd.api.types.is_numeric_dtype(final_df[col]):
                   final_df[col] = final_df[col].fillna(0).round(2)

            if top_n is not None and day_flag is not None:
                if "exposure_sum" in final_df.columns:
                    final_df = final_df.sort_values(by="exposure_sum", ascending=False).head(top_n)

            result = final_df.to_dict(orient="records")"""
            #if day_flag is  None:
                

            if day_flag and top_n  and  "exposure_sum" in result_df.columns and group_by_field  in ["cust_id","cust_name"]:
                result_df = result_df.sort_values(by="exposure_sum", ascending=False).head(top_n)
                # fill missing numeric columns with 0s (if any) — no merge with full_index needed
                for col in result_df.columns:
                    if col != group_by_field and pd.api.types.is_numeric_dtype(result_df[col]):
                        result_df[col] = result_df[col].fillna(0).round(2)
                result = result_df.to_dict(orient="records")
            else:
                final_df = pd.merge(full_index, result_df, on=group_by_field, how="left")
                for col in final_df.columns:
                    if col != group_by_field and pd.api.types.is_numeric_dtype(final_df[col]):
                        final_df[col] = final_df[col].fillna(0).round(2)
                result = final_df.to_dict(orient="records")
    
            header = {}
            if filters_dict:
                for field, value in filters_dict.items():
                    header[field] = int(value) if field == "group" else value

            if top_n and day_flag :
                header["top_n"] = top_n
                header["day_flag"] = day_flag
                
            if header:
                return [header] + result
            else:
                return result

        # Flat Aggregation
        else:
            result = {}
            
            for field, agg_type in metrics_dict.items():
                if field not in df.columns:
                    result[field] = None
                    continue

                if agg_type == "sum":
                    value = df[field].sum()
                    col_name = f"{field}_sum"
                elif agg_type == "count":
                    value = df[field].nunique()
                    col_name = f"{field}_count"
                elif agg_type == "mean":
                    value = df[field].mean()
                    col_name = f"{field}_mean"

                elif agg_type == "weighted_average":
                    weight_field = "exposure"
                    if weight_field not in df.columns:
                        value = None
                    else:
                        w_sum = df[weight_field].sum()
                        value = (df[field] * df[weight_field]).sum() / w_sum if w_sum != 0 else 0
                    col_name = field + "_wavg"
                else:
                    return {"error": f"Unsupported aggregation type: {agg_type}"}

                if isinstance(value, (np.integer, np.int64)):
                    value = int(value)
                elif isinstance(value, (np.floating, np.float64)):
                    value = round(float(value), 2)

                result[col_name] = value

            
            if additional_field and additional_field in df.columns:
                extra = df[additional_field].dropna().iloc[0] if not df[additional_field].dropna().empty else None
                if isinstance(extra, (np.integer, np.int64)):
                    extra = int(extra)
                elif isinstance(extra, (np.floating, np.float64)):
                    extra = float(extra)
                result[additional_field] = extra
            if "exposure_sum" in result and "provision_sum" in result and result["exposure_sum"] != 0:
                result["provision_percentage"] = round((result["provision_sum"] / result["exposure_sum"]) * 100, 2)

            if filters_dict:
                header = {}
                for field, value in filters_dict.items():
                    header[field] = int(value) if field == "group" else value
                result = {**header, **result}

            return result
    
    
    def get_dynamic_distribution(
            self,
            fact_field: str,
            group_by_field: str = "collateral_type",
            date_filter: Optional[str] = None,
            dimension_filter_field: Optional[str] = None,
            dimension_filter_value: Optional[str] = None,
            apply_haircut: bool = False,
            source: str = "risk"
        ) -> Dict[str, List[Dict[str, Union[str, int, float]]]]:

        if source != "risk":
            return {"error": f"Unsupported source '{source}'. Only 'risk' is allowed."}

        if self.df_joined is None:
            return {"error": "No joined fact risk data available"}

        df = self.df_joined.copy()

        # --- Normalize column names early ---
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(",", "")      # remove commas first
            .str.replace("&", "and")   # replace ampersands
            .str.replace(" ", "_")     # then replace spaces
        )

        print("\n--- Available Columns After Normalization ---")
        print(df.columns.tolist())
        print(df.head(2))

        # Validate fact_field and group_by_field
        if fact_field != "total_collateral":
            return {"error": "Fact field must be 'total_collateral' only."}

        if group_by_field != "collateral_type":
            return {"error": "Group by field must be 'collateral_type' only."}

        # Apply date filter
        if date_filter:
            try:
                df["date"] = pd.to_datetime(df["date"], dayfirst=True)
                df = df[df["date"] == pd.to_datetime(date_filter, dayfirst=True)]
            except Exception:
                return {"error": "Invalid date format. Use DD/MM/YYYY."}

        # Optional dimension filter
        valid_filters = ["cust_id", "sector", "group_id"]
        if dimension_filter_field and dimension_filter_value:
            field = dimension_filter_field.strip().lower().replace(" ", "_")
            if field not in df.columns or field not in valid_filters:
                return {"error": f"Invalid filter field '{dimension_filter_field}'. Must be one of {valid_filters}."}

            if field == "group_id":
                try:
                    df = df[df[field] == int(dimension_filter_value)]
                except ValueError:
                    return {"error": f"Invalid value '{dimension_filter_value}' for group_id; must be an integer."}
            else:
                df = df[df[field].astype(str).str.lower() == str(dimension_filter_value).lower()]

        if df.empty:
            return {"error": "No data after applying filters."}

        # Corrected column keys
        column_map = {
            False: {
                "collateral_land_and_building": "Collateral Land & Building",
                "collateral_cash_gold_and_other_riskfree_assests": "Collateral Cash, Gold & Other Riskfree Assets",
                "collateral_shares_and_other_paper_assests": "Collateral Shares & Other Paper Assets",
                "collateral_hawalat_haq": "Others"
            },
            True: {
                "hc_collateral_land_and_building": "Collateral Land & Building",
                "hc_collateral_cash_gold_and_other_riskfree_assests": "Collateral Cash, Gold & Other Riskfree Assets",
                "hc_collateral_shares_and_other_paper_assests": "Collateral Shares & Other Paper Assets",
                "hc_collateral_hawalat_haq": "Others"
            }
        }

        selected_columns = column_map[apply_haircut]
        result = []
        total_sum = 0

        for col, display_name in selected_columns.items():
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(",", "").fillna("0").astype(float)
                col_sum = df[col].sum()
            else:
                col_sum = 0
            result.append({"collateral_type": display_name, "total": int(col_sum), "percentage": 0.0})
            total_sum += col_sum

        for item in result:
            item["percentage"] = round((item["total"] / total_sum * 100), 2) if total_sum > 0 else 0.0

        return {"data": result}

    def get_summary_table(self, date_filter: str, top_n: int = 10, filter_field: Optional[str] = None, filter_value: Optional[str] = None):
        df = self.df_joined.copy()
        try:
            target_date = pd.to_datetime(date_filter, dayfirst=True)
        except:
            raise HTTPException(status_code=422, detail="Invalid date format. Use DD/MM/YYYY.")

        df = df[df["date"] == target_date]

        if filter_field and filter_value:
            if filter_field not in df.columns:
                raise HTTPException(status_code=400, detail=f"Column '{filter_field}' not found.")
            if filter_field == "group_id":
                try:
                    df = df[df["group_id"] == int(filter_value)]
                except ValueError:
                    raise HTTPException(status_code=400, detail="group_id must be an integer.")
            else:
                df = df[df[filter_field].str.lower() == filter_value.lower()]

        if df.empty:
            return {"error": "No data after applying filters."}

        if "cust_name" not in df.columns:
            for col in df.columns:
                if "name" in col and "cust" in col:
                    df = df.rename(columns={col: "cust_name"})
                    break

        total_exposure = df["exposure"].sum()
        total_hc = df["total_hc_collateral"].sum()
        coverage_ratio = (total_hc / total_exposure * 100) if total_exposure else 0

        grouped = (
            df.groupby(["cust_id", "cust_name"])
            .agg(
                exposure=("exposure", "sum"),
                total_collateral=("total_collateral", "sum"),
                total_hc_collateral=("total_hc_collateral", "sum")
            )
            .reset_index()
        )
        grouped["coverage_ratio"] = grouped["total_hc_collateral"] / grouped["exposure"] * 100

        grouped = grouped.round(2)
        for col in grouped.columns:
            grouped[col] = grouped[col].apply(lambda x: x.item() if hasattr(x, "item") else x)

        return {
            "total_exposure": round(float(total_exposure), 2),
            "hc_collateral": round(float(total_hc), 2),
            "coverage_ratio": round(float(coverage_ratio), 2),
            "top_customers": grouped.sort_values("total_collateral", ascending=False).head(top_n).to_dict(orient="records")
        }
               
    def trend_by_period(
        self,
        end_date: str,
        period_type: str,
        fact_fields_str: str,
        filter_field: Optional[str],
        filter_value: Optional[str]
    ):
        df = self.df_joined.copy()

        try:
            end = pd.to_datetime(end_date, dayfirst=True)
        except:
            raise HTTPException(422, "Invalid date format. Use DD/MM/YYYY.")

        if period_type not in ("M", "Q"):
            raise HTTPException(422, "period_type must be 'M' or 'Q'")

        # Parse comma-separated fields
        fields = [f.strip() for f in fact_fields_str.split(",") if f.strip()]
        if "exposure" not in fields or len(fields) < 2:
            raise HTTPException(400, "fact_fields must include 'exposure' and at least one other field")

        for f in fields:
            if f not in df.columns:
                raise HTTPException(400, f"Field '{f}' not found in data.")

        if filter_field and filter_value:
            if filter_field not in df.columns:
                raise HTTPException(400, f"Filter field '{filter_field}' not found")
            if filter_field == "group_id":
                try:
                    df = df[df["group_id"] == int(filter_value)]
                except:
                    raise HTTPException(400, "group_id must be integer")
            else:
                df = df[df[filter_field].astype(str).str.lower() == filter_value.lower()]

        # Assign periods
        if period_type == "M":
            df["period_date"] = df["date"].values.astype("datetime64[M]")
        else:
            df["period_date"] = df["date"].dt.to_period("Q").dt.to_timestamp("Q") + pd.offsets.QuarterEnd(0)

        df = df[df["period_date"] <= end]
        if df.empty:
            return {"error": "No data after applying filters."}

        # Aggregate selected fields
        grp = (
            df.groupby("period_date")[fields]
            .sum()
            .reset_index()
            .sort_values("period_date", ascending=False)
            .head(6)
            .sort_values("period_date")
        )

        # Format date label
        grp["date"] = grp["period_date"].dt.strftime("%b %Y")

        # Calculate coverage ratio for each non-exposure field
        for f in fields:
            if f != "exposure":
                grp[f"coverage_ratio_{f}"] = grp[f] / grp["exposure"] * 100

        # Build final output columns
        output_fields = ["date"] + fields + [f"coverage_ratio_{f}" for f in fields if f != "exposure"]
        grp = grp[output_fields].round(2)

        # Convert numpy types to native Python types
        for col in grp.columns:
            grp[col] = grp[col].apply(lambda x: x.item() if hasattr(x, "item") else x)

        return grp.to_dict("records")

    
    def get_customer_details(
        self,
        attributes: str,
        customer_fields: str,
        base_date: str,
        comparison_date: Optional[str] = None,
        top_n: Optional[int] = None,
        period_type: Optional[str] = "M",
        lookback: Optional[int] = None,
        day_flag: Optional[int] = None,
        dimension_filters: Optional[str] = None,):
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
        filters_dict = {}
        result_cust_df=pd.DataFrame()
        # Validate and parse inputs
        attributes = [attr.strip() for attr in attributes.split(',') if attr.strip()]
        customer_cols = [col.strip() for col in customer_fields.split(',') if col.strip()]

        for field in attributes + customer_cols:
            if field not in df.columns:
                raise ValueError(f"Field '{field}' is not found in the dataset.")

        if base_date:
            try:
                base_date_obj = pd.to_datetime(base_date, dayfirst=True)
            except Exception:
                raise ValueError(f"Invalid date format: '{base_date}'. Please use 'dd/mm/yyyy'.")

        if comparison_date:
            try:
                comp_date_obj = pd.to_datetime(comparison_date, dayfirst=True)
            except Exception:
                raise ValueError(f"Invalid date format: '{comparison_date}'. Please use 'dd/mm/yyyy'.")
        
        if top_n is not None:
            if not isinstance(top_n, int) or top_n <= 0:
                raise ValueError("Invalid top_n: '{top_n}'.It must be a positive integer.")
        
        if period_type not in ("M", "Q"):
            raise ValueError(f"Unexpected value;Only 'M' (monthly) or 'Q' (quarterly) are allowed.")
        if lookback:
            if not isinstance(lookback, int) or lookback <= 0:
                raise ValueError("Invalid lookback value. It must be a positive integer greater than 0.")
            
        if day_flag is not None:
            if not isinstance(day_flag, int) or day_flag not in [90, 180, 360]:
                raise ValueError("day_flag must be one of [90, 180, 360]")
            
            if "dpd" not in df.columns:
                raise ValueError("dpd field is required for day_flag filtering.")
            
            if day_flag == 90:
                df = df[(df["dpd"] >= 61) & (df["dpd"] <= 89)]
            elif day_flag == 180:
                df = df[(df["dpd"] >= 151) & (df["dpd"] <= 179)]
            elif day_flag == 360:
                df = df[(df["dpd"] >= 331) & (df["dpd"] <= 359)]
            
        # Helper to format Month-Year
        def format_month_year(date_str):
            date_obj = pd.to_datetime(date_str, dayfirst=True)
            return date_obj.strftime('%b-%Y')

        result = {}

        if 'cust_id' in customer_cols and 'cust_name' not in customer_cols and 'cust_name' in df.columns:
            customer_cols.append('cust_name')

        if base_date:
            base_label = format_month_year(base_date)
            base_subset = df[df["date"] == base_date_obj]

            if base_subset.empty:
                raise FileNotFoundError("No data found for the specified base date.")

            if comparison_date and lookback is None:
                comp_label = format_month_year(comparison_date)
                result["base_period"] = base_label
                result["comparison_period"] = comp_label

                if dimension_filters:
                    for pair in dimension_filters.split(','):
                        if ':' in pair:
                            field, value = [p.strip() for p in pair.split(':', 1)]
                            if field not in base_subset.columns:
                                raise ValueError(f"Dimension filter field '{field}' is not found in the dataset.")
                            filters_dict[field] = value
                            if pd.api.types.is_numeric_dtype(base_subset[field]):
                                base_subset = base_subset[base_subset[field] == pd.to_numeric(value, errors='coerce')]
                            else:
                                base_subset = base_subset[base_subset[field].astype(str) == str(value)]
                        else:
                            raise ValueError(f"Invalid dimension filter format: '{pair}'. Expected 'field:value'.")

                if base_subset.empty:
                    raise FileNotFoundError("No data found after applying filters on base date.")

                customer_ids = base_subset["cust_id"].unique()
                base_filtered = base_subset[base_subset["cust_id"].isin(customer_ids)]
                comp_subset = df[(df["date"] == comp_date_obj) & (df["cust_id"].isin(customer_ids))]

                customer_cols_clean = [col for col in customer_cols if col != "cust_id"]

                base_data = base_filtered[["cust_id"] + customer_cols_clean + attributes].drop_duplicates(subset=["cust_id"]).rename(
                columns={attr: f"{attr}_{base_label}" for attr in attributes})
                comp_data = comp_subset[["cust_id"] + attributes].drop_duplicates(subset=["cust_id"]).rename(
                columns={attr: f"{attr}_{comp_label}" for attr in attributes})

                merged = pd.merge(base_data, comp_data, on="cust_id", how="left")
                result_cust_df=merged
                #result["customers"] = merged.to_dict(orient="records")
            elif lookback and period_type and comparison_date is None:
                base_date_obj = pd.to_datetime(base_date)
                period_list = [(base_date_obj - relativedelta(months=i if period_type == "M" else i * 3)).to_period(period_type) for i in range(lookback + 1)]
                period_list = sorted(period_list, reverse=True)
                #period_labels = [format_month_year(str(p)) for p in period_list] 
                period_labels = []
                for p in period_list:
                    if period_type == "Q":
                        date_str = p.end_time.strftime('%Y-%m-%d') 
                    else:
                        date_str = p.strftime('%Y-%m-%d')
                    label = format_month_year(date_str)
                    period_labels.append(label)

                # Assign period column as Period dtype
                df["period"] = df["date"].dt.to_period(period_type)

                base_period_obj = base_date_obj.to_period(period_type)
                base_subset = df[df["period"] == base_period_obj]

                if base_subset.empty:
                    raise FileNotFoundError("No data found for the base period.")
                
                if dimension_filters:
                    for pair in dimension_filters.split(','):
                        if ':' in pair:
                            field, value = [p.strip() for p in pair.split(':', 1)]
                            if field not in base_subset.columns:
                                raise ValueError(f"Dimension filter field '{field}' is not found in the dataset.")
                            if pd.api.types.is_numeric_dtype(base_subset[field]):
                                base_subset = base_subset[base_subset[field] == pd.to_numeric(value, errors='coerce')]
                            else:
                                base_subset = base_subset[base_subset[field].astype(str) == str(value)]
                        else:
                            raise ValueError(f"Invalid dimension filter format: '{pair}'. Expected 'field:value'.")

                if base_subset.empty:
                    raise FileNotFoundError("No data found after applying filters on base period.")

                customer_ids = base_subset["cust_id"].unique()
                customer_cols_clean = [col for col in customer_cols if col != "cust_id"]
                # Prepare base period dataframe with renamed columns
                base_data = base_subset[["cust_id"] + customer_cols_clean + attributes].drop_duplicates(subset=["cust_id"]).rename(columns={attr: f"{attr}_{period_labels[0]}" for attr in attributes})
                merged_df = base_data.copy()
                for i, period_date in enumerate(period_list[1:], start=1):
                    period_label = period_labels[i]
                    comp_subset = df[(df["period"] == period_date) & (df["cust_id"].isin(customer_ids))]
                    if comp_subset.empty:
                        zero_data = pd.DataFrame({"cust_id": customer_ids, **{f"{attr}_{period_label}": 0 for attr in attributes}})
                        merged_df = pd.merge(merged_df, zero_data, on="cust_id", how="left")
                        continue

                    comp_data = comp_subset[["cust_id"] + attributes].drop_duplicates(subset=["cust_id"]).rename(columns={attr: f"{attr}_{period_label}" for attr in attributes})
                    merged_df = pd.merge(merged_df, comp_data, on="cust_id", how="left")
                result_cust_df = merged_df
                result["base_period"] = period_labels[0]  
                result["other_periods"] = period_labels[1:]
                
            elif lookback is None and comparison_date is None:
                if dimension_filters:
                    for pair in dimension_filters.split(','):
                        if ':' in pair:
                            field, value = [p.strip() for p in pair.split(':', 1)]
                            if field not in df.columns:
                                raise ValueError(f"Dimension filter field '{field}' is not found in the dataset.")
                            filters_dict[field] = value
                            if pd.api.types.is_numeric_dtype(df[field]):
                                df = df[df[field] == pd.to_numeric(value, errors='coerce')]
                            else:
                                df = df[df[field].astype(str) == str(value)]
                        else:
                            raise ValueError(f"Invalid dimension filter format: '{pair}'. Expected 'field:value'.")

                if df.empty:
                    raise FileNotFoundError("No data found after applying filters.")

                result["base_period"] = base_label
                #base_result = df[df["date"] == base_date_obj][["cust_id"] + customer_cols + attributes].drop_duplicates(subset=["cust_id"])
                #result["customers"] = base_result.to_dict(orient="records")
                result_cust_df = df[df["date"] == base_date_obj][["cust_id"] + customer_cols + attributes].drop_duplicates(subset=["cust_id"])
            """if top_n:
                exposure_col = f"exposure_{base_label}" if (comparison_date or lookback) else "exposure"
                if exposure_col not in result_cust_df.columns:
                    raise ValueError(f"Cannot sort by exposure. Exposure related column is not found.")
                result_cust_df = result_cust_df.sort_values(by=exposure_col, ascending=False).head(top_n)"""
            exposure_col = f"exposure_{base_label}" if (comparison_date or lookback) else "exposure"
            if exposure_col not in result_cust_df.columns:
                raise ValueError("Cannot sort by exposure. Exposure related column is not found.")
            result_cust_df = result_cust_df.sort_values(by=exposure_col, ascending=False)
            if top_n:
                result_cust_df = result_cust_df.head(top_n)
            
            #result["customers"] = result_cust_df.to_dict(orient="records")
            try:
                #result["customers"] = result_cust_df.fillna(value=None).to_dict(orient="records")
                result["customers"] = result_cust_df.replace({np.nan: None}).to_dict(orient="records")
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error converting DataFrame: {e}")

        if dimension_filters:
            result["filters"] = filters_dict

        return result
    
    def writtenoff_customer_details(self,
    attributes: str,
    customer_fields: str,
    base_date: str,
    dimension_filters: Optional[str] = None):
        
        if self.df_written is None:
            raise FileNotFoundError("Source data is not found.")
        
        df = self.df_written.copy()
        result = {}
        result_cust_df=pd.DataFrame()
        # Validate and parse inputs
        attributes = [attr.strip() for attr in attributes.split(',') if attr.strip()]
        customer_cols = [col.strip() for col in customer_fields.split(',') if col.strip()]
        selected_fields = customer_cols + attributes

        for field in attributes + customer_cols:
            if field not in df.columns:
                raise ValueError(f"Field '{field}' is not found in the dataset.")

        
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        if base_date:
            try:
                date_val = pd.to_datetime(base_date, dayfirst=True)
                df = df[df["date"] == date_val]
                result["base_period"] = date_val.strftime('%b %Y')
            except Exception:
                raise ValueError(f"Invalid date format: '{base_date}'. Please use 'dd/mm/yyyy' format.")
            
        filters_dict = {}
        if dimension_filters:
            for pair in dimension_filters.split(','):
                if ':' in pair:
                    field, value = [p.strip() for p in pair.split(':', 1)]
                    if field not in df.columns:
                        raise ValueError(f"Dimension filter field '{field}' is not found in the dataset.")
                    filters_dict[field] = value
                else:
                    raise ValueError(f"Invalid dimension filter format: '{pair}'. Expected 'field:value'.")

        for field, value in filters_dict.items():
            if pd.api.types.is_numeric_dtype(df[field]):
                df = df[df[field] == pd.to_numeric(value, errors='coerce')]
            else:
                df = df[df[field].astype(str) == str(value)]
        
        if df.empty:
            raise FileNotFoundError("No data found after applying filters.")

        result_cust_df = df[selected_fields]      
        if 'amount' in result_cust_df.columns:
            result_cust_df = result_cust_df.sort_values(by='amount', ascending=False)
        else:
            raise ValueError("Field 'amount' is not available for sorting. Please include it in the attributes.")

        result["customers"] = result_cust_df.replace({np.nan: None}).to_dict(orient="records")
        if filters_dict:
            result["filters"] = filters_dict
        return result
    
    
    def get_provision_distr_type(self, dimension_filters: Optional[str] = None, date_filter: Optional[str] = None):
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)

        if dimension_filters:
            filters_dict = {}
            for pair in dimension_filters.split(','):
                if ':' in pair:
                    field, value = [p.strip() for p in pair.split(':', 1)]
                    if field not in df.columns:
                        raise ValueError(f"Dimension filter field '{field}' is not found in the dataset.")
                    filters_dict[field] = value
                else:
                    raise ValueError(f"Invalid dimension filter format: '{pair}'. Expected 'field:value'.")

            for field, value in filters_dict.items():
                if pd.api.types.is_numeric_dtype(df[field]):
                    df = df[df[field] == pd.to_numeric(value, errors='coerce')]
                else:
                    df = df[df[field].astype(str) == str(value)]

        if date_filter:
            try:
                date_obj = pd.to_datetime(date_filter, dayfirst=True)
                df = df[df["date"] == date_obj]
            except Exception:
                raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy' format.")
        
        if "direct_provision" in df.columns and "indirect_provision" in df.columns:
            result = { "Direct": float(df["direct_provision"].sum()),
            "Indirect": float(df["indirect_provision"].sum())}
        else:
            raise ValueError("Required columns 'direct_provision' and 'indirect_provision' not found in data.")
        return result


    def get_transition_matrix(
        self,
        fact_field: str,
        base_date: str,
        comparison_date: Optional[str] = None,
        dimension_filters: Optional[str] = None,
        column_field: Optional[str] = None,
        output_mode: str = "absolute"):
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()

        # Validate fact_field
        if fact_field not in df.columns:
            raise ValueError(f"Fact field '{fact_field}' is not found in the dataset.")

        # Validate output_mode
        if output_mode not in ["absolute", "percentage"]:
            raise ValueError(f"Invalid output mode '{output_mode}'. Only 'absolute' or 'percentage' allowed.")

        # Validate and parse dates
        if base_date:
            try:
                base_date_obj = pd.to_datetime(base_date, dayfirst=True)
            except Exception:
                raise ValueError(f"Invalid base date format: '{base_date}'.Please use 'dd/mm/yyyy.")
            
        if comparison_date:
            try:
                comp_date_obj = pd.to_datetime(comparison_date, dayfirst=True)
            except Exception:
                raise ValueError(f"Invalid comparison date format:'{comparison_date}'. Please use 'dd/mm/yyyy'.")

        result = {}

        # Apply dimension filters
        if dimension_filters:
            for pair in dimension_filters.split(','):
                if ':' in pair:
                    field, value = [p.strip() for p in pair.split(':', 1)]
                    if field not in df.columns:
                        raise ValueError(f"Dimension filter field '{field}' is not found in the dataset.")
                    result[field] = value
                    if pd.api.types.is_numeric_dtype(df[field]):
                        df = df[df[field] == pd.to_numeric(value, errors='coerce')]
                    else:
                        df = df[df[field].astype(str) == str(value)]
                else:
                    raise ValueError(f"Invalid dimension filter format: '{pair}'. Expected format 'field:value'.")

        if df.empty:
            raise FileNotFoundError("No data available after applying dimension filters.")

        #base_label = base_date_obj.strftime('%b-%Y')
        #comp_label = comp_date_obj.strftime('%b-%Y')
        result["base_period"] = base_date_obj.strftime('%b-%Y')
        if comparison_date:
            result["comparison_period"] = comp_date_obj.strftime('%b-%Y')
            base_df = df[df["date"] == base_date_obj][["cust_id", fact_field]].drop_duplicates(subset=["cust_id"]).rename(columns={fact_field: "base_value"})
            comp_df = df[df["date"] == comp_date_obj][["cust_id", fact_field]].drop_duplicates(subset=["cust_id"]).rename(columns={fact_field: "comp_value"})
            merged = pd.merge(comp_df, base_df, on="cust_id", how="outer")
            if merged.empty:
                raise FileNotFoundError("No matching records found between base and comparison dates.")

            merged["comp_value"] = merged["comp_value"].fillna("Unrated")
            merged["base_value"] = merged["base_value"].fillna("Unrated")
            merged["comp_value"] = merged.apply(
            lambda x: "New" if x["comp_value"] == "Unrated" and x["base_value"] != "Unrated" else x["comp_value"], axis=1)
            merged["base_value"] = merged.apply(
            lambda x: "Closed" if x["base_value"] == "Unrated" and x["comp_value"] != "Unrated" else x["base_value"], axis=1)

            if fact_field == "rating":
                all_cats = sorted(self.df_rating["internal_rating"].dropna().unique().astype(int).tolist())
            elif fact_field == "group":
                all_cats = sorted(self.df_customer["group_id"].dropna().unique().astype(int).tolist())
            else:
                all_cats = sorted(df[fact_field].dropna().unique().tolist(), key=str)

            row_cats = all_cats + ["Unrated", "New"]
            col_cats = all_cats + ["Unrated", "Closed"]
            matrix = pd.DataFrame(0, index=row_cats, columns=col_cats)
            for _, row in merged.iterrows():
                matrix.loc[row["comp_value"], row["base_value"]] += 1

            matrix["Total"] = matrix.sum(axis=1)
            total_row = matrix.sum(axis=0)
            matrix.loc["Total"] = total_row

            if matrix.empty or matrix["Total"]["Total"] == 0:
                raise FileNotFoundError("No data available to build transition matrix.")

            if output_mode == "percentage":
                matrix = matrix.div(matrix.loc["Total", "Total"]).fillna(0).round(2) * 100
            
            result["headers"] = [str(c) for c in matrix.columns]
            result["rows"] = [str(r) for r in matrix.index]
            result["values"] = matrix.values.tolist()
        elif column_field and comparison_date is None:
            if column_field not in df.columns:
                raise ValueError(f"Column field '{column_field}' is not found in the dataset.")
    
            single_date_df = df[df["date"] == base_date_obj]
            if single_date_df.empty:
                raise FileNotFoundError("No data available for base_date after filtering.")
            if fact_field == "rating":
                row_cats = sorted(self.df_rating["internal_rating"].dropna().unique().astype(int).tolist())
            elif fact_field == "group":
                row_cats = sorted(self.df_customer["group_id"].dropna().unique().astype(int).tolist())
            else:
                row_cats = sorted(df[fact_field].dropna().unique().tolist(), key=str)
    
            if column_field == "rating":
                col_cats = sorted(self.df_rating["internal_rating"].dropna().unique().astype(int).tolist())
            elif column_field == "group":
                col_cats = sorted(self.df_customer["group_id"].dropna().unique().astype(int).tolist())
            else:
                col_cats = sorted(df[column_field].dropna().unique().tolist(), key=str)
                
            pivot = single_date_df.pivot_table(index=fact_field,columns=column_field,values="cust_id",
                            aggfunc=lambda x: x.nunique(),fill_value=0)
            
            pivot = pivot.reindex(index=row_cats, columns=col_cats, fill_value=0)
            pivot["Total"] = pivot.sum(axis=1)
            total_row = pivot.sum(axis=0)
            pivot.loc["Total"] = total_row
            
            if output_mode == "percentage":
                total = pivot.loc["Total", "Total"]
                if total == 0:
                    raise ValueError("Cannot compute percentage. Total is zero.")
                pivot = (pivot.div(total) * 100).fillna(0).round(2)
            matrix = pivot
            result["headers"] = [str(col) for col in matrix.columns]
            result["rows"] = [str(idx) for idx in matrix.index]
            result["values"] = matrix.values.tolist()
        return result
    
    def get_metric_ratio(
    self,
    numerator_field: str,
    denominator_field: str = "exposure",
    group_by_field: Optional[str] = None,
    date_filter: Optional[str] = None,
    dimension_filter_field: Optional[str] = None,
    dimension_filter_value: Optional[str] = None
    ):
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()

        # Validate fields
        for field in [numerator_field, denominator_field]:
            if field not in df.columns:
                raise ValueError(f"Field '{field}' is not found in the dataset.")
            
        if numerator_field not in ["provision", "total_hc_collateral"]:
            raise ValueError(f"'{numerator_field}' is not a valid numeric field.")

        if group_by_field and group_by_field not in df.columns:
            raise ValueError(f"Group by field '{group_by_field}' is not found in the dataset.")

        if dimension_filter_field:
            if dimension_filter_field not in df.columns:
                raise ValueError(f"Dimension filter field '{dimension_filter_field}' is not found in the dataset.")
            if dimension_filter_value is None:
                raise ValueError("Dimension filter value is missing for the given dimension filter field.")

        # Apply date filter
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        if date_filter:
            date_obj = pd.to_datetime(date_filter, dayfirst=True)
            df = df[df["date"] == date_obj]

        # Apply dimension filter
        if dimension_filter_field and dimension_filter_value:
            if pd.api.types.is_numeric_dtype(df[dimension_filter_field]):
                df = df[df[dimension_filter_field] == pd.to_numeric(dimension_filter_value, errors='coerce')]
            else:
                df = df[df[dimension_filter_field].astype(str) == str(dimension_filter_value)]

            if df.empty:
                raise FileNotFoundError("No data available after applying filters.")
            
        label = "provision_percentage" if numerator_field == "provision" else "coverage_ratio"
        if group_by_field:
            grouped = df.groupby(group_by_field).agg(
                numerator_sum=(numerator_field, 'sum'),
                denominator_sum=(denominator_field, 'sum')
            ).reset_index()

            grouped[label] = grouped.apply(
                lambda row: round((row["numerator_sum"] / row["denominator_sum"]) * 100, 2)
                if row["denominator_sum"] != 0 else None, axis=1
            )

            result = grouped[[group_by_field, label]].to_dict(orient="records")

            if not result:
                raise FileNotFoundError("No data available after applying filters and calculations.")

            if dimension_filter_field and dimension_filter_value:
                result.insert(0, {dimension_filter_field: dimension_filter_value})

            return result

        else:
            numerator_sum = df[numerator_field].sum()
            denominator_sum = df[denominator_field].sum()

            if denominator_sum == 0:
                raise FileNotFoundError("No valid denominator found to compute coverage ratio.")

            ratio = round((numerator_sum / denominator_sum) * 100, 2)

            result = {label: ratio}
            if dimension_filter_field and dimension_filter_value:
                result = {dimension_filter_field: dimension_filter_value, **result}

            return result 
        
        
    from urllib.parse import unquote

    def get_collateral_distribution(
        self,
        category_level: str,
        sub_category_level: Optional[str],
        date_filter: str,
        apply_haircut: bool
    ) -> Dict[str, Any]:

        if self.df_collateral_joined is None:
            raise FileNotFoundError("Collateral data is not available.")

        def slugify(name: str) -> str:
            return name.strip().lower().replace(" ", "_").replace("&", "and")

        def deslugify(slug: str) -> str:
            return unquote(slug).replace("and", "&").replace("_", " ").title().strip()

        def resolve_slug(slug: str, valid_options: List[str]) -> Optional[str]:
            normalized_map = {slugify(option): option for option in valid_options}
            return normalized_map.get(slugify(unquote(slug)))

        category_level_actual = resolve_slug(category_level, self.valid_collateral_types)
        if not category_level_actual:
            raise ValueError(f"Invalid category_level: '{category_level}'")

        sub_level_actual = None
        if sub_category_level:
            all_subcats = self.df_collateral_joined["collateral_category"].dropna().unique()
            sub_level_actual = resolve_slug(sub_category_level, all_subcats)
            if not sub_level_actual:
                raise ValueError(f"Invalid sub_category_level: '{sub_category_level}'")

        try:
            date_obj = pd.to_datetime(date_filter, dayfirst=True)
        except Exception:
            raise ValueError(f"Invalid date format: '{date_filter}' (Expected dd/mm/yyyy)")

        df = self.df_collateral_joined.copy()
        df = df[
            (df["collateral_type"] == category_level_actual) &
            (df["date"] == date_obj)
        ]

        # ✅ Deduplicate rows to avoid inflated totals from joins or double entries
        df = df.drop_duplicates(subset=[
            "customer_id", "collateral_value", "collateral_type",
            "collateral_category", "collateral_sub-category", "date"
        ])

        df["hair_cut"] = (
            pd.to_numeric(df["hair_cut"].astype(str).str.replace('%', ''), errors="coerce")
            .fillna(0) / 100
        )

        df["adjusted_collateral_value"] = (
            df["collateral_value"] * (1 - df["hair_cut"])
            if apply_haircut else df["collateral_value"]
        )

        if df.empty:
            return {
                "collateral_parent_type": sub_level_actual or category_level_actual,
                "data": []
            }

        # Sub-category view
        if sub_level_actual:
            sub_df = df[df["collateral_category"] == sub_level_actual]

            # ✅ Optional safety deduplication again at sub-level (edge case)
            sub_df = sub_df.drop_duplicates(subset=[
                "customer_id", "collateral_value", "collateral_type",
                "collateral_category", "collateral_sub-category", "date"
            ])

            if "collateral_sub-category" not in sub_df.columns:
                raise ValueError("Missing 'collateral_sub-category' in filtered data.")

            total = sub_df["adjusted_collateral_value"].sum()
            grouped = sub_df.groupby("collateral_sub-category")["adjusted_collateral_value"].sum().reset_index()

            data = [
                {
                    "collateral_type": slugify(row["collateral_sub-category"]),
                    "total": round(row["adjusted_collateral_value"]),
                    "percentage": round((row["adjusted_collateral_value"] / total) * 100) if total else 0
                }
                for _, row in grouped.iterrows()
            ]

            return {
                "collateral_parent_type": sub_category_level,
                "data": data
            }

        # Category view
        total = df["adjusted_collateral_value"].sum()
        grouped = df.groupby("collateral_category")["adjusted_collateral_value"].sum().reset_index()

        data = [
            {
                "collateral_type": slugify(row["collateral_category"]),
                "total": round(row["adjusted_collateral_value"]),
                "percentage": round((row["adjusted_collateral_value"] / total) * 100) if total else 0
            }
            for _, row in grouped.iterrows()
        ]

        return {
            "collateral_parent_type": category_level,
            "data": data
        }

    
    def get_top_collaterals(self, collateral_type: str, date_filter: str, top_n: Optional[int] = 10) -> List[Dict[str, Any]]:
        import os
        from urllib.parse import unquote
        from typing import Optional

        # Define slugify and resolve_slug inside this method
        def slugify(name: str) -> str:
            return name.strip().lower().replace(" ", "_").replace("&", "and")

        def resolve_slug(slug: str, valid_options: list) -> Optional[str]:
            normalized_map = {slugify(option): option for option in valid_options}
            return normalized_map.get(slugify(unquote(slug)))

        # Validate collateral_type against your valid types
        collateral_type_actual = resolve_slug(collateral_type, self.valid_collateral_types)
        if not collateral_type_actual:
            raise ValueError(f"Invalid collateral_type: '{collateral_type}'")

        collateral_type = collateral_type_actual

        if self.df_collateral is None or self.df_fact_risk is None:
            raise FileNotFoundError("Missing collateral or fact_risk data.")

        # Step 1: Load customer data locally from Excel to get cust_name
        customer_data = []
        for filename in os.listdir(self.data_folder):
            if filename.endswith(".xlsx") and not filename.startswith("~$"):
                path = os.path.join(self.data_folder, filename)
                try:
                    xls = pd.ExcelFile(path)
                    if "CUSTOMER" in xls.sheet_names:
                        df_cust = xls.parse("CUSTOMER")
                        df_cust.columns = [str(c).strip().lower().replace(" ", "_") for c in df_cust.columns]
                        customer_data.append(df_cust)
                except Exception as e:
                    logger.warning(f"Failed to read {filename}: {e}")
                    continue

        if not customer_data:
            raise FileNotFoundError("No valid CUSTOMER sheet found to extract cust_name.")

        df_customer = pd.concat(customer_data, ignore_index=True).drop_duplicates(subset=["cust_id"])

        # Step 2: Prepare working copies
        df_coll = self.df_collateral.copy()
        df_fact = self.df_fact_risk.copy()

        # Step 3: Clean and join
        df_coll["date"] = pd.to_datetime(df_coll["date"], errors="coerce", dayfirst=True)
        df_fact.columns = [str(c).strip().lower().replace(" ", "_") for c in df_fact.columns]
        df_fact["date"] = pd.to_datetime(df_fact["date"], errors="coerce", dayfirst=True)

        df = pd.merge(df_coll, df_customer, how="left", left_on="customer_id", right_on="cust_id")
        df = pd.merge(df, df_fact[["cust_id", "exposure", "date"]], how="left", on=["cust_id", "date"])

        try:
            date_obj = pd.to_datetime(date_filter, dayfirst=True)
        except Exception:
            raise ValueError("Invalid date format. Use DD/MM/YYYY.")

        df = df[(df["collateral_type"] == collateral_type) & (df["date"] == date_obj)].copy()

        if df.empty:
            return []

        # Haircut processing
        df["hair_cut"] = df["hair_cut"].astype(str).str.replace('%', '', regex=False).str.strip()
        df["hair_cut"] = pd.to_numeric(df["hair_cut"], errors="coerce") / 100.0
        df["hair_cut"] = df["hair_cut"].clip(lower=0, upper=1).fillna(0)

        df["collateral_value"] = pd.to_numeric(df["collateral_value"], errors="coerce").fillna(0)
        df["hc_collateral_value"] = df["collateral_value"] * (1 - df["hair_cut"])
        df["customer_hc_collateral"] = df["hc_collateral_value"]

        if "cust_name" not in df.columns:
            raise ValueError("cust_name still not found after merge.")

        grouped = df.groupby("collateral_name").agg({
            "date": "first",
            "collateral_type": "first",
            "collateral_grade": "mean",
            "collateral_value": "sum",
            "hc_collateral_value": "sum"
        }).reset_index()

        customer_info = df.groupby("collateral_name").apply(
            lambda x: x[["cust_name", "exposure", "customer_hc_collateral"]].to_dict(orient="records")
        ).reset_index(name="customers")

        result_df = pd.merge(grouped, customer_info, on="collateral_name", how="left")

        result = []
        for _, row in result_df.iterrows():
            entry = {
                "Date": row["date"].strftime('%d/%m/%Y'),
                "Collateral Name": row["collateral_name"],
                "Type": row["collateral_type"],
                "Grade": round(row["collateral_grade"], 2) if pd.notna(row["collateral_grade"]) else None,
                "Collateral Value": row["collateral_value"],
                "HC Collateral Value": row["hc_collateral_value"],
                "Customers": [
                    {
                        "Customer Name": cust["cust_name"],
                        "Customer Exposure": cust["exposure"],
                        "Customer HC Collateral": cust["customer_hc_collateral"]
                    }
                    for cust in row["customers"]
                ]
            }
            result.append(entry)

        result.sort(key=lambda x: x["Collateral Value"] if x["Collateral Value"] is not None else float('-inf'), reverse=True)
        return result[:top_n] if top_n is not None else result
    
    def get_available_additional_provision_percent(self) -> List[int]:
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        matching_cols = [
            int(col.replace("additional_provision_at_", "").replace("_percent", ""))
            for col in self.df_joined.columns
            if col.startswith("additional_provision_at_") and col.endswith("_percent")
        ]

        if not matching_cols:
            raise FileNotFoundError("No 'additional_provision_at_<x>_percent' columns found in the dataset.")

        return sorted(matching_cols)
    
    def calculate_incremental_provision(
    self,
    date_filter: str,
    additional_provision_percent: int,
    customize_additional_provision_percent: Optional[str] = None,
    dimension_filters: Optional[str] = None
    ):
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)

        try:
            date_obj = pd.to_datetime(date_filter, dayfirst=True)
        except Exception:
            raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy' format.")

        df = df[df["date"] == date_obj]

        filters_dict = {}
        if dimension_filters:
            for pair in dimension_filters.split(','):
                if ':' in pair:
                    field, value = [p.strip() for p in pair.split(':', 1)]
                    if field not in df.columns:
                        raise ValueError(f"Filter field '{field}' not found in dataset.")
                    filters_dict[field] = value
                    if pd.api.types.is_numeric_dtype(df[field]):
                        df = df[df[field] == pd.to_numeric(value, errors='coerce')]
                    else:
                        df = df[df[field] == value]
                else:
                    raise ValueError(f"Invalid filter format: '{pair}'. Expected 'field:value'.")

        if df.empty:
            raise FileNotFoundError("No data found for the given filters and date.")

        default_col = f"additional_provision_at_{additional_provision_percent}_percent"
        if default_col not in df.columns:
            raise ValueError(f"'{default_col}' column not found in dataset.")

        df["_provision"] = df[default_col]

        if customize_additional_provision_percent:
            try:
                custom_dict = parse_custom_ecl_param(customize_additional_provision_percent)
            except Exception:
                raise ValueError("Invalid format for 'customize_additional_provision_percent'. Use format like '1:2,3:5'")

            for cust_id, pct in custom_dict.items():
                col = f"additional_provision_at_{pct}_percent"
                if col not in df.columns:
                    raise ValueError(f"'{col}' column not found in dataset.")
                df.loc[df["cust_id"] == cust_id, "_provision"] = df.loc[df["cust_id"] == cust_id, col]

        total_provision = round(df["_provision"].sum(), 2)

        return {
            "incremental_provision": total_provision,
            "filters": filters_dict if filters_dict else None
        }
    
    def get_total_revised_provision(
    self,
    date_filter: str,
    additional_provision_percent: int,
    customize_additional_provision_percent: Optional[str] = None,
    dimension_filters: Optional[str] = None
    ):
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
        df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")

        try:
            date_obj = pd.to_datetime(date_filter, dayfirst=True)
        except Exception:
            raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy' format.")

        df = df[df["date"] == date_obj]

        filters_dict = {}
        if dimension_filters:
            for pair in dimension_filters.split(','):
                if ':' not in pair:
                    raise ValueError(f"Invalid filter format: '{pair}'. Expected 'field:value'.")
                field, value = pair.split(':', 1)
                field = field.strip()
                value = value.strip()
                if field not in df.columns:
                    raise ValueError(f"Dimension filter field '{field}' not found in data.")
                filters_dict[field] = value
                if pd.api.types.is_numeric_dtype(df[field]):
                    df = df[df[field] == pd.to_numeric(value, errors='coerce')]
                else:
                    df = df[df[field].astype(str) == value]

        if df.empty:
            raise FileNotFoundError("No data available after applying filters.")

        base_col = f"additional_provision_at_{additional_provision_percent}_percent"
        if base_col not in df.columns:
            raise ValueError(f"Column '{base_col}' not found in data.")

        df["total_provision"] = df["provision"] + df[base_col]

        if customize_additional_provision_percent:
            try:
                overrides = {
                    int(k.strip()): int(v.strip())
                    for k, v in (item.split(':') for item in customize_additional_provision_percent.split(','))
                }
            except Exception:
                raise ValueError("Invalid format for 'customize_additional_provision_percent'. Expected '1:5,2:7,...'")

            for cust_id, custom_percent in overrides.items():
                custom_col = f"additional_provision_at_{custom_percent}_percent"
                if custom_col not in df.columns:
                    raise ValueError(f"Column '{custom_col}' not found for customized provision percent.")
                df.loc[df["cust_id"] == cust_id, "total_provision"] = (
                    df.loc[df["cust_id"] == cust_id, "provision"] +
                    df.loc[df["cust_id"] == cust_id, custom_col]
                )

        total = round(df["total_provision"].sum(), 2)
        return {
            "total_revised_provision": total,
            "filters": filters_dict if filters_dict else None
        }
    
    def get_revised_provision_percentage(
    self,
    date_filter: str,
    additional_provision_percent: int,
    customize_additional_provision_percent: Optional[str] = None,
    dimension_filters: Optional[str] = None
    ):
        if not hasattr(self, "df_joined") or self.df_joined is None:
            raise FileNotFoundError("Source data is not found.")

        df = self.df_joined.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)

        try:
            date_obj = pd.to_datetime(date_filter, dayfirst=True)
        except Exception:
            raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy' format.")

        df = df[df["date"] == date_obj]

        filters_dict = {}
        if dimension_filters:
            for pair in dimension_filters.split(","):
                if ':' not in pair:
                    raise ValueError(f"Invalid filter format: '{pair}'. Expected 'field:value'.")
                field, value = pair.split(":", 1)
                field = field.strip()
                value = value.strip()
                if field not in df.columns:
                    raise ValueError(f"Field '{field}' not found in dataset.")
                filters_dict[field] = value
                if pd.api.types.is_numeric_dtype(df[field]):
                    df = df[df[field] == pd.to_numeric(value, errors="coerce")]
                else:
                    df = df[df[field] == value]

        if df.empty:
            raise FileNotFoundError("No data available after applying filters.")

        customized_map = {}
        if customize_additional_provision_percent:
            try:
                customized_map = {
                    int(k.strip()): int(v.strip())
                    for k, v in (entry.split(":") for entry in customize_additional_provision_percent.split(","))
                }
            except Exception:
                raise ValueError("Invalid format for 'customize_additional_ecl_percent'. Use 'cust_id:percent' pairs like '1:2,5:10'.")

        base_col = f"additional_provision_at_{additional_provision_percent}_percent"
        if base_col not in df.columns:
            raise ValueError(f"Column '{base_col}' not found in dataset.")

        def compute_adjusted(row):
            if row["cust_id"] in customized_map:
                custom_col = f"additional_provision_at_{customized_map[row['cust_id']]}_percent"
                if custom_col not in df.columns:
                    raise ValueError(f"Column '{custom_col}' not found for customized provision percent.")
                return row["provision"] + row.get(custom_col, 0)
            return row["provision"] + row[base_col]

        df["adjusted_provision"] = df.apply(compute_adjusted, axis=1)

        total_adjusted_provision = df["adjusted_provision"].sum()
        total_exposure = df["exposure"].sum()

        revised_charge = round((total_adjusted_provision / total_exposure) * 100, 2) if total_exposure != 0 else None

        return {
            "revised_provision_percentage": revised_charge,
            "filters": filters_dict if filters_dict else None
        }
    
    def get_customer_provision_details(
    self,
    attributes: List[str],
    customer_fields: List[str],
    date_filter: str,
    additional_provision_percent: int,
    customize_additional_provision_percent: Optional[str],
    dimension_filters: Optional[str]
    ) -> List[Dict[str, Union[str, int, float]]]:

        if self.df_joined is None or self.df_joined.empty:
            raise FileNotFoundError("Source data not loaded or merged.")

        try:
            target_date = pd.to_datetime(date_filter, dayfirst=True)
        except Exception:
            raise ValueError(f"Invalid date format: '{date_filter}'. Please use 'dd/mm/yyyy'.")

        df_filtered = self.df_joined.copy()
        df_filtered["date"] = pd.to_datetime(df_filtered["date"], errors="coerce", dayfirst=True)
        df_filtered = df_filtered[df_filtered["date"] == target_date]

        filters_dict = {}
        if dimension_filters:
            for pair in dimension_filters.split(","):
                if ':' in pair:
                    field, value = pair.split(":", 1)
                    field = field.strip()
                    value = value.strip()
                    filters_dict[field] = value
                    if field not in df_filtered.columns:
                        raise ValueError(f"Field '{field}' not found in dataset.")
                    if pd.api.types.is_numeric_dtype(df_filtered[field]):
                        df_filtered = df_filtered[df_filtered[field] == pd.to_numeric(value, errors="coerce")]
                    else:
                        df_filtered = df_filtered[df_filtered[field] == value]

        all_required = list(set(customer_fields + ["cust_name"] + attributes))

        missing_cols = [col for col in all_required if col not in df_filtered.columns]
        if missing_cols:
            raise ValueError(f"Missing columns in dataset: {', '.join(missing_cols)}")

        df_result = df_filtered[all_required].copy()

        # --- Handle overrides ---
        custom_ecl = {}
        if customize_additional_provision_percent:
            try:
                for pair in customize_additional_provision_percent.split(","):
                    cust_id, percent = pair.split(":")
                    custom_ecl[str(cust_id).strip()] = int(percent)
            except Exception:
                raise ValueError("Invalid format for customize_additional_ecl_percent. Expected format: 'cust_id:percent'")

        # --- Compute provision & charges ---
        provision_revised = []
        provision_charge_revised = []
        applied_ecl_percent = []

        for _, row in df_result.iterrows():
            cust_id = str(row["cust_id"])
            base_provision = float(row["provision"])
            exposure = float(row["exposure"])
            ecl_pct = custom_ecl.get(cust_id, additional_provision_percent)

            col_name = f"additional_provision_at_{ecl_pct}_percent"
            if col_name not in df_filtered.columns:
                raise ValueError(f"Expected column '{col_name}' not found in dataset.")

            addl_provision = df_filtered.loc[df_filtered["cust_id"] == row["cust_id"], col_name].values
            addl_provision = float(addl_provision[0]) if len(addl_provision) else 0.0

            revised = base_provision + addl_provision
            charge_pct = (revised / exposure * 100) if exposure else 0.0

            provision_revised.append(round(revised, 2))
            provision_charge_revised.append(round(charge_pct, 2))
            applied_ecl_percent.append(ecl_pct)

        df_result["provision_revised"] = provision_revised
        df_result["provision_percentage_revised"] = provision_charge_revised
        df_result["additional_provision_percentage"] = applied_ecl_percent

        if "ecl" in attributes and "ecl" in df_result.columns:
            df_result.rename(columns={"ecl": "provision_percentage"}, inplace=True)

        desired_order = [
            "cust_id", "cust_name", "exposure", "provision", "provision_percentage",
            "additional_provision_percentage", "provision_revised", "provision_percentage_revised",
            "staging", "dpd", "rating", "total_collateral"
        ]

        final_columns = [col for col in desired_order if col in df_result.columns]
        df_result = df_result[final_columns]

        if "exposure" in df_result.columns:
            df_result = df_result.sort_values(by="exposure", ascending=False).reset_index(drop=True)
        
        result = df_result.to_dict(orient="records")
        if filters_dict:
            return [filters_dict] + result
        else:
            return result


def calculate_breaches(requested_date: date, page: int, size: int, customer_df, fact_df, rl_df):
    if fact_df is None or customer_df is None:
        raise HTTPException(400, "Missing required data")
    if rl_df is None:
        raise HTTPException(400, "Risk Limit data required for breach calculations")

    date_str = requested_date.strftime('%d/%m/%Y')
    logger.info(f"Calculating breaches for date: {date_str}")

    exposures = fact_df[fact_df['date'] == date_str].copy()
    if exposures.empty:
        available_dates = fact_df['date'].unique().tolist() if fact_df is not None else []
        logger.warning(f"No exposure data found for {date_str}. Available dates: {available_dates}")
        raise HTTPException(
            404,
            f"No exposures for date {date_str}. Available dates: {', '.join(available_dates) or 'none'}"
        )

    limits = rl_df[rl_df['effective_date'] == date_str].copy()
    if limits.empty:
        available_dates = rl_df['effective_date'].unique().tolist() if rl_df is not None else []
        logger.warning(f"No risk limit data found for {date_str}. Available dates: {available_dates}")
        raise HTTPException(
            404,
            f"No risk limits for date {date_str}. Available dates: {', '.join(available_dates) or 'none'}"
        )

    cust_limits = (
        limits[['internal_risk_rating', 'customer_level_limit']]
        .dropna(subset=['internal_risk_rating'])
        .drop_duplicates()
        .rename(columns={'internal_risk_rating': 'rating', 'customer_level_limit': 'exposure_limit'})
    )
    sector_limits = (
        limits[['sector', 'sector_limit']]
        .dropna(subset=['sector'])
        .drop_duplicates()
    )
    group_limits = (
        limits[['group_name', 'group_limit']]
        .dropna(subset=['group_name'])
        .drop_duplicates()
        .rename(columns={'group_name': 'group_id', 'group_limit': 'exposure_limit'})
    )

    exposures = exposures.drop(columns=['cust_name', 'group'], errors='ignore')
    exposures = exposures.merge(customer_df[['cust_id', 'cust_name', 'sector', 'group_id']], on='cust_id', how='left')

    cust = exposures.merge(cust_limits, on='rating', how='left')
    cust['excess_exposure'] = cust['exposure'] - cust['exposure_limit'].fillna(float('inf'))
    cust_breach = cust[cust['exposure'] > cust['exposure_limit']]
    start, end = (page - 1) * size, page * size
    cust_page = cust_breach.iloc[start:end]
    cust_resp = PagedResponse(
        page=page,
        page_size=size,
        total=len(cust_breach),
        total_exposure=safe_float(cust_breach['excess_exposure'].sum()),
        items=[
            CustomerItem(
                customer_name=row['cust_name'] or f"ID:{row['cust_id']}",
                exposure=safe_float(row['exposure']),
                rating=int(row['rating']) if pd.notna(row['rating']) else 0,
                hc_collateral=safe_float(row.get('total_hc_collateral', 0)),
                provision=safe_float(row['provision']),
                exposure_limit=safe_float(row['exposure_limit']),
                excess_exposure=safe_float(row['excess_exposure']),
            )
            for _, row in cust_page.iterrows()
        ]
    )

    sector_agg = cust_breach.groupby('sector').apply(
        lambda df: pd.Series({
            'exposure': df['exposure'].sum(),
            'hc_collateral': df['total_hc_collateral'].sum(),
            'provision': df['provision'].sum(),
            'avg_rating': (df['rating'] * df['exposure']).sum() / df['exposure'].sum() if df['exposure'].sum() > 0 else 0
        })
    ).reset_index()
    sector = sector_agg.merge(sector_limits, on='sector', how='left')
    sector['excess_exposure'] = sector['exposure'] - sector['sector_limit'].fillna(float('inf'))
    sector_breach = sector[sector['exposure'] > sector['sector_limit']]
    sec_resp = PagedResponse(
        page=page,
        page_size=size,
        total=len(sector_breach),
        total_exposure=safe_float(sector_breach['excess_exposure'].sum()),
        items=[
            SectorItem(
                sector=row['sector'] or "Unknown",
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

    group_agg = cust_breach.groupby('group_id').apply(
        lambda df: pd.Series({
            'exposure': df['exposure'].sum(),
            'hc_collateral': df['total_hc_collateral'].sum(),
            'provision': df['provision'].sum(),
            'avg_rating': (df['rating'] * df['exposure']).sum() / df['exposure'].sum() if df['exposure'].sum() > 0 else 0
        })
    ).reset_index()
    grp = group_agg.merge(group_limits, on='group_id', how='left')
    grp['excess_exposure'] = grp['exposure'] - grp['exposure_limit'].fillna(float('inf'))
    grp_breach = grp[grp['exposure'] > grp['exposure_limit']]
    grp_resp = PagedResponse(
        page=page,
        page_size=size,
        total=len(grp_breach),
        total_exposure=safe_float(grp_breach['excess_exposure'].sum()),
        items=[
            GroupItem(
                group_id=int(row['group_id']) if pd.notna(row['group_id']) else 0,
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

app = FastAPI(title="Unified Risk API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])



def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    openapi_schema["openapi"] = "3.0.0"
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FOLDER = os.path.join(SCRIPT_DIR, "Sample_Bank_Data")
logger.info(f"Looking for data in: {DATA_FOLDER}")
customer_df, fact_df, rl_df, rating_df,factrestr_df, written_df, collateral_df = load_data(DATA_FOLDER)
risk_model = RiskDataModel(customer_df, fact_df, rl_df, rating_df, factrestr_df,written_df,collateral_df)
from typing import Any, Optional
from fastapi import HTTPException, Query
from pydantic import BaseModel, Field
from datetime import datetime

class ErrorResponse(BaseModel):
    code: int = Field(..., description="HTTP status code")
    message: str = Field(..., description="Short, user-friendly error message")
    details: Optional[Any] = Field(None, description="Optional extra context")

    class Config:
        schema_extra = {
            "example": {
                "code": 400,
                "message": "Date must be in DD/MM/YYYY format",
                "details": None
            }
        }
@app.get(
    "/breaches",
    response_model=BreachesResponse,
    responses={
        200: {
            "model": BreachesResponse,
            "description": "Successfully retrieved breaches.",
            "content": {
                "application/json": {
                    "example": {
                        "customer_level": {
                            "page": 1,
                            "page_size": 10,
                            "total": 2,
                            "total_exposure": 120000000.0,
                            "items": [
                                {
                                    "customer_name": "SABIC",
                                    "exposure": 101610694.0,
                                    "rating": 1,
                                    "hc_collateral": 105120194.5,
                                    "provision": 215184.64,
                                    "exposure_limit": 120000000.0,
                                    "excess_exposure": -18389306.0
                                }
                            ]
                        },
                        "sector_level": {
                            "page": 1,
                            "page_size": 10,
                            "total": 1,
                            "total_exposure": 3836000000.0,
                            "items": [
                                {
                                    "sector": "Financials",
                                    "avg_rating": 2.5,
                                    "exposure": 3836000000.0,
                                    "hc_collateral": 4000000000.0,
                                    "provision": 5000000.0,
                                    "exposure_limit": 3500000000.0,
                                    "excess_exposure": 336000000.0
                                }
                            ]
                        },
                        "group_level": {
                            "page": 1,
                            "page_size": 10,
                            "total": 1,
                            "total_exposure": 540000000.0,
                            "items": [
                                {
                                    "group_id": 1,
                                    "avg_rating": 3.0,
                                    "exposure": 540000000.0,
                                    "hc_collateral": 600000000.0,
                                    "provision": 1000000.0,
                                    "exposure_limit": 500000000.0,
                                    "excess_exposure": 40000000.0
                                }
                            ]
                        }
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Bad request — invalid or missing parameters.",
            "content": {
                "application/json": {
                    "example": {
                        "code": 400,
                        "message": "Date must be DD/MM/YYYY format",
                        "details": None
                    }
                }
            }
        },
        404: {
            "model": ErrorResponse,
            "description": "No breaches found for the specified date.",
            "content": {
                "application/json": {
                    "example": {
                        "code": 404,
                        "message": "No breach data found for 05/04/2025",
                        "details": None
                    }
                }
            }
        },
        422: {
            "model": ErrorResponse,
            "description": "Validation error — check query parameters.",
            "content": {
                "application/json": {
                    "example": {
                        "code": 422,
                        "message": "Validation error: missing or invalid parameters",
                        "details": [
                            {
                                "loc": ["query", "date"],
                                "msg": "field required",
                                "type": "value_error.missing"
                            }
                        ]
                    }
                }
            }
        },
        500: {
            "model": ErrorResponse,
            "description": "Unexpected server error.",
            "content": {
                "application/json": {
                    "example": {
                        "code": 500,
                        "message": "Internal server error",
                        "details": "Database connection lost"
                    }
                }
            }
        }
    },
    summary="Get Breaches by Date",
    description="Returns paginated breaches at customer, sector, and/or group levels for a specified date."
)
def get_breaches(
    date: str = Query(..., description="Date in DD/MM/YYYY format"),
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    size: int = Query(10, ge=1, description="Number of items per page"),
    level: Optional[BreachLevel] = Query(None, description="Filter by breach level")
):
    try:
        req_date = datetime.strptime(date, "%d/%m/%Y").date()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=ErrorResponse(
                code=400,
                message="Date must be in DD/MM/YYYY format",
                details=None
            ).dict()
        )

    try:
        full = calculate_breaches(req_date, page, size, customer_df, fact_df, rl_df)

        if not any(full.values()):
            raise HTTPException(
                status_code=404,
                detail=ErrorResponse(
                    code=404,
                    message=f"No breach data found for {date}",
                    details=None
                ).dict()
            )

        if level is None:
            return BreachesResponse(
                customer_level=full["customer"],
                sector_level=full["sector"],
                group_level=full["group"]
            )

        data = full[level.value]
        return BreachesResponse(**{f"{level.value}_level": data})

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing breaches for date {date}: {e}")
        raise HTTPException(
            status_code=500,
            detail=ErrorResponse(
                code=500,
                message="Internal server error",
                details=str(e)
            ).dict()
        )

# --- Custom Field Format Validator ---
def validate_field_names(field_list: List[str], field_name: str):
    invalid = [f for f in field_list if not f.islower() or " " in f]
    if invalid:
        raise HTTPException(
            status_code=422,
            detail=[
                {
                    "loc": ["query", field_name],
                    "msg": f"Invalid field name(s): {', '.join(invalid)}. Use lowercase with underscores (e.g., 'cust_name')",
                    "type": "value_error.custom"
                }
            ]
        )

# --- Period Type Validator ---
def validate_period_type(period_type: str):
    if period_type not in ["M", "Q"]:
        raise HTTPException(
            status_code=422,
            detail=[{
                "loc": ["query", "period_type"],
                "msg": "period_type must be either 'M' (Monthly) or 'Q' (Quarterly)",
                "type": "value_error.enum"
            }]
        )
        
#------ End point: get_distinct_values function -------    
class DistinctValuesResponse(BaseModel):
    column: str = Field(..., description="The name of the column queried", example="group")
    values: List[Union[str, int, float]] = Field(..., description="List of distinct (non-null) values from the specified column", example= ["1", "2", "3","4","5"])
    class Config:
        json_schema_extra = {
            "example": {
                "column": "group",
                "values": ["1", "2", "3","4","5"]
            }
        }

class DistinctValuesErrorResponse(BaseModel):
    error: str = Field(..., description="Description of the error encountered during processing", example="Column 'xyz' not found in the dataset.")

@app.get(
    "/distinct_values",
    response_model=DistinctValuesResponse,
    responses={
        200: {"description": "Successfully retrieved distinct values."},
        400: {"model": DistinctValuesErrorResponse,"description": "Bad request — unexpected internal error.",
            "content": {
            "application/json": {
                "example": {
                    "error": "An unexpected error occurred'"
                }
            }
        }
    },

        404: {
        "model": DistinctValuesErrorResponse,
        "description": "Source data not found.",
        "content": {
            "application/json": {
                "example": {
                    "error": "Source file not found. Please ensure the dataset is loaded."
                }
            }
        }
    },
        422: {
            "description": "Validation error due to missing or incorrect parameters.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "column"],
                                "msg": "Field 'exposuree' is not found.",
                                "type": "value_error.missing"
                            },
                            {
                                "loc": ["query", "column"],
                                "msg": "Invalid field name: Cust Name. Use lowercase with underscores (e.g., 'cust_name')",
                                "type": "value_error.custom"
                            }
                        ]
                    }
                }
            }
        }
    },
    summary="Get Distinct Values from Column",
    description="Returns a list of unique, non-null values from the specified column in the dataset."
)
def get_distinct_values(
    column: str = Query(..., description="Name of the column to query for distinct values (e.g., 'staging', 'date', 'cust_name')")
):
    try:
        validate_field_names([column], "column")
        result = risk_model.get_distinct_values(column_name=column)
        return {"column": column, "values": result}

    except ValueError as ve:
       raise HTTPException(status_code=422, detail=str(ve))

    except HTTPException as http_exc:
        # Let FastAPI handle already well-formed HTTPExceptions like 422
        raise http_exc
    
    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))
    
    # Catch any other general errors 
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

#------ End point: get_sum_by_dimension -------  
class GroupedSumRecord(RootModel[List[Dict[str, Union[str, int, float]]]]):

    class Config:
        json_schema_extra = {
            "examples": [
                {"group": 1,"exposure": 6560092018},{"group": 2,"exposure": 6158477730},
                {"group": 3, "exposure": 9392741393},{"group": 4,"exposure": 12429719801},
                { "group": 5,"exposure": 4287179535}
            ]
        }

# --- Success Model: Ungrouped Sum (single-row dict with optional dimension field) ---
class UngroupedSumResponse(RootModel[Dict[str, Union[str, int, float]]]):
    class Config:
        json_schema_extra = {
            "example": {
                "sector": "Financials",
                "exposure": 46716468254
            }
        }


# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Description of the error encountered")

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Column 'group' not found in the dataset."
            }
        }
    
@app.get(
    "/sum_by_dimension",
    response_model=Union[GroupedSumRecord, UngroupedSumResponse],
    responses={
    200: {
        "description": "Returns aggregated results (grouped or total).",
        "content": {
            "application/json": {
                "examples": {
                    "Ungrouped Result": {
                        "summary": "Basic aggregation",
                        "value": [
                            {"exposure": 66090714697}
                          
                        ]
                    },
                    "Grouped result": {
                        "summary": "Aggregated result using a group field",
                        "value": [
                            {"group": 1,"exposure": 6560092018},{"group": 2,"exposure": 6158477730},
                            {"group": 3, "exposure": 9392741393},{"group": 4,"exposure": 12429719801},
                            { "group": 5,"exposure": 4287179535}
                          
                        ]
                    },
                    
                    "Grouped result with date and filter": {
                        "summary": "Aggregated result using a group field , date 31/12/2024 and filter sector: Financials",
                        "value": [
                            {"sector": "Financials"},
                            {"group": 1,"exposure": 331321632},{"group": 2,"exposure": 411806375},
                            { "group": 3,"exposure": 629415285},{"group": 4,"exposure": 553838990},
                            {"group": 5,"exposure": 310177741}
                          
                        ]
                    },
                    "Ungrouped result with filter": {
                        "summary": "Flat total with dimension filter ",
                        "value": {
                            "sector": "Financials",
                            "exposure": 46716468254
                        }
                    }
                }
            }
        }
    },
    400: {"model": ErrorResponse,"description": "Bad request — unexpected internal error.",
            "content": {
            "application/json": {
                "example": {
                    "error": "An unexpected error occurred'"
                }
            }
        }
    },
    404: {
        "model": ErrorResponse,
        "description": "Source data not found.",
        "content": {
            "application/json": {
                "example": {
                    "error": "Source file not found. Please ensure the dataset is loaded."
                }
            }
        }
    },
    422: {
        "description": "Validation error due to missing or incorrect query parameters.",
        "content": {
            "application/json": {
                "example": {
                    "detail": [
                        {
                            "loc": ["query", "fact_fields"],
                            "msg": "field required",
                            "type": "value_error.missing"
                        },
                        {
                            "loc": ["query", "fact_fields"],
                            "msg": "Fact field 'exposureee' not found",
                            "type": "value_error.custom"
                        },
                        {
                            "loc": ["query", "group_by_fields"],
                            "msg": "str type expected",
                            "type": "type_error.str"
                        },
                        {
                            "loc": ["query", "date_filter"],
                            "msg": "Invalid date format: '12/10/145'. Please use 'dd/mm/yyyy' format.",
                            "type": "value_error.date"
                        },
                        {
                            "loc": ["query", "dimension_filter_field"],
                            "msg": "str type expected",
                            "type": "type_error.str"
                        },
                        {
                            "loc": ["query", "dimension_filter_value"],
                            "msg": "Dimension Value 'finance' is not found in the filter 'sector'",
                            "type": "value_error.custom"
                        },
                        {
                            "loc": ["query", "dimension_filter_value"],
                            "msg": "Dimension filter value is missing for the given dimension filter.",
                            "type": "value_error.missing"
                        },
                        {
                            "loc": ["query", "fact_fields"],
                            "msg": "Invalid field name(s): Cust Name. Use lowercase with underscores (e.g., 'cust_name')",
                            "type": "value_error.custom"
                        },
                        {
                            "loc": ["query", "group_by_fields"],
                            "msg": "Invalid field name(s): Group ID. Use lowercase with underscores (e.g., 'group_id')",
                            "type": "value_error.custom"
                        },
                        {
                            "loc": ["query", "dimension_filter_field"],
                            "msg": "Invalid field name: Sector Name. Use lowercase with underscores (e.g., 'sector_name')",
                            "type": "value_error.custom"
                        }
                    ]
                }
            }
        }
    }
},
    summary="Get Sum by Dimension",
    description="Aggregates one numeric fact field, optionally grouped by dimensions and filtered by date or a dimension value."
)
def get_sum_by_dimension(
    fact_field: str = Query(..., description="Fact field to aggregate, e.g., 'exposure'"),
    group_by_field: str = Query(None, description="Field to group by, e.g., 'cust_id'"),
    date_filter: Optional[str] = Query(None, description="Date in dd/mm/yyyy format"),
    dimension_filter_field: Optional[str] = Query(None, description="Field name to filter the data by, e.g., 'sector'"),
    dimension_filter_value: Optional[str] = Query(None, description="Value of the dimension field to filter by, e.g., 'Financials'")
):
    try:
        validate_field_names([fact_field], "fact_field")
        if group_by_field:
            validate_field_names([group_by_field], "group_by_field")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")

        result = risk_model.get_sum_by_dimension(
            fact_field=fact_field,
            group_by_field=group_by_field,
            date_filter=date_filter,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value
        )
        return result

    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except HTTPException as http_exc:
        # Re-raise HTTP exceptions to let FastAPI handle them properly
        raise http_exc
    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")
    
# --- Success Model ---
class WrittenoffSumResponse(RootModel[Dict[str, Union[str, int, float]]]):
    class Config:
        json_schema_extra = {
            "example": {
                "court_Case": "y",
                "amount": 46716468254
            }
        }

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Description of the error encountered")

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Column 'group' not found in the dataset."
            }
        }
    
@app.get(
    "/writtenoff_sum_by_dimension",
    response_model=WrittenoffSumResponse,
    responses={
    200: {
        "description": "Returns aggregated results (grouped or total).",
        "content": {
            "application/json": {
                "examples": {
                    "Ungrouped Result": {
                        "summary": "Basic aggregation",
                        "value": [
                            {"amount": 66090714697}
                          
                        ]
                    },
                    
            
                    "Ungrouped result with filter": {
                        "summary": "Flat total with dimension filter ",
                        "value": {
                            "court_case": "y",
                            "amount": 46716468254
                        }
                    }
                }
            }
        }
    },
    400: {"model": ErrorResponse,"description": "Bad request — unexpected internal error.",
            "content": {
            "application/json": {
                "example": {
                    "error": "An unexpected error occurred'"
                }
            }
        }
    },
    404: {
        "model": ErrorResponse,
        "description": "Source data not found.",
        "content": {
            "application/json": {
                "example": {
                    "error": "Source file not found. Please ensure the dataset is loaded."
                }
            }
        }
    },
    422: {
        "description": "Validation error due to missing or incorrect query parameters.",
        "content": {
            "application/json": {
                "example": {
                    "detail": [
                        {
                            "loc": ["query", "fact_fields"],
                            "msg": "field required",
                            "type": "value_error.missing"
                        },
                        {
                            "loc": ["query", "fact_fields"],
                            "msg": "Fact field 'exposureee' not found",
                            "type": "value_error.custom"
                        },
                        {
                            "loc": ["query", "date_filter"],
                            "msg": "Invalid date format: '12/10/145'. Please use 'dd/mm/yyyy' format.",
                            "type": "value_error.date"
                        },
                        {
                            "loc": ["query", "dimension_filter_field"],
                            "msg": "str type expected",
                            "type": "type_error.str"
                        },
                        {
                            "loc": ["query", "fact_fields"],
                            "msg": "Invalid field name(s): Cust Name. Use lowercase with underscores (e.g., 'cust_name')",
                            "type": "value_error.custom"
                        },
                        {
                            "loc": ["query", "dimension_filter_field"],
                            "msg": "Invalid field name: Sector Name. Use lowercase with underscores (e.g., 'sector_name')",
                            "type": "value_error.custom"
                        }
                    ]
                }
            }
        }
    }
},
    summary="Get Sum by Dimension",
    description="Aggregates one numeric fact field, optionally  filtered by date or a dimension value."
)
def writtenoff_sum_by_dimension(
    fact_field: str = Query(..., description="Fact field to aggregate, e.g., 'exposure'"),
    date_filter: Optional[str] = Query(None, description="Date in dd/mm/yyyy format"),
    dimension_filters: Optional[str] = Query(None, description="Comma-separated filter:value pairs like sector:Financials,staging:1")):
    try:
        validate_field_names([fact_field], "fact_field")
        if dimension_filters:
            filter_fields = [pair.split(':', 1)[0] for pair in dimension_filters.split(',') if ':' in pair]
            validate_field_names(filter_fields, "dimension_filters")

        result = risk_model.writtenoff_sum_by_dimension(
            fact_field=fact_field,
            date_filter=date_filter,
            dimension_filters=dimension_filters)
        return result

    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except HTTPException as http_exc:
        # Re-raise HTTP exceptions to let FastAPI handle them properly
        raise http_exc
    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")
    

# --- Success Response Model ---
class GroupedAvgRecord(RootModel[List[Dict[str, Union[str, int, float]]]]):
    class Config:
        json_schema_extra = {
            "examples": [{"sector": "Chemicals", "exposure": 101573364,"provision": 201811},
                         {"sector": "Consumer Staples","exposure": 105583832,"provision": 10359582},
                         {"sector": "Financials","exposure": 108139973,"provision": 41866895},
                         {"sector": "Industrials","exposure": 175477898,"provision": 79002909},
                         {"sector": "Real Estate","exposure": 133492046,"provision": 96949},
                         {"sector": "Telecommunications","exposure": 111448437,"provision": 39958564},
                         {"sector": "Utilities","exposure": 98245750,"provision": 12380848}]
        }

class UngroupedAvgResponse(RootModel[Dict[str, Union[str, int, float]]]):
    class Config:
        json_schema_extra = {
            "example": {
                "group": "2","exposure": 102641296,"provision": 5222179
            }
        }

class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Column 'group' not found in the dataset."
            }
        }

@app.get(
    "/avg_by_dimension",
    response_model=Union[GroupedAvgRecord, UngroupedAvgResponse],
    responses={
        200: {
            "description": "Returns average values of specified fact fields, grouped and/or filtered if specified.",
            "content": {
                "application/json": {
                    "examples": {
                        "Grouped Average": {
                            "summary": "Grouped by sector",
                            "value": [
                                {"sector": "Chemicals", "exposure": 101573364,"provision": 201811},
                                {"sector": "Consumer Staples","exposure": 105583832,"provision": 10359582},
                                {"sector": "Financials","exposure": 108139973,"provision": 41866895},
                                {"sector": "Industrials","exposure": 175477898,"provision": 79002909},
                                {"sector": "Real Estate","exposure": 133492046,"provision": 96949},
                                {"sector": "Telecommunications","exposure": 111448437,"provision": 39958564},
                                {"sector": "Utilities","exposure": 98245750,"provision": 12380848}
                            ]
                        },
                        "Ungrouped with Filter": {
                            "summary": "Flat average with dimension filter",
                            "value": {
                                "group": "2","exposure": 102641296,"provision": 5222179
                            }
                        },
                        "Ungrouped Total Average": {
                            "summary": "Flat average with no filters",
                            "value": {
                                "exposure": 110151191,
                                "provision": 36844802
                            }
                        }
                    }
                }
            }
        },
        400: {"model": ErrorResponse,"description": "Bad request — unexpected internal error.",
            "content": {
            "application/json": {
                "example": {
                    "error": "An unexpected error occurred'"
                }
            }
        }
    },
        404: {"model": ErrorResponse,"description": "Source data not found.",
        "content": {
            "application/json": {
                "example": {
                    "error": "Source file not found. Please ensure the dataset is loaded."
                }
            }
        }
    },
        422: {
            "description": "Validation errors — missing, malformed, or incorrectly formatted input fields.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                            "loc": ["query", "fact_fields"],
                            "msg": "field required",
                            "type": "value_error.missing"
                        },
                        {
                            "loc": ["query", "fact_fields"],
                            "msg": "Fact field 'exposureee' not found",
                            "type": "value_error.custom"
                        },
                        {
                            "loc": ["query", "group_by_fields"],
                            "msg": "str type expected",
                            "type": "type_error.str"
                        },
                        {
                            "loc": ["query", "date_filter"],
                            "msg": "Invalid date format: '12/10/145'. Please use 'dd/mm/yyyy' format.",
                            "type": "value_error.date"
                        },
                        {
                            "loc": ["query", "dimension_filter_field"],
                            "msg": "str type expected",
                            "type": "type_error.str"
                        },
                        {
                            "loc": ["query", "dimension_filter_value"],
                            "msg": "Dimension Value 'finance' is not found in the filter 'sector'",
                            "type": "value_error.custom"
                        },
                        {
                            "loc": ["query", "dimension_filter_value"],
                            "msg": "Dimension filter value is missing for the given dimension filter.",
                            "type": "value_error.missing"
                        },
                        {
                            "loc": ["query", "fact_fields"],
                            "msg": "Invalid field name(s): Cust Name. Use lowercase with underscores (e.g., 'cust_name')",
                            "type": "value_error.custom"
                        },
                        {
                            "loc": ["query", "group_by_fields"],
                            "msg": "Invalid field name(s): Group ID. Use lowercase with underscores (e.g., 'group_id')",
                            "type": "value_error.custom"
                        },
                        {
                            "loc": ["query", "dimension_filter_field"],
                            "msg": "Invalid field name: Sector Name. Use lowercase with underscores (e.g., 'sector_name')",
                            "type": "value_error.custom"
                        }
                        ]
                    }
                }
            }
        }
    },
    summary="Get Average by Dimension",
    description="Computes average values of the given fact fields, optionally grouped and filtered by dimensions and date."
)
def get_avg_by_dimension(
    fact_fields: str = Query(..., description="Comma-separated list of numeric fields to average, e.g. 'exposure,provision'"),
    group_by_fields: Optional[str] = Query(None, description="Comma-separated list of fields to group by, e.g. 'sector'"),
    date_filter: Optional[str] = Query(None, description="Optional date filter in 'dd/mm/yyyy' format"),
    dimension_filter_field: Optional[str] = Query(None, description="Field to filter by (e.g., 'group')"),
    dimension_filter_value: Optional[str] = Query(None, description="Value for the filter field (e.g., '3')")
):
    try:
        fact_fields_list = [f.strip() for f in fact_fields.split(',')]
        validate_field_names(fact_fields_list, "fact_fields")

        group_by_fields_list = []
        if group_by_fields:
            group_by_fields_list = [g.strip() for g in group_by_fields.split(',')]
            validate_field_names(group_by_fields_list, "group_by_fields")

        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")

        result = risk_model.get_avg_by_dimension(
            fact_fields=fact_fields_list,
            group_by_fields=group_by_fields_list if group_by_fields else None,
            date_filter=date_filter,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value)
        return result

    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except HTTPException as http_exc:
        raise http_exc

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

# --- Success Response Model ---
class CountDistinctResponse(RootModel[Dict[str, Union[str, int]]]):
    class Config:
        json_schema_extra = {
            "example": {
                "sector": "Retail",
                "count": 42
            }
        }

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Column 'segment_name' not found in the dataset."
            }
        }

@app.get(
    "/count_distinct",
    response_model=CountDistinctResponse,
    responses={
        200: {
            "description": "Returns count of unique values in a dimension field.",
            "content": {
                "application/json": {
                    "examples": {
                        "Without Filter": {
                            "summary": "Number of distinct non-null values dimension field: cust_id",
                            "value": {
                                "count": 50
                            }
                        },
                        "With Filter": {
                            "summary": "Distinct non-null values with filter",
                            "value": {
                                "sector": "Financials",
                                "count": 36
                            }
                        },
                        "With Groupby": {
                            "summary": "Grouped distinct counts",
                            "value": {
                                "Retail": 20 ,
                                "Financials": 15,
                                "Chemicals": 1
                            }
                        }
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Unexpected internal error.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "An unexpected error occurred"
                    }
                }
            }
        },
        404: {
            "model": ErrorResponse,
            "description": "Source data not found.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "Source file not found. Please ensure the dataset is loaded."
                    }
                }
            }
        },
        422: {
            "description": "Validation error — bad field, bad type, or bad value.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "dimension"],
                                "msg": "field required",
                                "type": "value_error.missing"
                            },
                            {
                                "loc": ["query", "date_filter"],
                                "msg": "Invalid date format: '12/10/145'. Please use 'dd/mm/yyyy' format.",
                                "type": "value_error.date"
                            },
                            {
                                "loc": ["query", "dimension"],
                                "msg": "Invalid field name(s): Cust ID. Use lowercase with underscores (e.g., 'cust_id')",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "dimension_filter_field"],
                                "msg": "Invalid field name: Sector Name. Use lowercase with underscores (e.g., 'sector_name')",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "dimension_filter_value"],
                                "msg": "Dimension value 'finance' is not found in the filter 'sector'",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "dimension_filter_value"],
                                "msg": "Dimension filter value is missing for the given dimension filter.",
                                "type": "value_error.missing"
                            }
                        ]
                    }
                }
            }
        }
    },
    summary="Count Distinct Values in a Dimension",
    description="Returns the number of distinct non-null values in a specified dimension column, optionally grouped by another field and filtered by date or dimension."
)
def count_distinct_values(
    dimension: str = Query(..., description="Dimension field to count distinct values from (e.g., 'cust_id')"),
    date_filter: Optional[str] = Query(None, description="Filter by date (format: dd/mm/yyyy)"),
    compare_date: Optional[str] = Query(None, description="Comparison date in dd/mm/yyyy format"),
    dimension_filters: Optional[str] = Query(None, description="Comma-separated filter:value pairs like sector:Financials,staging:1"),
    groupby_field: Optional[str] = Query(None, description="Optional field to group by before counting (e.g., 'sector')")):

    try:
        validate_field_names([dimension], "dimension")
        if dimension_filters:
            filter_fields = [pair.split(':', 1)[0] for pair in dimension_filters.split(',') if ':' in pair]
            validate_field_names(filter_fields, "dimension_filters")
        if groupby_field:
            validate_field_names([groupby_field], "groupby_field")

        result = risk_model.count_distinct(dimension,date_filter,compare_date,dimension_filters,groupby_field)
        return result or {"count": 0}  
        
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except HTTPException as http_exc:
        raise http_exc

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")
    
# --- Success Response Model ---
class WrittenoffCountDistinctResponse(RootModel[Dict[str, Union[str, int]]]):
    class Config:
        json_schema_extra = {
            "example": {
                "count": 40
            }
        }

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Column 'segment_name' not found in the dataset."
            }
        }

@app.get(
    "/writtenoff_count_distinct",
    response_model=WrittenoffCountDistinctResponse,
    responses={
        200: {
            "description": "Returns count of unique values in a dimension field.",
            "content": {
                "application/json": {
                    "examples": {
                        "Without Filter": {
                            "summary": "Number of distinct non-null values dimension field: cust_id",
                            "value": {
                                "count": 40
                            }
                        },
                        "With Filter": {
                            "summary": "Distinct non-null values with filter",
                            "value": {
                                "count": 28,
                                "court_case": "Y"
                            }
                        }
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Unexpected internal error.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "An unexpected error occurred"
                    }
                }
            }
        },
        404: {
            "model": ErrorResponse,
            "description": "Source data not found.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "Source file not found. Please ensure the dataset is loaded."
                    }
                }
            }
        },
        422: {
            "description": "Validation error — bad field, bad type, or bad value.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "dimension"],
                                "msg": "field required",
                                "type": "value_error.missing"
                            },
                            {
                                "loc": ["query", "date_filter"],
                                "msg": "Invalid date format: '12/10/145'. Please use 'dd/mm/yyyy' format.",
                                "type": "value_error.date"
                            },
                            {
                                "loc": ["query", "dimension"],
                                "msg": "Invalid field name(s): Cust ID. Use lowercase with underscores (e.g., 'cust_id')",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "dimension_filter_field"],
                                "msg": "Invalid field name: Sector Name. Use lowercase with underscores (e.g., 'sector_name')",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "dimension_filter_value"],
                                "msg": "Dimension value 'finance' is not found in the filter 'sector'",
                                "type": "value_error.custom"
                            }
                        ]
                    }
                }
            }
        }
    },
    summary="Count Distinct Values in a Dimension",
    description="Returns the number of distinct non-null values in a specified dimension column, optionally filtered by date or dimension."
)
def writtenoff_count_distinct_values(
    dimension: str = Query(..., description="Dimension field to count distinct values from (e.g., 'cust_id')"),
    date_filter: Optional[str] = Query(None, description="Filter by date (format: dd/mm/yyyy)"),
    dimension_filters: Optional[str] = Query(None, description="Comma-separated filter:value pairs like sector:Financials,staging:1") ):
    try:
        validate_field_names([dimension], "dimension")
        if dimension_filters:
            filter_fields = [pair.split(':', 1)[0] for pair in dimension_filters.split(',') if ':' in pair]
            validate_field_names(filter_fields, "dimension_filters")
     
        result = risk_model.writtenoff_count_distinct(dimension,date_filter,dimension_filters)
        return result or {"count": 0}  
        
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except HTTPException as http_exc:
        raise http_exc

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")    


#endpoint: get_concentration-----------------
# Success Response Model ---
class ConcentrationResponse(RootModel[Dict[str, Union[str, float, int]]]):
    class Config:
        json_schema_extra = {
            "example": {
                "exposure": 54799105113,
                "concentration_percentage": "83.0%"
            }
        }

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Column 'segment name' not found in the dataset."
            }
        }

@app.get(
    "/get_concentration",
    response_model=ConcentrationResponse,
    responses={
        200: {
            "description": "Returns the concentration percentage of the top N entities over the total.",
            "content": {
                "application/json": {
                    "examples": {
                        "With Filter": {
                            "summary": "Filtered by dimension field",
                            "value": {
                                "sector": "Financials",
                                "exposure": 1200000.0,
                                "concentration_percentage": "63%"
                            }
                        },
                        "Without Filter": {
                            "summary": "No dimension filter applied",
                            "value": {
                                "exposure": 54799105113,
                                "concentration_percentage": "83.0%"
                            }
                        }
                    }
                }
            }
        },
        400: {"model": ErrorResponse,"description": "Bad request — unexpected internal error.",
            "content": {
            "application/json": {
                "example": {
                    "error": "An unexpected error occurred'"
                }
            }
        }
    },
        404: {"model": ErrorResponse,"description": "Source data not found.",
        "content": {
            "application/json": {
                "example": {
                    "error": "Source file not found. Please ensure the dataset is loaded."
                }
            }
        }
    },
        422: {
            "description": "Validation error — required fields missing or field naming incorrect.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "fact_fields"],
                                "msg": "field required",
                                "type": "value_error.missing"
                            },
                            {
                                "loc": ["query", "group_by_fields"],
                                "msg": "str type expected",
                                "type": "type_error.str"
                            },
                            {
                                "loc": ["query", "date_filter"],
                                "msg": "Invalid date format: '12/10/145'. Please use 'dd/mm/yyyy' format.",
                                "type": "value_error.date"
                            },
                            {
                                "loc": ["query", "top_n"],
                                "msg": "int type expected",
                                "type": "type_error.int"
                            },
                            {
                                "loc": ["query", "fact_fields"],
                                "msg": "Invalid field name(s): Exposure, Cust ID. Use lowercase with underscores",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "group_by_fields"],
                                "msg": "Invalid field name(s): Segment Name. Use lowercase with underscores",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "dimension_filter_field"],
                                "msg": "Invalid field name: Sector Name. Use lowercase with underscores",
                                "type": "value_error.custom"
                            },
                            {
                            "loc": ["query", "dimension_filter_value"],
                            "msg": "Dimension Value 'finance' is not found in the filter 'sector'",
                            "type": "value_error.custom"
                            },
                            {
                            "loc": ["query", "dimension_filter_value"],
                            "msg": "Dimension filter value is missing for the given dimension filter.",
                            "type": "value_error.missing"
                            }
                        ]
                    }
                }
            }
        }
    },
    summary="Get Concentration Percentage of Top N Entities",
    description="Calculates the concentration percentage of the top N groups based on a fact field, optionally filtered by date or dimension."
)
def get_concentration(
    fact_fields: str = Query(..., description="Comma-separated list of fact fields. First is numerator, second is denominator."),
    group_by_fields: Optional[str] = Query(None, description="Comma-separated list of fields to group by."),
    date_filter: Optional[str] = Query(None, description="Date in dd/mm/yyyy format."),
    top_n: int = Query(10, description="Top N groups to consider in numerator."),
    dimension_filter_field: Optional[str] = Query(None, description="Optional dimension field to filter by."),
    dimension_filter_value: Optional[str] = Query(None, description="Value of the dimension field.")
):
    try:
        fact_fields_list = [f.strip() for f in fact_fields.split(',')]
        validate_field_names(fact_fields_list, "fact_fields")

        group_by_fields_list = []
        if group_by_fields:
            group_by_fields_list = [g.strip() for g in group_by_fields.split(',')]
            validate_field_names(group_by_fields_list, "group_by_fields")

        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")

        result = risk_model.get_concentration(
            fact_fields=fact_fields_list,
            group_by_fields=group_by_fields_list if group_by_fields else None,
            date_filter=date_filter,
            top_n=top_n,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value)
        return result

    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except HTTPException as http_exc:
        raise http_exc

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

# --- Success Response Model ---
class PortfolioTrendResponse(RootModel[List[Dict[str, Union[str, int, float, None]]]]):
    class Config:
        json_schema_extra = {
            "example": [
                {
                    "period": "Jan, 2024",
                    "exposure": 1500000.0,
                    "direct_exposure": 900000.0,
                    "avg_rating_score": 3.8,
                    "total_customers": 72
                },
                {
                    "period": "Feb, 2024",
                    "exposure": 1450000.0,
                    "direct_exposure": 920000.0,
                    "avg_rating_score": 4.0,
                    "total_customers": 70
                }
            ]
        }

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Invalid date format or missing column in fact_fields"
            }
        }

@app.get(
    "/portfolio_trend",
    response_model=PortfolioTrendResponse,
    responses={
        200: {
            "description": "Returns period-wise aggregation of selected fields and customer rating summary.",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "period": "Jan, 2024",
                            "exposure": 1500000.0,
                            "avg_rating_score": 3.8,
                            "total_customers": 72
                        },
                        {
                            "period": "Feb, 2024",
                            "exposure": 1450000.0,
                            "avg_rating_score": 4.0,
                            "total_customers": 70
                        }
                    ]
                }
            }
        },
        400: {"model": ErrorResponse,"description": "Bad request — unexpected internal error.",
            "content": {
            "application/json": {
                "example": {
                    "error": "An unexpected error occurred'"
                }
            }
        }
    },
        404: {"model": ErrorResponse,"description": "Source data not found.",
        "content": {
            "application/json": {
                "example": {
                    "error": "Source file not found. Please ensure the dataset is loaded."
                }
            }
        }
    },
        422: {
            "description": "Validation error — missing or incorrectly formatted input fields.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "fact_fields"],
                                "msg": "field required",
                                "type": "value_error.missing"
                            },
                            {
                                "loc": ["query", "fact_fields"],
                                "msg": "Invalid field name(s): Exposure Limit. Use lowercase with underscores (e.g., 'exposure_limit')",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "fact_fields"],
                                "msg": "Fact field must be a valid numeric field.",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "date_filter"],
                                "msg": "Invalid date format: '12/10/145'. Please use 'dd/mm/yyyy' format",
                                "type": "value_error.date"
                            },
                            {
                                "loc": ["query", "period_type"],
                                "msg": "Unexpected value; Only 'M' (monthly) or 'Q' (quarterly) are allowed.",
                                "type": "value_error.enum"
                            },
                            {
                                "loc": ["query", "lookback"],
                                "msg": "Invalid lookback value. It must be a positive integer greater than 0",
                                "type": "type_error.integer"
                            }
                        ]
                    }
                }
            }
        }
    },
    summary="Get Portfolio Trend Summary",
    description="Aggregates trend data over a given lookback period (monthly or quarterly), including average rating and total customers."
)
def portfolio_trend(
    fact_fields: str = Query(..., description="Comma-separated fact fields to aggregate, e.g. 'exposure,direct_exposure'"),
    date_filter: str = Query(..., description="End date for the trend period in dd/mm/yyyy format"),
    period_type: Literal["M", "Q"] = Query("M", description="Period granularity: 'M' for monthly, 'Q' for quarterly"),
    lookback: Optional[int]= Query(5, description="Number of periods (months or quarters) to look back"),
    compare_date: Optional[str] = Query(None, description="Comparison date in dd/mm/yyyy format"),
    dimension_filters: Optional[str] = Query(None, description="Comma-separated filter:value pairs like sector:Financials,staging:1")):
    try:
        fact_field_list = [field.strip() for field in fact_fields.split(",") if field.strip()]
        validate_field_names(fact_field_list, "fact_fields")
        if dimension_filters:
            filter_fields = [pair.split(':', 1)[0] for pair in dimension_filters.split(',') if ':' in pair]
            validate_field_names(filter_fields, "dimension_filters")

        result = risk_model.get_portfolio_trend_summary(
            fact_fields=fact_field_list,
            date_filter=date_filter,
            period_type=period_type,
            lookback=lookback,
            compare_date=compare_date,
            dimension_filters=dimension_filters)
        return result

    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except HTTPException as http_exc:
        raise http_exc

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

# --- Success Response Model ---
class SegmentDistributionResponse(RootModel[List[Dict[str, Union[str, int]]]]):
    class Config:
        json_schema_extra = {
            "example": [
                {"segment": "Top 1–10", "exposure": 1200000, "percentage": "60.0%"},
                {"segment": "Top 11–20", "exposure": 500000, "percentage": "25.0%"},
                {"segment": "Others", "exposure": 300000, "percentage": "15.0%"}
            ]
        }

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {"error": "Column 'cust segment' not found in the dataset."}
        }

@app.get(
    "/segment_distribution",
    response_model=SegmentDistributionResponse,
    responses={
        200: {
            "description": "Returns segmented distribution of fact field across dimension groups.",
            "content": {
                "application/json": {
                    "examples": {
                        "Without Filter": {
                            "summary": "Basic segment distribution",
                            "value": [
                                {"segment": "Top 1–10", "exposure": 1200000, "percentage": "60.0%"},
                                {"segment": "Top 11–20", "exposure": 500000, "percentage": "25.0%"},
                                {"segment": "Others", "exposure": 300000, "percentage": "15.0%"}
                            ]
                        },
                        "With Dimension Filter": {
                            "summary": "With dimension filter",
                            "value": [
                                {"sector": "Financials"},
                                {"segment": "Top 1–10", "exposure": 800000, "percentage": "50.0%"},
                                {"segment": "Top 11–20", "exposure": 600000, "percentage": "37.5%"},
                                {"segment": "Others", "exposure": 200000, "percentage": "12.5%"}
                            ]
                        }
                    }
                }
            }
        },
        400: {"model": ErrorResponse,"description": "Bad request — unexpected internal error.",
            "content": {
            "application/json": {
                "example": {
                    "error": "An unexpected error occurred'"
                }
            }
        }
    },
        404: {"model": ErrorResponse,"description": "Source data not found.",
        "content": {
            "application/json": {
                "example": {
                    "error": "Source file not found. Please ensure the dataset is loaded."
                }
            }
        }
    },
        422: {
            "description": "Validation error — incorrect or malformed input parameters.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {"loc": ["query", "fact_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "dimension_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "fact_field"], "msg": "Invalid field name(s): Exposure Amt. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_field"], "msg": "Invalid field name(s): Cust ID. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "date_filter"], "msg": "Invalid date format: '12/10/145'. Please use 'dd/mm/yyyy' format", "type": "value_error.date"},
                            {"loc": ["query", "start"], "msg": "'start' must be a positive integer (>= 1). Got: 0.", "type": "type_error.integer"},
                            {"loc": ["query", "end"], "msg": "'end' must be an integer greater than 'start'. Got start=5, end=1.", "type": "type_error.integer"},
                            {"loc": ["query", "interval"], "msg": "'interval' must be <= (end - start + 1). Got: interval=11, start=1, end=10, so max allowed is 10.", "type": "type_error.integer"},
                            {"loc": ["query", "others"], "msg": "'others' must be a boolean (true/false).", "type": "type_error.bool"},
                            {"loc": ["query", "dimension_filter_field"], "msg": "Invalid field name(s): Sector Name. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_filter_value"], "msg": "Dimension Value 'finance' is not found in the filter 'sector'","type": "value_error.custom"},
                            { "loc": ["query", "dimension_filter_value"],"msg": "Dimension filter value is missing for the given dimension filter.","type": "value_error.missing"
                            }
                        ]
                    }
                }
            }
        }
    },
    summary="Get Segment Distribution",
    description="Ranks dimension values by a fact field, segments them into top intervals, and returns percentage share including an 'Others' group."
)
def segment_distribution(
    fact_field: str = Query(..., description="Fact field to aggregate (e.g., 'exposure')"),
    dimension_field: str = Query(..., description="Dimension to group by (e.g., 'cust_id')"),
    date_filter: Optional[str] = Query(None, description="Date filter (dd/mm/yyyy)"),
    start: int = Query(1, description="Start index of ranking (default: 1)"),
    end: Optional[int] = Query(20, description="End index for segmentation (optional)"),
    interval: Optional[int] = Query(10, description="Interval size for each segment group (optional)"),
    others: bool = Query(True, description="Whether to group tail values under 'Others'"),
    dimension_filter_field: Optional[str] = Query(None, description="Filter field name (e.g., 'sector')"),
    dimension_filter_value: Optional[str] = Query(None, description="Value to filter on (e.g., 'Financials')")
):
    try:
        validate_field_names([fact_field], "fact_field")
        if dimension_field:validate_field_names([dimension_field], "dimension_field")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")

        result = risk_model.get_segment_distribution(
            fact_field=fact_field,
            dimension_field=dimension_field,
            date_filter=date_filter,
            start=start,
            end=end,
            interval=interval,
            others=others,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value)

        return result
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except HTTPException as http_exc:
        raise http_exc

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

# --- Success Response Model ---
class RankedEntitiesResponse(RootModel[List[Dict[str, Union[str, int]]]]):
    class Config:
        json_schema_extra = {
            "example": [
                {"cust_name": "Alpha Ltd", "exposure": 800000, "percentage": "40.0%"},
                {"cust_name": "Beta Corp", "exposure": 600000, "percentage": "30.0%"},
                {"cust_name": "Gamma Inc", "exposure": 400000, "percentage": "20.0%"}
            ]
        }

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Column 'segment name' not found in the dataset."
            }
        }

@app.get(
    "/ranked_entities_with_others",
    response_model=RankedEntitiesResponse,
    responses={
        200: {
            "description": "Returns top N ranked entities with optional 'Others' category.",
            "content": {
                "application/json": {
                    "examples": {
                        "Ranked Entities (Customer ID)": {
                            "summary": "Top customers with name mapping",
                            "value": [
                                {"cust_name": "Alpha Ltd", "exposure": 800000, "percentage": "40.0%"},
                                {"cust_name": "Beta Corp", "exposure": 600000, "percentage": "30.0%"},
                                {"cust_name": "Gamma Inc", "exposure": 400000, "percentage": "20.0%"}
                            ]
                        },
                        "With Others": {
                            "summary": "Only Others bucket shown",
                            "value": [
                                {"segment": "Others", "exposure": 300000, "percentage": "100.0%"}
                            ]
                        },
                        "With Filter": {
                            "summary": "Filtered by sector = Financials",
                            "value": [
                                {"sector": "Retail"},
                                {"cust_name": "Retail Co A", "exposure": 500000, "percentage": "50.0%"},
                                {"cust_name": "Retail Co B", "exposure": 300000, "percentage": "30.0%"}
                            ]
                        }
                    }
                }
            }
        },
        400: {"model": ErrorResponse,"description": "Bad request — unexpected internal error.",
            "content": {
            "application/json": {
                "example": {
                    "error": "An unexpected error occurred'"
                }
            }
        }
    },
        404: {"model": ErrorResponse,"description": "Source data not found.",
        "content": {
            "application/json": {
                "example": {
                    "error": "Source file not found. Please ensure the dataset is loaded."
                }
            }
        }
    },
        422: {
            "description": "Validation error for query parameters",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {"loc": ["query", "fact_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "dimension_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "fact_field"], "msg": "Invalid field name(s): Exposure Amt. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_field"], "msg": "Invalid field name(s): Cust ID. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "date_filter"], "msg": "Invalid date format: '12/10/145'. Please use 'dd/mm/yyyy' format", "type": "value_error.date"},
                            {"loc": ["query", "start"], "msg": "'start' must be a positive integer (>= 1). Got: 0.", "type": "type_error.integer"},
                            {"loc": ["query", "end"], "msg": "'end' must be an integer greater than 'start'. Got start=5, end=1.", "type": "type_error.integer"},
                            {"loc": ["query", "interval"], "msg": "'interval' must be <= (end - start + 1). Got: interval=11, start=1, end=10, so max allowed is 10.", "type": "type_error.integer"},
                            {"loc": ["query", "others_option"], "msg": "'others' must be a boolean (true/false).", "type": "type_error.bool"},
                            {"loc": ["query", "dimension_filter_field"], "msg": "Invalid field name(s): Sector Name. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_filter_value"], "msg": "Dimension Value 'finance' is not found in the filter 'sector'","type": "value_error.custom"},
                            { "loc": ["query", "dimension_filter_value"],"msg": "Dimension filter value is missing for the given dimension filter.","type": "value_error.missing"}
                        ]
                    }
                }
            }
        }
    },
    summary="Get Ranked Entities with Others",
    description="Returns top N entities ranked by a fact field, optionally including an 'Others' group and name mapping for customers."
)
def get_ranked_entities_with_others(
    fact_field: str = Query(..., description="Numeric field to aggregate (e.g., 'exposure')"),
    dimension_field: str = Query(..., description="Dimension field to rank entities by (e.g., 'cust_id', 'sector')"),
    date_filter: Optional[str] = Query(None, description="Date in dd/mm/yyyy format"),
    start: int = Query(1, description="Start rank"),
    end: Optional[int] = Query(10, description="End rank (inclusive)"),
    others_option: bool = Query(False, description="Return only the 'Others' group"),
    dimension_filter_field: Optional[str] = Query(None, description="Optional filter field (e.g., 'sector')"),
    dimension_filter_value: Optional[str] = Query(None, description="Value for dimension filter (e.g., 'Financials')")
):
    try:
        validate_field_names([fact_field], "fact_field")
        validate_field_names([dimension_field], "dimension_field")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")

        result = risk_model.get_ranked_entities_with_others(
            fact_field=fact_field,
            dimension_field=dimension_field,
            date_filter=date_filter,
            start=start,
            end=end,
            others_option=others_option,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value)
        return result
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except HTTPException as http_exc:
        raise http_exc

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

# --- Success Response Model ---
class RankedDistributionResponse(RootModel[List[Dict[str, Union[str, int]]]]):
    class Config:
        json_schema_extra = {
            "example": [
                {"group": 1, "exposure": 500000, "percentage": "25%"},
                {"group": 2, "exposure": 300000, "percentage": "15%"},
                {"group": 3, "exposure": 200000, "percentage": "10%"}
            ]
        }

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {"error": "Column 'group_id' not found in the dataset."}
        }

@app.get(
    "/ranked_distribution_by_grouping",
    response_model=RankedDistributionResponse,
    responses={
        200: {
            "description": "Returns ranked distribution of entities grouped by a field.",
            "content": {
                "application/json": {
                    "examples": {
                        "Without Filter": {
                            "summary": "Basic ranked distribution",
                            "value": [
                                {"group": 1, "exposure": 500000, "percentage": "25%"},
                                {"group": 2, "exposure": 300000, "percentage": "15%"},
                                {"group": 3, "exposure": 200000, "percentage": "10%"}
                            ]
                        },
                        "With Dimension Filter": {
                            "summary": "Filtered by sector = Financials",
                            "value": [
                                {"sector": "Financials"},
                                {"group": 1, "exposure": 400000, "percentage": "40%"},
                                {"group": 2, "exposure": 300000, "percentage": "30%"},
                                {"group": 3, "exposure": 200000, "percentage": "20%"}
                            ]
                        }
                    }
                }
            }
        },
        400: {"model": ErrorResponse,"description": "Bad request — unexpected internal error.",
            "content": {
            "application/json": {
                "example": {
                    "error": "An unexpected error occurred'"
                }
            }
        }
    },
        404: {"model": ErrorResponse,"description": "Source data not found.",
        "content": {
            "application/json": {
                "example": {
                    "error": "Source file not found. Please ensure the dataset is loaded."
                }
            }
        }
    },
        422: {
            "description": "Validation error for query parameters",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {"loc": ["query", "fact_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "dimension_field_to_rank"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "group_by_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "fact_field"], "msg": "Invalid field name(s): Exposure Amt. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_field_to_rank"], "msg": "Invalid field name(s): Cust ID. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "group_by_field"], "msg": "Invalid field name(s): Group Name. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "date_filter"], "msg": "Invalid date format: '12/10/145'. Please use 'dd/mm/yyyy' format", "type": "value_error.date"},
                            {"loc": ["query", "start_rank"], "msg": "'start_rank' must be a positive integer (>= 1). Got: 0.", "type": "type_error.integer"},
                            {"loc": ["query", "end_rank"], "msg": "'end_rank' must be an integer greater than 'start_rank'. Got start_rank=5, end_rank=1.", "type": "type_error.integer"},
                            {"loc": ["query", "other_option"], "msg": "'others_option' must be a boolean (true/false).", "type": "type_error.bool"},
                            {"loc": ["query", "dimension_filter_field"], "msg": "Invalid field name(s): Sector Name. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_filter_value"], "msg": "Dimension Value 'finance' is not found in the filter 'sector'","type": "value_error.custom"},
                            { "loc": ["query", "dimension_filter_value"],"msg": "Dimension filter value is missing for the given dimension filter.","type": "value_error.missing"}
                        ]
                    }
                }
            }
        }
    },
    summary="Get Ranked Distribution by Grouping",
    description="Ranks entities by a fact field, groups by another field, and returns the percentage distribution."
)
def get_ranked_distribution_by_grouping(
    fact_field: str = Query(..., description="Fact field to be aggregated (e.g., 'exposure')"),
    dimension_field_to_rank: str = Query(..., description="Dimension field to rank by (e.g., 'cust_id')"),
    group_by_field: str = Query(..., description="Field to group by (e.g., 'rating')"),
    start_rank: int = Query(1, ge=1, description="Start rank for top N entities"),
    end_rank: Optional[int] = Query(None, ge=1, description="End rank for top N entities"),
    others_option: Optional[bool] = Query(False, description="Whether to group entities beyond end rank into 'Others'"),
    date_filter: Optional[str] = Query(None, description="Date filter (dd/mm/yyyy)"),
    dimension_filter_field: Optional[str] = Query(None, description="Dimension field to filter (optional)"),
    dimension_filter_value: Optional[str] = Query(None, description="Value for the dimension filter (optional)")
):
    try:
        validate_field_names([fact_field], "fact_field")
        validate_field_names([dimension_field_to_rank], "dimension_field_to_rank")
        validate_field_names([group_by_field], "group_by_field")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")

        result = risk_model.get_ranked_distribution_by_grouping(
            fact_field=fact_field,
            dimension_field_to_rank=dimension_field_to_rank,
            group_by_field=group_by_field,
            start_rank=start_rank,
            end_rank=end_rank,
            others_option=others_option,
            date_filter=date_filter,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value)

        return result
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except HTTPException as http_exc:
        raise http_exc

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")
    
# --- Success Response Model ---
class PercentDistributionResponse(RootModel[List[Dict[str, Union[str, int]]]]):
    class Config:
        json_schema_extra = {
            "example": [
                {"sector": "Chemicals","percentage": "2.0%" },{ "sector": "Consumer Staples", "percentage": "6.0%"},
                {"sector": "Financials","percentage": "71.0%"}, {"sector": "Industrials","percentage": "3.0%"},
                {"sector": "Real Estate","percentage": "5.0%"}, {"sector": "Telecommunications","percentage": "10.0%"},
                {"sector": "Utilities","percentage": "4.0%"}
            ]
        }

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {"error": "Column 'sector' not found in the dataset."}
        }

@app.get(
    "/percent_distribution_by_field",
    response_model=PercentDistributionResponse,
    responses={
        200: {
            "description": "Returns the percentage distribution of a fact field across the given dimension.",
            "content": {
                "application/json": {
                    "examples": {
                        "Without Filter": {
                            "summary": "Basic percentage distribution by sector",
                            "value": [
                                {"sector": "Chemicals","percentage": "2.0%" },{ "sector": "Consumer Staples", "percentage": "6.0%"},
                                {"sector": "Financials","percentage": "71.0%"}, {"sector": "Industrials","percentage": "3.0%"},
                                {"sector": "Real Estate","percentage": "5.0%"}, {"sector": "Telecommunications","percentage": "10.0%"},
                                {"sector": "Utilities","percentage": "4.0%"}
                            ]
                        },
                        "With Dimension Filter": {
                            "summary": "Filtered by group = 1",
                            "value": [
                                {"group": "1"},
                                {"sector": "Chemicals","percentage": "19.0%"},
                                {"sector": "Consumer Staples", "percentage": "21.0%"},
                                {"sector": "Financials","percentage": "61.0%"}
                            ]
                        }
                    }
                }
            }
        },
        400: {"model": ErrorResponse,"description": "Bad request — unexpected internal error.",
            "content": {
            "application/json": {
                "example": {
                    "error": "An unexpected error occurred'"
                }
            }
        }
    },
        404: {"model": ErrorResponse,"description": "Source data not found.",
        "content": {
            "application/json": {
                "example": {
                    "error": "Source file not found. Please ensure the dataset is loaded."
                }
            }
        }
    },
        422: {
            "description": "Validation error for query parameters",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {"loc": ["query", "fact_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "dimension_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "fact_field"], "msg": "Invalid field name(s): Exposure Amt. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_field"], "msg": "Invalid field name(s): Sector Name. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "date_filter"], "msg":  "Invalid date format: '12/10/145'. Please use 'dd/mm/yyyy' format", "type": "value_error.date"},
                            {"loc": ["query", "dimension_filter_field"], "msg": "Invalid field name(s): Sector Name. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_filter_value"], "msg": "Dimension Value 'finance' is not found in the filter 'sector'","type": "value_error.custom"},
                            { "loc": ["query", "dimension_filter_value"],"msg": "Dimension filter value is missing for the given dimension filter.","type": "value_error.missing"}
                        ]
                    }
                }
            }
        }
    },
    summary="Get Percentage Distribution by Field",
    description="Calculates the percentage distribution of a fact field across a given dimension, including optional filters."
)
def perc_distribution_by_field(
    fact_field: str = Query(..., description="Fact field to aggregate (e.g., exposure)"),
    dimension_field: str = Query(..., description="Dimension field to group by (e.g., sector)"),
    date_filter: Optional[str] = Query(None, description="Date filter in dd/mm/yyyy format (optional)"),
    dimension_filter_field: Optional[str] = Query(None, description="Field to filter the data (optional)"),
    dimension_filter_value: Optional[str] = Query(None, description="Value to filter the data (optional)")
):
    try:
        validate_field_names([fact_field], "fact_field")
        validate_field_names([dimension_field], "dimension_field")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")


        result = risk_model.get_perc_distribution_by_field(
            fact_field=fact_field,
            dimension_field=dimension_field,
            date_filter=date_filter,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value)
        return result
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except HTTPException as http_exc:
        raise http_exc

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

# --- Success Response Model ---
class PercentageTrendResponse(RootModel[List[Dict[str, Union[str, int]]]]):
    class Config:
        json_schema_extra = {
            "example": [
                {"period": "Oct 24","Chemicals": "0%","Financials": "0%","Telecommunications": "0%",
                "Utilities": "0%","Real Estate": "0%","Consumer Staples": "0%","Industrials": "0%"},
                {"period": "Nov 24","Chemicals": "0%","Financials": "0%","Telecommunications": "0%",
                "Utilities": "0%","Real Estate": "0%","Consumer Staples": "0%","Industrials": "0%"},
                {"period": "Dec 24","Chemicals": "2.0%","Financials": "71.0%","Telecommunications": "10.0%",
                 "Utilities": "4.0%","Real Estate": "5.0%","Consumer Staples": "6.0%","Industrials": "3.0%" }
            ]
        }

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {"error": "Column 'sector' not found in the dataset."}
        }

@app.get(
    "/percentage_trend_by_field",
    response_model=PercentageTrendResponse,
    responses={
        200: {
            "description": "Returns percentage trend by field for a given fact field and dimension.",
            "content": {
                "application/json": {
                    "examples": {
                        "Without Filter": {
                            "summary": "Basic percentage trend",
                            "value": [
                                {"period": "Oct 24","Chemicals": "0%","Financials": "0%","Telecommunications": "0%",
                                "Utilities": "0%","Real Estate": "0%","Consumer Staples": "0%","Industrials": "0%"},
                                {"period": "Nov 24","Chemicals": "0%","Financials": "0%","Telecommunications": "0%",
                                "Utilities": "0%","Real Estate": "0%","Consumer Staples": "0%","Industrials": "0%"},
                                {"period": "Dec 24","Chemicals": "2.0%","Financials": "71.0%","Telecommunications": "10.0%",
                                "Utilities": "4.0%","Real Estate": "5.0%","Consumer Staples": "6.0%","Industrials": "3.0%" }
                            ]
                        },
                        "With Dimension Filter": {
                            "summary": "Filtered by group = 1",
                            "value": [
                                {"group": "1"},
                                {"period": "Oct 24","Chemicals": "0%","Financials": "0%","Telecommunications": "0%",
                                "Utilities": "0%","Real Estate": "0%","Consumer Staples": "0%","Industrials": "0%"},
                                {"period": "Nov 24","Chemicals": "0%","Financials": "0%","Telecommunications": "0%",
                                "Utilities": "0%","Real Estate": "0%","Consumer Staples": "0%","Industrials": "0%"},
                                {"period": "Dec 24","Chemicals": "2.0%","Financials": "71.0%","Telecommunications": "10.0%",
                                "Utilities": "4.0%","Real Estate": "5.0%","Consumer Staples": "6.0%","Industrials": "3.0%" }
                            ]
                        }
                    }
                }
            }
        },
        400: {"model": ErrorResponse,"description": "Bad request — unexpected internal error.",
            "content": {
            "application/json": {
                "example": {
                    "error": "An unexpected error occurred'"
                }
            }
        }
    },
        404: {"model": ErrorResponse,"description": "Source data not found.",
        "content": {
            "application/json": {
                "example": {
                    "error": "Source file not found. Please ensure the dataset is loaded."
                }
            }
        }
    },
        422: {
            "description": "Validation error for query parameters",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {"loc": ["query", "fact_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "dimension_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "fact_field"], "msg": "Invalid field name(s): Exposure Amt. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_field"], "msg": "Invalid field name(s): Cust ID. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "period_type"], "msg": "period_type must be either 'M' (Monthly) or 'Q' (Quarterly)", "type": "value_error.enum"},
                            {"loc": ["query", "lookback_range"], "msg": "Invalid lookback value. It must be a positive integer greater than 0.", "type": "type_error.integer"},
                            {"loc": ["query", "date_filter"], "msg":  "Invalid date format: '12/10/145'. Please use 'dd/mm/yyyy' format", "type": "value_error.date"},
                            {"loc": ["query", "dimension_filter_field"], "msg": "Invalid field name(s): Sector Name. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_filter_value"], "msg": "Dimension Value 'finance' is not found in the filter 'sector'","type": "value_error.custom"},
                            { "loc": ["query", "dimension_filter_value"],"msg": "Dimension filter value is missing for the given dimension filter.","type": "value_error.missing"}
                        
                        ]
                    }
                }
            }
        }
    },
    summary="Get Percentage Trend by Field",
    description="Calculates percentage trends over a period range (monthly or quarterly) for a given fact field and dimension."
)
def percentage_trend_by_field(
    fact_field: str = Query(..., description="Fact field to aggregate (e.g., exposure)"),
    dimension_field: str = Query(..., description="Dimension field to group by (e.g., sector)"),
    date: str = Query(..., description="Date in dd/mm/yyyy format"),
    period_type: str = Query(..., description="Period type ('M' for monthly or 'Q' for quarterly)"),
    lookback_range: int = Query(..., description="Lookback range (number of periods to look back)"),
    dimension_filter_field: Optional[str] = Query(None, description="Field to filter by (optional)"),
    dimension_filter_value: Optional[str] = Query(None, description="Value to filter by (optional)")
):
    try:
        validate_field_names([fact_field], "fact_field")
        validate_field_names([dimension_field], "dimension_field")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")

        validate_period_type(period_type)

        result = risk_model.get_percentage_trend_by_field(
            fact_field=fact_field,
            dimension_field=dimension_field,
            date=date,
            period_type=period_type,
            lookback_range=lookback_range,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value)

        return result
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except HTTPException as http_exc:
        raise http_exc

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

# --- Success Response Model ---
class RankedDataPeriodResponse(RootModel[List[Dict[str, Any]]]):
    class Config:
        json_schema_extra = {
            "example": [{"Customer ID": 42,
                        "Customer Name": "Doha Bank",
                        "Periods": [{ "Period": "Dec 24", "exposure": 175581216, "Rank": 1,"rating": 8},
                        {"Period": "Sep 24","exposure": 175577660,"Rank": 1,"rating": 8}]},
                       
                        {"Customer ID": 24,
                        "Customer Name": "Industries Qatar",
                        "Periods": [{"Period": "Dec 24","exposure": 175470348,"Rank": 2,"rating": 10},
                                    {"Period": "Sep 24","exposure": 175468165,"Rank": 2,"rating": 11} ]} ]
        }
        

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {"error": "Dimension Value 'finance' is not found in the filter 'sector'"}
        }

@app.get(
    "/ranked_data_by_period",
    response_model=RankedDataPeriodResponse,
    responses={
        200: {
            "description": "Returns ranked data by period ",
            "content": {
                "application/json": {
                    "examples": {
                        "Without Filter": {
                            "summary": "Basic ranked data",
                            "value": [{"Customer ID": 42,
                                    "Customer Name": "Doha Bank",
                                     "Periods": [{ "Period": "Dec 24", "exposure": 175581216, "Rank": 1,"rating": 8},
                                      {"Period": "Sep 24","exposure": 175577660,"Rank": 1,"rating": 8}]},
                       
                                     {"Customer ID": 24,
                                     "Customer Name": "Industries Qatar",
                                     "Periods": [{"Period": "Dec 24","exposure": 175470348,"Rank": 2,"rating": 10},
                                     {"Period": "Sep 24","exposure": 175468165,"Rank": 2,"rating": 11} ]} ]
                        },
                        "With Dimension Filter": {
                            "summary": "Filtered by sector = Financials",
                            "value":  [{"Customer ID": 42,
                                      "Customer Name": "Doha Bank",
                                      "sector": "Financials",
                                      "Periods": [{ "Period": "Dec 24", "exposure": 175581216, "Rank": 1,"rating": 8},
                                      {"Period": "Sep 24","exposure": 175577660,"Rank": 1,"rating": 8}]},
                       
                                     {"Customer ID": 24,
                                     "Customer Name": "Industries Qatar",
                                     "sector": "Financials",
                                     "Periods": [{"Period": "Dec 24","exposure": 175470348,"Rank": 2,"rating": 10},
                                     {"Period": "Sep 24","exposure": 175468165,"Rank": 2,"rating": 11} ]} ]
                            
                        }
                    }
                }
            }
        },
        400: {"model": ErrorResponse,"description": "Bad request — unexpected internal error.",
            "content": {
            "application/json": {
                "example": {
                    "error": "An unexpected error occurred'"
                }
            }
        }
    },
        404: {"model": ErrorResponse,"description": "Source data not found.",
        "content": {
            "application/json": {
                "example": {
                    "error": "Source file not found. Please ensure the dataset is loaded."
                }
            }
        }
    },
        422: {
            "description": "Validation error for query parameters",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {"loc": ["query", "fact_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "dimension_field_to_rank"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "fact_field"], "msg": "Invalid field name(s): Exposure Amt. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_field_to_rank"], "msg": "Invalid field name(s): Cust ID. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "date_filter"], "msg": "Invalid date format: '12/10/145'. Please use 'dd/mm/yyyy' format", "type": "value_error.date"},
                            {"loc": ["query", "start_rank"], "msg": "'start_rank' must be a positive integer (>= 1). Got: 0.", "type": "type_error.integer"},
                            {"loc": ["query", "end_rank"], "msg": "'end_rank' must be an integer greater than 'start_rank'. Got start_rank=5, end_rank=1.", "type": "type_error.integer"},
                            {"loc": ["query", "dimension_filter_field"], "msg": "Invalid field name(s): Sector Name. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_filter_value"], "msg": "Dimension Value 'finance' is not found in the filter 'sector'","type": "value_error.custom"},
                            { "loc": ["query", "dimension_filter_value"],"msg": "Dimension filter value is missing for the given dimension filter.","type": "value_error.missing"}
                        ]
                    }
                }
            }
        }
    },
    summary="Get Ranked Data by period",
    description="Ranked data for a given fact field, dimension, and periods (monthly or quarterly) with optional filters.")

def ranked_data_by_period(
    fact_field: str = Query(..., description="The fact field to be aggregated."),
    dimension_field_to_rank: str = Query(..., description="The dimension field to rank by."),
    date: str = Query(..., description="End date dd/mm/yyyy."),
    start_rank: Optional[int] = Query(None, description="Minimum rank to include."),
    end_rank: Optional[int] = Query(None, description="Maximum rank to include."),
    period_type: str = Query('Q', description="'M' for monthly or 'Q' for quarterly."),
    lookback: int = Query(5, ge=1, description="Number of periods to look back."),
    dimension_field: str = Query('rating', description="Additional dimension to include."),
    dimension_filter_field: Optional[str] = Query(None, description="Field to filter on (e.g., sector)."),
    dimension_filter_value: Optional[str] = Query(None, description="Value for the filter field.")) -> List[Dict[str, Any]]:
    
    try:
        validate_field_names([fact_field], "fact_field")
        validate_field_names([dimension_field_to_rank], "dimension_field_to_rank")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")
        if dimension_field:
            validate_field_names([dimension_field], "dimension_field")

        result = risk_model.get_ranked_data_by_period(
            fact_field,dimension_field_to_rank,
            date,start_rank,end_rank,period_type,lookback,
            dimension_field,dimension_filter_field,dimension_filter_value)
        return result
    
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except HTTPException as http_exc:
        raise http_exc

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")
    
# --- Success Response Model ---
class WeightedAverageResponse(RootModel[Dict[str, Union[float, str]]]):
    class Config:
        json_schema_extra = {
            "example": {
                "sector": "Financials",
                "rating": 3.76
            }
        }

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Total weight is zero; cannot compute weighted average."
            }
        }

@app.get(
    "/weighted_average",
    response_model=WeightedAverageResponse,
    responses={
        200: {
            "description": "Successful weighted average result.",
            "content": {
                "application/json": {
                    "examples": {
                        "Without Filter": {
                            "summary": "Basic percentage trend",
                            "value": {
                               
                                "rating": 3.91
                            }
                        },
                        "With Dimension Filter": {
                            "summary": "Filtered by group = 1",
                            "value": {
                                "sector": "Financials",
                                "rating": 3.91
                            }
                        }
                    }
                }
            }
        },
        400: {"model": ErrorResponse,"description": "Bad request — unexpected internal error.",
            "content": {
            "application/json": {
                "example": {
                    "error": "An unexpected error occurred'"
                }
            }
        }
    },
        404: {"model": ErrorResponse,"description": "Source data not found.",
        "content": {
            "application/json": {
                "example": {
                    "error": "Source file not found. Please ensure the dataset is loaded."
                }
            }
        }
    },
        422: {
            "description": "Validation error for query parameters",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {"loc": ["query", "fact_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "weight_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "weight_field"], "msg": "Invalid field name(s): Exposure Amt. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "date_filter"], "msg":  "Invalid date format: '12/10/145'. Please use 'dd/mm/yyyy' format", "type": "value_error.date"},
                            {"loc": ["query", "dimension_filter_field"], "msg": "Invalid field name(s): Sector Name. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_filter_value"], "msg": "Dimension Value 'finance' is not found in the filter 'sector'","type": "value_error.custom"},
                            { "loc": ["query", "dimension_filter_value"],"msg": "Dimension filter value is missing for the given dimension filter.","type": "value_error.missing"}
                        
                        ]
                    }
                }
            }
        }
    },
    summary="Calculate Weighted Average",
    description="Computes the weighted average of a fact field using a weight field, with optional date and dimension filters."
)
def calculate_weighted_average(
    fact_field: str = Query(..., description="Field to compute weighted average (e.g., 'rating')"),
    weight_field: str = Query(..., description="Field to be used for weights (e.g., 'exposure')"),
    date_filter: Optional[str] = Query(None, description="Date in dd/mm/yyyy format (optional)"),
    dimension_filter_field: Optional[str] = Query(None, description="Optional dimension field to filter by"),
    dimension_filter_value: Optional[str] = Query(None, description="Optional value for the filter field")
): 
    try:
        validate_field_names([fact_field], "fact_field")
        validate_field_names([weight_field], "weight_field")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")

        result = risk_model.calculate_weighted_average(
            fact_field=fact_field,
            weight_field=weight_field,
            date_filter=date_filter,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value)

        return result
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except HTTPException as http_exc:
        raise http_exc

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")
    
# --- Success Response Model ---
class WeightedTrendResponse(RootModel[
    Union[ Dict[str, Dict[str, float]],
        List[Union[str, Dict[str, Dict[str, float]]]] ]]):
    class Config:
        json_schema_extra = {
            "example": {
                "Jan 2024": {"rating": 3.8, "pd": 0.002341},
                "Apr 2024": {"rating": 3.9, "pd": 0.002189}
            }
        }

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Weight field 'exposure' not found."
            }
        }

@app.get(
    "/weighted_average_trend",
    response_model=WeightedTrendResponse,
    responses={
        200: {
            "description": "Returns weighted average trend by period.",
            "content": {
                "application/json": {
                    "examples": {
                        "Without Filter": {
                            "summary": "Basic weighted trend",
                            "value": {
                                "Jan 2024": {"rating": 3.8, "pd": 0.002341},
                                "Apr 2024": {"rating": 3.9, "pd": 0.002189}
                            }
                        },
                        "With Filter": {
                            "summary": "With sector filter",
                            "value": [
                                "sector:Retail",
                                {
                                    "Jan 2024": {"rating": 4.0},
                                    "Apr 2024": {"rating": 4.1}
                                }
                            ]
                        }
                    }
                }
            }
        },
        400: {"model": ErrorResponse,"description": "Bad request — unexpected internal error.",
            "content": {
            "application/json": {
                "example": {
                    "error": "An unexpected error occurred'"
                }
            }
        }
    },
        404: {"model": ErrorResponse,"description": "Source data not found.",
        "content": {
            "application/json": {
                "example": {
                    "error": "Source file not found. Please ensure the dataset is loaded."
                }
            }
        }
    },
        422: {
            "description": "Validation error for query parameters",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {"loc": ["query", "fact_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "weight_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "weight_field"], "msg": "Invalid field name(s): Exposure Amt. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "date_filter"], "msg":  "Invalid date format: '12/10/145'. Please use 'dd/mm/yyyy' format", "type": "value_error.date"},
                            {"loc": ["query", "dimension_filter_field"], "msg": "Invalid field name(s): Sector Name. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_filter_value"], "msg": "Dimension Value 'finance' is not found in the filter 'sector'","type": "value_error.custom"},
                            { "loc": ["query", "dimension_filter_value"],"msg": "Dimension filter value is missing for the given dimension filter.","type": "value_error.missing"}
                        
                        ]
                    }
                }
            }
        }
    },
    summary="Weighted Average Trend",
    description="Computes a trend of weighted averages over monthly or quarterly periods with optional filtering."
)    
def weighted_average_trend(
    fact_field: str = Query(..., description="Comma-separated fact fields to aggrgeate (e.g., 'rating', 'pd')"),
    weight_field: str = Query(..., description="Field to calculate weights by (e.g., 'exposure')"),
    date_filter: Optional[str] = Query(None, description="Date in 'DD/MM/YYYY'"),
    lookback: int = Query(5, description="Number of months or quarters to roll back"),
    frequency: str = Query("q", description="'m' for monthly or 'q' for quarterly"),
    dimension_filter_field: Optional[str] = Query(None, description="Optional dimension field to filter by"),
    dimension_filter_value: Optional[str] = Query(None, description="Optional value for the filter field")
) -> Any:
    try:
        fact_field_list = [field.strip() for field in fact_field.split(",") if field.strip()]
        validate_field_names(fact_field_list, "fact_fields")
        validate_field_names([weight_field], "weight_field")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")
            
        result = risk_model.weighted_avg_trend(
            fact_fields=fact_field_list,
            weight_field=weight_field.lower(),
            date_filter=date_filter,
            lookback=lookback,
            frequency=frequency.lower(),
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value)
        return result
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except HTTPException as http_exc:
        raise http_exc

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

# --- Success Response Model ---
class AggregatedMetricsResponse(  RootModel[Union[
        Dict[str, Union[str, float, int]],
        List[Dict[str, Union[str, float, int]]] ]]):
    class Config:
        json_schema_extra = {
            "example": [{"rating": 1,"exposure": 549773740,"pd": 0},{"rating": 2,"exposure": 1369028386,"pd": 0.02},
                        {"rating": 3,"exposure": 1597898868,"pd": 0.05},{"rating": 4,"exposure": 1379167161,"pd": 0.11 },
                        {"rating": 5,"exposure": 2159521340,"pd": 0.21}]
        }

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {
                "error":"Unsupported aggregation type: max"
            }
        }
def validate_metrics(metrics_str: str):
    valid_aggs = {"sum", "mean", "count", "weighted_average"}
    invalid = []
    for item in metrics_str.split(","):
        parts = item.strip().split(":")
        if len(parts) != 2 or parts[1] not in valid_aggs:
            invalid.append(item)
    if invalid:
        raise HTTPException(
            status_code=422,
            detail=[{
                "loc": ["query", "metrics"],
                "msg": f"Invalid metric(s): {', '.join(invalid)}. Format should be 'field:agg' with agg in {valid_aggs}",
                "type": "value_error.custom"
            }]
        )  

@app.get(
    "/aggregated_metrics_by_field",
    response_model=AggregatedMetricsResponse,
    responses={
        200: {
            "description": "Aggregated results by group or as a flat record",
            "content": {
                "application/json": {
                    "examples": {
                        "Without Filter": {
                            "summary": "Basic percentage trend",
                            "value": {
                                "exposure": 66090714697,
                                "pd": 5.68
                            }
                        },
                        "With Dimension Filter": {
                            "summary": "Filtered by group = 1",
                             "value": [{"rating": 1,"exposure": 549773740,"pd": 0},{"rating": 2,"exposure": 1369028386,"pd": 0.02},
                        {"rating": 3,"exposure": 1597898868,"pd": 0.05},{"rating": 4,"exposure": 1379167161,"pd": 0.11 },
                        {"rating": 5,"exposure": 2159521340,"pd": 0.21}]
                        }
                    }
                }
            }
        },
        400: {"model": ErrorResponse,"description": "Bad request — unexpected internal error.",
            "content": {
            "application/json": {
                "example": {
                    "error": "An unexpected error occurred'"
                }
            }
        }
    },
        404: {"model": ErrorResponse,"description": "Source data not found.",
        "content": {
            "application/json": {
                "example": {
                    "error": "Source file not found. Please ensure the dataset is loaded."
                }
            }
        }
    },
        422: {
            "description": "Validation error for query parameters",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {"loc": ["query", "matrics"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "group_by_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "group_by_field"], "msg": "Invalid field name(s): Exposure Amt. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "date_filter"], "msg":  "Invalid date '12/10/145'. Use 'DD/MM/YYYY'", "type": "value_error.date"},
                            {"loc": ["query", "dimension_filter_field"], "msg": "Invalid field name(s): Sector Name. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_filter_value"], "msg": "Dimension Value 'finance' is not found in the filter 'sector'","type": "value_error.custom"},
                            { "loc": ["query", "dimension_filter_value"],"msg": "Dimension filter value is missing for the given dimension filter.","type": "value_error.missing"}
                        
                        ]
                    }
                }
            }
        }
    },
    summary="Get Aggregated Metrics by Field",
    description="Aggregates one or more metrics grouped optionally by a field, with support for filters and weighted averages."
)

def aggregated_metrics_by_field(
    metrics: str = Query(..., description="Comma-separated metrics with aggregation types, e.g. 'exposure:sum,provision:weighted_average'"),
    group_by_field: Optional[str] = Query(None, description="Field to group the results by, e.g. 'rating'"),
    date_filter: Optional[str] = Query(None, description="Date filter in 'dd/mm/yyyy' format for example 31/12/2024"),
    dimension_filters: Optional[str] = Query(None, description="Comma-separated filter:value pairs like sector:Financials,staging:1"),
    top_n: Optional[int] = Query(10, description="Maximum number of top items to return."),
    day_flag: Optional[int] = Query(None, description="Number of days must be one of [90, 180, 360] "),
    additional_field: Optional[str] = Query(None, description="Field to include as-is in results without aggregation Eg.pd"),
    ):
    try:
        validate_metrics(metrics)
        for f in metrics.split(","):
            validate_field_names([f.split(":")[0]], "metrics")
        if group_by_field:
            validate_field_names([group_by_field], "group_by_field")
        if dimension_filters:
            filter_fields = [pair.split(':', 1)[0] for pair in dimension_filters.split(',') if ':' in pair]
            validate_field_names(filter_fields, "dimension_filters")
        if additional_field:
            validate_field_names([additional_field], "additional_field")
            
        result = risk_model.get_aggregated_metrics_by_field(
            metrics=metrics,
            group_by_field=group_by_field,
            date_filter=date_filter,
            dimension_filters=dimension_filters,
            top_n=top_n,
            day_flag=day_flag,
            additional_field=additional_field)
        return result
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except HTTPException as http_exc:
        raise http_exc

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

# --- Success Response Model ---
class DistributionItem(BaseModel):
    collateral_type: str = Field(..., description="Group/category of the collateral")
    total: float = Field(..., description="Aggregated total value for the group")
    percentage: float = Field(..., description="Share percentage of the total")

class DynamicDistributionResponse(BaseModel):
    data: List[DistributionItem]

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    code: int = Field(..., description="HTTP status code returned by the API")
    message: str = Field(..., description="Explanation of what went wrong")
    details: Optional[str] = Field(None, description="Detailed context or debugging info (if available)")

@app.get(
    "/dynamic-distribution",
    response_model=DynamicDistributionResponse,
    responses={
        200: {
            "description": "Distribution retrieved successfully.",
            "content": {
                "application/json": {
                    "example": {
                        "data": [
                            {
                                "collateral_type": "Collateral Land & Building",
                                "total": 1011295337,
                                "percentage": 20.43
                            },
                            {
                                "collateral_type": "Collateral Cash, Gold & Other Riskfree Assets",
                                "total": 832945157,
                                "percentage": 16.82
                            },
                            {
                                "collateral_type": "Collateral Shares & Other Paper Assets",
                                "total": 1114673154,
                                "percentage": 22.51
                            },
                            {
                                "collateral_type": "Others",
                                "total": 1802960480,
                                "percentage": 36.42
                            }
                        ]
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Bad Request – Unexpected error.",
            "content": {
                "application/json": {
                    "example": {
                        "code": 400,
                        "message": "Unexpected error while processing the request",
                        "details": "AttributeError: 'NoneType' object has no attribute 'columns'"
                    }
                }
            }
        },
        404: {
            "model": ErrorResponse,
            "description": "Not Found – Required source data missing",
            "content": {
                "application/json": {
                    "example": {
                        "code": 404,
                        "message": "Source data not found",
                        "details": "df_joined is missing or None"
                    }
                }
            }
        },
        422: {
            "model": ErrorResponse,
            "description": "Unprocessable Entity – Invalid input format or field",
            "content": {
                "application/json": {
                    "example": {
                        "code": 422,
                        "message": "Invalid date format. Use DD/MM/YYYY.",
                        "details": "ValueError: time data '31-05-2025' does not match format"
                    }
                }
            }
        },
        500: {
            "model": ErrorResponse,
            "description": "Internal Server Error – unexpected crash",
            "content": {
                "application/json": {
                    "example": {
                        "code": 500,
                        "message": "Internal server error",
                        "details": "Unexpected error during distribution generation"
                    }
                }
            }
        }
    },
    summary="Get Dynamic Collateral Distribution",
    description="Returns grouped distribution of a fact field (like exposure or collateral) by a categorical dimension, with optional filtering and haircut adjustment."
)
def get_dynamic_distribution_api(
    fact_field: str = Query("total_collateral", description="Numeric field to aggregate ('total_collateral')"),
    group_by_field: str = Query("collateral_type", description="Categorical field to group by"),
    date_filter: Optional[str] = Query(None, description="Date filter in DD/MM/YYYY"),
    dimension_filter_field: Optional[str] = Query(None, description="Filter field (e.g., 'group_id')"),
    dimension_filter_value: Optional[str] = Query(None, description="Filter value"),
    apply_haircut: bool = Query(False, description="Apply haircut adjustment")
):
    try:
        result = risk_model.get_dynamic_distribution(
            fact_field=fact_field,
            group_by_field=group_by_field,
            date_filter=date_filter,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value,
            apply_haircut=apply_haircut,
            source="risk"
        )

        if "error" in result:
            raise HTTPException(
                status_code=404,
                detail=ErrorResponse(code=404, message=result["error"], details=None).dict()
            )

        return JSONResponse(status_code=status.HTTP_200_OK, content=result)

    except ValueError as ve:
        raise HTTPException(
            status_code=422,
            detail=ErrorResponse(
                code=422,
                message="Invalid input format or field",
                details=str(ve)
            ).dict()
        )

    except AttributeError as ae:
        raise HTTPException(
            status_code=400,
            detail=ErrorResponse(
                code=400,
                message="Unexpected error while processing the request",
                details=str(ae)
            ).dict()
        )

    except Exception as e:
        logger.error(f"Unexpected error in /dynamic-distribution: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=ErrorResponse(
                code=500,
                message="Internal server error",
                details=str(e)
            ).dict()
        )

from fastapi.responses import JSONResponse
from fastapi import status
# --- Success Response Model ---
class SummaryTableResponse(BaseModel):
    total_exposure: float = Field(..., description="Sum of all exposures across selected customers")
    hc_collateral: float = Field(..., description="Total haircut-adjusted collateral across selected customers")
    coverage_ratio: float = Field(..., description="Overall coverage ratio: hc_collateral / exposure * 100")
    top_customers: List[Dict[str, Union[int, str, float]]] = Field(
        ..., description="List of top customers with exposure and collateral details"
    )

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    code: int = Field(..., description="HTTP status code returned by the API")
    message: str = Field(..., description="Explanation of what went wrong")
    details: Optional[str] = Field(None, description="Detailed context or debugging info (if available)")

@app.get(
    "/summary_table",
    responses={
        200: {
            "description": "Summary data retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "total_exposure": 546705241,
                        "hc_collateral": 340842650.5,
                        "coverage_ratio": 62.34,
                        "top_customers": [
                            {
                                "cust_id": 21,
                                "cust_name": "Almarai",
                                "exposure": 113776220,
                                "total_collateral": 225850539,
                                "total_hc_collateral": 107321812.5,
                                "coverage_ratio": 94.33
                            },
                            {
                                "cust_id": 46,
                                "cust_name": "Investcorp",
                                "exposure": 131012615,
                                "total_collateral": 130840283,
                                "total_hc_collateral": 122213491,
                                "coverage_ratio": 93.28
                            },
                            {
                                "cust_id": 1,
                                "cust_name": " SABIC",
                                "exposure": 101603793,
                                "total_collateral": 101713643,
                                "total_hc_collateral": 105119490.5,
                                "coverage_ratio": 103.46
                            }
                        ]
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Bad Request — Invalid filters or empty data",
            "content": {
                "application/json": {
                    "example": {
                        "code": 400,
                        "message": "Column 'sector' not found.",
                        "details": None
                    }
                }
            }
        },
        422: {
            "model": ErrorResponse,
            "description": "Unprocessable Entity — Invalid date format",
            "content": {
                "application/json": {
                    "example": {
                        "code": 422,
                        "message": "Invalid date format. Use DD/MM/YYYY.",
                        "details": None
                    }
                }
            }
        },
        500: {
            "model": ErrorResponse,
            "description": "Internal Server Error — unexpected crash",
            "content": {
                "application/json": {
                    "example": {
                        "code": 500,
                        "message": "Internal server error",
                        "details": "Unexpected error occurred during summary generation"
                    }
                }
            }
        }
    },
    summary="Get Risk Summary Table",
    description="Returns summary of exposure, collateral, and coverage ratio for top customers filtered by date and optional dimensions."
)
def summary_table(
    date: str = Query(..., description="Date in DD/MM/YYYY format"),
    top_n: int = Query(10, ge=1, description="Top N customers to return"),
    filter_field: Optional[str] = Query(None, description="e.g. 'sector' or 'group_id'"),
    filter_value: Optional[str] = Query(None, description="Value to filter by")
):
    try:
        result = risk_model.get_summary_table(
            date_filter=date,
            top_n=top_n,
            filter_field=filter_field,
            filter_value=filter_value
        )

        if isinstance(result, dict) and "error" in result:
            raise HTTPException(
                status_code=400,
                detail=ErrorResponse(code=400, message=result["error"], details=None).dict()
            )

        return JSONResponse(status_code=status.HTTP_200_OK, content=result)

    except HTTPException as http_exc:
        raise http_exc

    except ValueError as ve:
        raise HTTPException(
            status_code=422,
            detail=ErrorResponse(code=422, message="Invalid date format. Use DD/MM/YYYY.", details=str(ve)).dict()
        )

    except Exception as e:
        logger.error(f"Unexpected error in /summary_table: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=ErrorResponse(
                code=500,
                message="Internal server error",
                details=str(e)
            ).dict()
        )

# --- Success Response Model ---
class CoverageTrendResponse(RootModel[List[Dict[str, Union[str, float]]]]):
    class Config:
        json_schema_extra = {
            "example": [
                {
                    "date": "Sep 2023",
                    "exposure": 5507873430,
                    "provision": 1842643865.2,
                    "coverage_ratio": 33.45
                },
                {
                    "date": "Dec 2024",
                    "exposure": 5507708060,
                    "total_hc_collateral": 2568069927.5,
                    "coverage_ratio": 46.63
                }
            ]
        }

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {"error": "No data after applying filters."}
        }

@app.get(
    "/coverage_trend",
    response_model=CoverageTrendResponse,
    responses={
        200: {
            "description": "Returns exposure, provision/hc_collateral, and coverage ratio trend by period",
            "content": {
                "application/json": {
                    "example": [
                        {"date": "Sep 2023", "exposure": 5507873430, "provision": 1842643865.2, "coverage_ratio": 33.45},
                        {"date": "Dec 2024", "exposure": 5507708060, "total_hc_collateral": 2568069927.5, "coverage_ratio": 46.63}
                    ]
                }
            }
        },
        400: {"model": ErrorResponse, "description": "Invalid input or internal error"},
        422: {
            "description": "Validation error for query parameters",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {"loc": ["query", "date"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "fact_fields"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "period_type"], "msg": "must be 'M' or 'Q'", "type": "value_error.custom"}
                        ]
                    }
                }
            }
        }
    },
    summary="Get Exposure & Coverage Trend",
    description="Returns last 6 months or quarters of exposure and provision/hc_collateral trend, with coverage ratio."
)
def get_coverage_trend(
    date: str = Query(..., description="End date (DD/MM/YYYY)"),
    period_type: str = Query(..., description="M = Monthly, Q = Quarterly"),
    fact_fields: str = Query(..., description="Comma-separated fields (must include 'exposure' and fields like 'provision' or 'total_hc_collateral')"),
    filter_field: Optional[str] = Query(None),
    filter_value: Optional[str] = Query(None)
):
    try:
        if not re.match(r"^\d{2}/\d{2}/\d{4}$", date):
            raise HTTPException(
                status_code=422,
                detail=[{"loc": ["query", "date"], "msg": "invalid date format, expected 'dd/mm/yyyy'", "type": "value_error.date"}]
            )

        result = risk_model.trend_by_period(
            end_date=date,
            period_type=period_type.upper(),
            fact_fields_str=fact_fields,
            filter_field=filter_field,
            filter_value=filter_value
        )

        if isinstance(result, dict) and "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
    status_code=400,
    detail=ErrorResponse(code=400, message="No data found", details=result["error"]).dict()
)

# --- Success Response Model ---
class ExposureCoverageByRatingResponse(RootModel[List[Dict[str, Union[int, float]]]]):
    class Config:
        json_schema_extra = {
            "example": [
                {
                    "rating": 2,
                    "exposure": 131022299,
                    "total_hc_collateral": 122195839,
                    "total_hc_collateral_coverage_ratio": 93.26
                },
                {
                    "rating": 4,
                    "exposure": 46920280,
                    "total_hc_collateral": 181497265.5,
                    "total_hc_collateral_coverage_ratio": 386.82
                }
            ]
        }
        
# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {"error": "No data after applying filters."}
        }

@app.get(
    "/exposure_coverage_by_rating",
    summary="Exposure & Coverage Ratio by Rating",
    description="Aggregates exposure and other fact fields by rating and computes coverage ratio for each non-exposure field.",
    responses={
        200: {
            "description": "Coverage data retrieved successfully",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "rating": 2,
                            "exposure": 131022299,
                            "total_hc_collateral": 122195839,
                            "total_hc_collateral_coverage_ratio": 93.26
                        },
                        {
                            "rating": 4,
                            "exposure": 46920280,
                            "total_hc_collateral": 181497265.5,
                            "total_hc_collateral_coverage_ratio": 386.82
                        }
                    ]
                }
            }
        },
        400: {
            "description": "Bad Request – Missing or invalid parameters",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "The first fact field must be 'exposure'"
                    }
                }
            }
        },
        404: {
            "description": "Not Found – No data available",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "No data found"
                    }
                }
            }
        },
        422: {
            "description": "Unprocessable Entity – Invalid format or field name",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Invalid date format: time data '2025-31-03' does not match format"
                    }
                }
            }
        },
        500: {
            "description": "Internal Server Error – Unexpected exception",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Internal server error: 'NoneType' object is not iterable"
                    }
                }
            }
        }
    }
)
def exposure_coverage_by_rating(
    fact_fields: str = Query(..., description="Comma-separated fields (first must be 'exposure')"),
    date_filter: str = Query(..., description="Date in DD/MM/YYYY format"),
    dimension_filters: Optional[str] = Query(None, description="Comma-separated filter:value pairs like sector:Financials,staging:1")
):
    try:
        # Validate and convert fact_fields to metrics format expected by model
        fields = [f.strip() for f in fact_fields.split(",")]
        if not fields or fields[0] != "exposure":
            raise HTTPException(status_code=400, detail="The first fact field must be 'exposure'")

        # Create the metrics string in the format expected by model function
        # 'exposure:sum,total_hc_collateral:sum' etc.
        metrics = ",".join([f"{field}:mean" if field == "pd" else f"{field}:sum" for field in fields])

        # Call the actual model function using mapped arguments
        result = risk_model.get_aggregated_metrics_by_field(
            metrics=metrics,
            group_by_field="rating",
            date_filter=date_filter,
            dimension_filters=dimension_filters,
        )

        # If no result
        if not result:
            raise HTTPException(status_code=404, detail="No data found")

        # Add coverage ratio calculations
        for row in result:
            exposure = row.get("exposure_sum", 0)
            if exposure:
                for field in fields[1:]:
                    field_sum_key = f"{field}_sum"
                    field_val = row.get(field_sum_key, 0)
                    if isinstance(field_val, (int, float)):
                        row[f"{field}_coverage_ratio"] = round(field_val / exposure * 100, 2)

        return JSONResponse(status_code=status.HTTP_200_OK, content=result)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

# --------- Response Models ---------
class CustomerDetailsResponse(BaseModel):
    base_period: str
    comparison_period: Optional[str] = None
    other_periods: Optional[List[str]] = None
    filters: Optional[Dict[str, Union[str, int, float]]] = None 
    #customers: List[Dict[str, Union[str, int, float]]]
    customers: List[Dict[str, Optional[Union[str, int, float,date]]]]

class ErrorResponse(BaseModel):
    error: str

@app.get(
    "/customer_details",
    response_model=CustomerDetailsResponse,
    responses={
        200: {
            "description": "Returns customer details for the given periods and attributes.",
            "content": {
                "application/json": {
                    "examples": {
                        "Base Only": {
                            "summary": "Base period only",
                            "value": {
                                "base_period": "Jan-2024",
                                "comparison_period": "null",
                                "customers": [
                                    {"cust_id": "1", "cust_name": "Acme", "exposure": 10000},
                                    {"cust_id": "2", "cust_name": "Beta", "exposure": 15000}
                                ]
                            }
                        },
                        "With Comparison": {
                            "summary": "Base and comparison periods",
                            "value": {
                                "base_period": "Jan-2024",
                                "comparison_period": "Dec-2023",
                                "customers": [
                                    {"cust_id": "1", "cust_name": "Acme", "exposure_Jan-2024": 10000, "exposure_Dec-2023": 9000},
                                    {"cust_id": "2", "cust_name": "Beta", "exposure_Jan-2024": 15000, "exposure_Dec-2023": 14000}
                                ]
                            }
                        }
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Bad request — unexpected internal error.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "An unexpected error occurred: 'NoneType' object has no attribute 'copy'"
                    }
                }
            }
        },
        404: {
            "model": ErrorResponse,
            "description": "Source data or result not found.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "No data found after applying filters."
                    }
                }
            }
        },
        422: {
            "description": "Validation errors — incorrect inputs or field naming issues.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "attributes"],
                                "msg": "Invalid field name(s): Exposure. Use lowercase with underscores (e.g., 'exposure').",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "customer_fields"],
                                "msg": "Invalid field name(s): Cust ID. Use lowercase with underscores (e.g., 'cust_id').",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "base_date"],
                                "msg": "Invalid date format: '31/02/2024'. Please use 'dd/mm/yyyy'.",
                                "type": "value_error.date"
                            },
                            {
                                "loc": ["query", "comparison_date"],
                                "msg": "Invalid date format: '31/02/2024'. Please use 'dd/mm/yyyy'.",
                                "type": "value_error.date"
                            },
                            {
                                "loc": ["query", "dimension_filters"],
                                "msg": "Invalid dimension filter format: 'sectorRetail'. Expected 'field:value'.",
                                "type": "value_error.custom"
                            }
                        ]
                    }
                }
            }
        }
    },
    summary="Get Customer Details",
    description="Fetches customer attributes and metrics for the base period, with optional comparison and filters."
)
def customer_details(
    attributes: str = Query(..., description="Comma-separated fields to include, e.g., exposure,provision,rating"),
    customer_fields: str = Query(..., description="Comma-separated identity fields, e.g., cust_id,cust_name"),
    base_date: str = Query(..., description="Base date in dd/mm/yyyy format"),
    comparison_date: Optional[str] = Query(None, description="Comparison date in dd/mm/yyyy format"),
    top_n: Optional[int] = Query(None, description="Maximum number of top items to return."),
    period_type: Literal["M", "Q"] = Query("M", description="Period granularity: 'M' for monthly, 'Q' for quarterly"),
    lookback: Optional[int]= Query(None, description="Number of periods (months or quarters) to look back"),
    day_flag: Optional[int] = Query(None, description="Number of days must be one of [90, 180, 360] "),
    dimension_filters: Optional[str] = Query(None, description="Comma-separated filter:value pairs like sector:Retail,rating:1")
):
    try:
        # Validate field naming
        attribute_list = [attr.strip() for attr in attributes.split(',')]
        customer_field_list = [col.strip() for col in customer_fields.split(',')]
        validate_field_names(attribute_list, "attributes")
        validate_field_names(customer_field_list, "customer_fields")
        if dimension_filters:
            filter_fields = [pair.split(':', 1)[0] for pair in dimension_filters.split(',') if ':' in pair]
            validate_field_names(filter_fields, "dimension_filters")

        result = risk_model.get_customer_details(
            attributes=attributes,
            customer_fields=customer_fields,
            base_date=base_date,
            comparison_date=comparison_date,
            top_n=top_n,
            period_type=period_type,
            lookback=lookback,
            day_flag=day_flag,
            dimension_filters=dimension_filters)
        return result

    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except HTTPException as http_exc:
        raise http_exc

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")
    

# --------- Response Models ---------
class WrittenoffCustomerDetailsResponse(BaseModel):
    base_period: str
    filters: Optional[Dict[str, Union[str, int, float]]] = None 
    customers: List[Dict[str, Optional[Union[str, int, float,date]]]]

class ErrorResponse(BaseModel):
    error: str

@app.get(
    "/writtenoff_customer_details",
    response_model= WrittenoffCustomerDetailsResponse,
    responses={
        200: {
            "description": "Returns customer details for the given periods and attributes.",
            "content": {
                "application/json": {
                    "examples": {
                        "Base Only": {
                            "summary": "Base period only",
                            "value": {
                                "base_period": "Jan-2024",
                                "customers": [
                                    {"cust_id": "1", "cust_name": "Acme", "exposure": 10000},
                                    {"cust_id": "2", "cust_name": "Beta", "exposure": 15000}
                                ]
                            }
                        }
                       
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Bad request — unexpected internal error.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "An unexpected error occurred: 'NoneType' object has no attribute 'copy'"
                    }
                }
            }
        },
        404: {
            "model": ErrorResponse,
            "description": "Source data or result not found.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "No data found after applying filters."
                    }
                }
            }
        },
        422: {
            "description": "Validation errors — incorrect inputs or field naming issues.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "attributes"],
                                "msg": "Invalid field name(s): Exposure. Use lowercase with underscores (e.g., 'exposure').",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "customer_fields"],
                                "msg": "Invalid field name(s): Cust ID. Use lowercase with underscores (e.g., 'cust_id').",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "base_date"],
                                "msg": "Invalid date format: '31/02/2024'. Please use 'dd/mm/yyyy'.",
                                "type": "value_error.date"
                            },
                            {
                                "loc": ["query", "dimension_filters"],
                                "msg": "Invalid dimension filter format: 'sectorRetail'. Expected 'field:value'.",
                                "type": "value_error.custom"
                            }
                        ]
                    }
                }
            }
        }
    },
    summary="Get Customer Details",
    description="Fetches customer attributes and metrics for the base period, with optional filters."
)
def writtenoff_customer_details(
    attributes: str = Query(..., description="Comma-separated fields to include, e.g., exposure,provision,rating"),
    customer_fields: str = Query(..., description="Comma-separated identity fields, e.g., cust_id,cust_name"),
    base_date: str = Query(..., description="Base date in dd/mm/yyyy format"),
    dimension_filters: Optional[str] = Query(None, description="Comma-separated filter:value pairs like sector:Retail,rating:1")
):
    try:
        # Validate field naming
        attribute_list = [attr.strip() for attr in attributes.split(',')]
        customer_field_list = [col.strip() for col in customer_fields.split(',')]
        validate_field_names(attribute_list, "attributes")
        validate_field_names(customer_field_list, "customer_fields")
        if dimension_filters:
            filter_fields = [pair.split(':', 1)[0] for pair in dimension_filters.split(',') if ':' in pair]
            validate_field_names(filter_fields, "dimension_filters")

        result = risk_model.writtenoff_customer_details(
            attributes=attributes,
            customer_fields=customer_fields,
            base_date=base_date,
            dimension_filters=dimension_filters)
        return result

    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except HTTPException as http_exc:
        raise http_exc

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")
    
    
#--end point : provision distribution by provision type---------
class ProvisionDistributionTypeResponse(RootModel[Dict[str, Union[float, int]]]):
    class Config:
        json_schema_extra = {
            "example": {"Direct": 12331655640.048,
                "Indirect": 9775225757.36928
                }
            
        }

# --- Error Response Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {"error": "Column 'sector' not found in the dataset."}
        }

@app.get(
    "/provision_distribution_type",
    response_model=ProvisionDistributionTypeResponse,
    responses={
        200: {
            "description": "Returns the percentage distribution of a fact field across the given dimension.",
            "content": {
                "application/json": {
                    "examples": {
                        "Without Filter": {
                            "summary": "provision distribution by type ",
                            "value":  {"Direct": 12331655640.048,
                                        "Indirect": 9775225757.36928}
                        },
                        "With Dimension Filter": {
                            "summary": "provision distribution by type using date filter ",
                            "value":  {"Direct": 65760842.99831998,
                                       "Indirect": 20483415.34504}
                        }
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Invalid inputs or internal error"
        },
        422: {
            "description": "Validation error for query parameters",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {"loc": ["query", "stage_filter"], "msg": "Invalid staging value: '3cc'. Available values are: 3, 2, 3D, 3E, 3C, 1"},
                            {"loc": ["query", "date_filter"], "msg": "invalid date format, expected 'dd/mm/yyyy'", "type": "value_error.date"},
                            
                        ]
                    }
                }
            }
        }
    },
    summary="Get Provision Distribution by provision Type ",
    description="Calculates the provision distribution by provision type using  optional stage value and date filter."
)
def get_provision_distr_type(
    dimension_filters: Optional[str] = Query(None, description="Comma-separated filters like staging:3C,group:1"),
    #stage_filter: Optional[str] = Query(None, description="Staging value Eg.1,2,3,3C etc.(optional) "),
    date_filter: Optional[str] = Query(None, description="Date in dd/mm/yyyy format (optional)")):
    try:
        if dimension_filters:
            filter_fields = [pair.split(':', 1)[0] for pair in dimension_filters.split(',') if ':' in pair]
            validate_field_names(filter_fields, "dimension_filters")

        result = risk_model.get_provision_distr_type(dimension_filters=dimension_filters,date_filter=date_filter)
        return result
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except HTTPException as http_exc:
        raise http_exc

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

# --------- Response Models ---------
class TransitionMatrixResponse(BaseModel):
    base_period: str
    comparison_period: Optional[str] = None
    headers: List[str]
    rows: List[str]
    values: List[List[Union[int, float]]]

class ErrorResponse(BaseModel):
    error: str

@app.get(
    "/transition_matrix",
    response_model=TransitionMatrixResponse,
    responses={
        200: {
            "description": "Returns the computed transition matrix between two periods.",
            "content": {
                "application/json": {
                    "examples": {
                        "Absolute Counts": {
                            "summary": "Absolute transition counts",
                            "value": {
                                "base_period": "Jan-2024",
                                "comparison_period": "Dec-2023",
                                "headers": ["1", "2", "Unrated", "Closed", "Total"],
                                "rows": ["1", "2", "Unrated", "New", "Total"],
                                "values": [
                                    [10, 2, 1, 0, 13],
                                    [0, 15, 0, 1, 16],
                                    [1, 0, 20, 0, 21],
                                    [0, 1, 0, 5, 6],
                                    [11, 18, 21, 6, 56]
                                ]
                            }
                        },
                        "Percentage Matrix": {
                            "summary": "Percentage-based transition matrix",
                            "value": {
                                "base_period": "Jan-2024",
                                "comparison_period": "Dec-2023",
                                "headers": ["1", "2", "Unrated", "Closed", "Total"],
                                "rows": ["1", "2", "Unrated", "New", "Total"],
                                "values": [
                                    [17.9, 3.6, 1.8, 0.0, 23.3],
                                    [0.0, 26.8, 0.0, 1.8, 28.6],
                                    [1.8, 0.0, 35.7, 0.0, 37.5],
                                    [0.0, 1.8, 0.0, 8.9, 10.7],
                                    [19.6, 32.1, 37.5, 10.7, 100.0]
                                ]
                            }
                        }
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Bad request — unexpected internal error.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "An unexpected error occurred: 'NoneType' object has no attribute 'copy'"
                    }
                }
            }
        },
        404: {
            "model": ErrorResponse,
            "description": "Source data or resulting matrix not found.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "No data available after applying dimension filters."
                    }
                }
            }
        },
        422: {
            "description": "Validation errors — incorrect input values, formats, or field naming issues.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "fact_field"],
                                "msg": "Fact field 'ratingg' is not found in the dataset.",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "output_mode"],
                                "msg": "Invalid output mode: 'percentages'. Only 'absolute' or 'percentage' allowed.",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "base_date"],
                                "msg": "Invalid date format: '31/02/2024'. Please use 'dd/mm/yyyy'.",
                                "type": "value_error.date"
                            },
                            {
                                "loc": ["query", "comparison_date"],
                                "msg": "Invalid date format: '31/02/2024'. Please use 'dd/mm/yyyy'.",
                                "type": "value_error.date"
                            },
                            {
                                "loc": ["query", "fact_field"],
                                "msg": "Invalid field name(s): Rating Score. Use lowercase with underscores (e.g., 'rating_score').",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "dimension_filters"],
                                "msg": "Invalid dimension filter format: 'sectorRetail'. Expected format 'field:value'.",
                                "type": "value_error.custom"
                            }
                        ]
                    }
                }
            }
        }
    },
    summary="Get Transition Matrix",
    description="Computes a transition matrix comparing fact field values between two dates, optionally filtered and presented as absolute counts or percentages."
)
def transition_matrix(
    fact_field: str = Query(..., description="Field to track transitions, e.g., rating"),
    base_date: str = Query(..., description="Base date (T+1) in dd/mm/yyyy"),
    comparison_date: Optional[str] = Query(None, description="Comparison date (T) in dd/mm/yyyy"),
    dimension_filters: Optional[str] = Query(None, description="Comma-separated filters like sector:Retail,group:1"),
    column_field: Optional[str] = Query(None, description=" A secondary field ,that is useful for comparing how 'fact_field' values distribute across another dimension"),
    output_mode: str = Query("absolute", description="absolute or percentage")
):
    try:
        # Validate field naming
        validate_field_names([fact_field], "fact_field")
        if dimension_filters:
            filter_fields = [pair.split(':', 1)[0] for pair in dimension_filters.split(',') if ':' in pair]
            validate_field_names(filter_fields, "dimension_filters")
        if column_field:
            validate_field_names([column_field], "column_field")


        result = risk_model.get_transition_matrix(
            fact_field=fact_field,
            base_date=base_date,
            comparison_date=comparison_date,
            dimension_filters=dimension_filters,
            column_field=column_field,
            output_mode=output_mode)
        return result

    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))

    except HTTPException as http_exc:
        raise http_exc

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

## end point 
class MetricRatioGroupResult(RootModel[List[Dict[str, Union[str, float]]]]):
    class Config:
        json_schema_extra = {
            "example": [
                {"sector": "Financials", "coverage_ratio": 85.23},
                {"sector": "Industrials", "coverage_ratio": 74.56},
                {"sector": "Utilities", "coverage_ratio": 95.12}
            ]
        }

class MetricRatioSingleResult(RootModel[Dict[str, Union[str, float]]]):
    class Config:
        json_schema_extra = {
            "example": {
                "sector": "Financials",
                "coverage_ratio": 85.23
            }
        }

class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Dimension filter value is missing for the given dimension filter field."
            }
        }

@app.get(
    "/metric_ratio",
    response_model=Union[MetricRatioGroupResult, MetricRatioSingleResult],
    responses={
        200: {
            "description": "Returns  ratio for total_hc_collateral OR provision over exposure, optionally grouped by a dimension.",
            "content": {
                "application/json": {
                    "examples": {
                        "Grouped Result": {
                            "summary": "Coverage ratios by sector",
                            "value": [
                                {"sector": "Financials", "coverage_ratio": 85.23},
                                {"sector": "Industrials", "coverage_ratio": 74.56},
                                {"sector": "Utilities", "coverage_ratio": 95.12}
                            ]
                        },
                        "Single Result": {
                            "summary": "Single overall coverage ratio",
                            "value": {
                                "coverage_ratio": 85.23
                            }
                        },
                        "With Filter": {
                            "summary": "Coverage ratio with sector filter",
                            "value": {
                                "sector": "Financials",
                                "coverage_ratio": 85.23
                            }
                        }
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Bad request — unexpected internal error.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "An unexpected error occurred: 'NoneType' object has no attribute 'copy'"
                    }
                }
            }
        },
        404: {
            "model": ErrorResponse,
            "description": "Source data or calculation result not found.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "No valid denominator found to compute coverage ratio."
                    }
                }
            }
        },
        422: {
            "description": "Validation error — invalid inputs, fields, or formats.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "group_by_field"],
                                "msg": "Invalid field name(s): Sector Name. Use lowercase with underscores.",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "dimension_filter_field"],
                                "msg": "Invalid field name(s): Sector Name. Use lowercase with underscores.",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "dimension_filter_value"],
                                "msg": "Dimension filter value is missing for the given dimension filter field.",
                                "type": "value_error.missing"
                            },
                            {
                                "loc": ["query", "date_filter"],
                                "msg": "Invalid date format: '32/13/2024'. Please use 'dd/mm/yyyy' format.",
                                "type": "value_error.date"
                            }
                        ]
                    }
                }
            }
        }
    },
    summary="Calculate Ratio",
    description="Computes sum(a valid numeric field) / sum(exposure) with optional filters and groupings."
)
def metric_ratio_endpoint(
    fact_field: str = Query(..., description="A valid Numerator field, e.g., 'provision' or 'total_hc_collateral'"),
    group_by_field: Optional[str] = Query(None, description="Optional group by field (e.g., 'sector')"),
    date_filter: Optional[str] = Query(None, description="Optional date filter (dd/mm/yyyy)"),
    dimension_filter_field: Optional[str] = Query(None, description="Optional filter field (e.g., 'sector')"),
    dimension_filter_value: Optional[str] = Query(None, description="Optional filter value (e.g., 'Financials')")
):
    try:
        validate_field_names([fact_field], "fact_field")
        if group_by_field:
            validate_field_names([group_by_field], "group_by_field")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")

        result = risk_model.get_metric_ratio(
            numerator_field=fact_field,
            denominator_field="exposure",
            group_by_field=group_by_field,
            date_filter=date_filter,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value
        )
        return result
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

class CollateralDistributionData(BaseModel):
    collateral_type: str
    total: int
    percentage: int

class CollateralDistributionResponse(BaseModel):
    collateral_parent_type: str  # changed from collateral_type
    data: List[CollateralDistributionData]  # changed from distribution
    
# ✅ ErrorResponse Model (unchanged)
class ErrorResponse(BaseModel):
    error: str

# ✅ Updated route with correct response model
@app.get(
    "/collateral_distribution",
    response_model=CollateralDistributionResponse,
    responses={
        200: {
            "description": "Returns collateral distribution by category and sub-category.",
            "content": {
                "application/json": {
                    "examples": {
                        "By Category": {
                            "summary": "Distribution by category only",
                            "value": {
                                "collateral_parent_type": "collateral_land_&_building",
                                "data": [
                                    {
                                        "collateral_type": "building",
                                        "total": 11828254410,
                                        "percentage": 65
                                    },
                                    {
                                        "collateral_type": "land",
                                        "total": 6451657056,
                                        "percentage": 35
                                    }
                                ]
                            }
                        },
                        "By Sub-Category": {
                            "summary": "Distribution by sub-category within a category",
                            "value": {
                                "collateral_parent_type": "land",
                                "data": [
                                    {
                                        "collateral_type": "empty_land",
                                        "total": 2236805109,
                                        "percentage": 35
                                    },
                                    {
                                        "collateral_type": "farm_land",
                                        "total": 2903538006,
                                        "percentage": 45
                                    },
                                    {
                                        "collateral_type": "other_functioning_land",
                                        "total": 1311313941,
                                        "percentage": 20
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Unexpected internal error.",
            "content": {
                "application/json": {
                    "example": {
                        "code": 400,
                        "message": "Bad Request",
                        "details": "An unexpected error occurred: 'NoneType' object has no attribute 'copy'"
                    }
                }
            }
        },
        404: {
            "model": ErrorResponse,
            "description": "Collateral data not found.",
            "content": {
                "application/json": {
                    "example": {
                        "code": 404,
                        "message": "Collateral data is not available.",
                        "details": None
                    }
                }
            }
        },
        422: {
            "model": ErrorResponse,
            "description": "Validation error — invalid date or parameters.",
            "content": {
                "application/json": {
                    "example": {
                        "code": 422,
                        "message": "Validation Error",
                        "details": [
                            {
                                "loc": ["query", "category_level"],
                                "msg": "Invalid collateral type: 'XYZ'. Allowed: ['collateral_land_&_building', 'shares']",
                                "type": "value_error"
                            },
                            {
                                "loc": ["query", "date"],
                                "msg": "Invalid date format: '31-04-2024'. Please use 'dd/mm/yyyy'.",
                                "type": "value_error"
                            }
                        ]
                    }
                }
            }
        }
    },
    summary="Get Collateral Distribution",
    description="Returns the distribution of collateral by category and optionally by sub-category for a given date. Supports haircut adjustments."
)

def collateral_distribution(
    category_level: str = Query(..., description="Collateral type, e.g., 'collateral_land_&_building'"),
    sub_category_level: Optional[str] = Query(None, description="Optional sub-category like 'building', 'land', 'shares'"),
    date: str = Query(..., description="Date in DD/MM/YYYY format"),
    haircut: bool = Query(False, description="Whether to apply haircut adjustment")
):
    try:
        # Optional backward compatibility fix
        if category_level == "collatral_land_and_building":
            category_level = "collateral_land_%26_building"

        result = risk_model.get_collateral_distribution(
            category_level=category_level,
            sub_category_level=sub_category_level,
            date_filter=date,
            apply_haircut=haircut
        )

        return result

    except ValueError as ve:
        raise HTTPException(status_code=422, detail=[{
            "loc": ["query"],
            "msg": str(ve),
            "type": "value_error"
        }])

    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail={"error": str(fnf)})

    except Exception as e:
        logger.exception("Unexpected error in /collateral_distribution")
        raise HTTPException(status_code=400, detail={"error": f"An unexpected error occurred: {str(e)}"})
import re

def to_snake_case(s):
    # Replace spaces and hyphens with underscores
    s = re.sub(r'[\s\-]+', '_', s)

    # Insert underscores between camelCase or PascalCase transitions (except acronyms like HC)
    s = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', s)

    return s.lower()

def convert_keys_to_snake_case(obj):
    if isinstance(obj, dict):
        return {to_snake_case(k): convert_keys_to_snake_case(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_keys_to_snake_case(item) for item in obj]
    else:
        return obj

#SuccessResponse Model
class CollateralCustomerItem(BaseModel):
    Customer_Name: str
    Customer_Exposure: Optional[float]
    Customer_HC_Collateral: Optional[float]

class TopCollateralItem(BaseModel):
    Date: str
    Collateral_Name: str
    Type: str
    Grade: Optional[float]
    Collateral_Value: Optional[float]
    HC_Collateral_Value: Optional[float]
    Customers: List[CollateralCustomerItem]

from pydantic import RootModel

class TopCollateralResponse(RootModel[List[TopCollateralItem]]):
    pass

#ErrorResponse Model 
class ErrorResponse(BaseModel):
    code: int
    message: str
    details: Optional[str] = None
    
from fastapi import Query, HTTPException
from typing import Optional, List, Dict, Any
from fastapi.responses import JSONResponse

@app.get(
    "/api_top_collaterals",
    summary="Get Top Collateral Items",
    description="Returns the top collateral items for a given type and date, with an optional limit on the result size.",
    responses={
        200: {
            "description": "A list of top collateral items",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "date": "31/12/2023",
                            "collateral_name": "Building3",
                            "type": "Collateral Land & Building",
                            "grade": 2,
                            "collateral_value": 139535892.5,
                            "hc_collateral_value": 69767946.25,
                            "customers": [
                                {
                                    "customer_name": "National Commercial Bank",
                                    "customer_exposure": 98785689,
                                    "customer_hc_collateral": 24702811.25
                                },
                                {
                                    "customer_name": "Almarai",
                                    "customer_exposure": 113793117,
                                    "customer_hc_collateral": 14200973.5
                                },
                                {
                                    "customer_name": "Commercial Bank",
                                    "customer_exposure": 69329856,
                                    "customer_hc_collateral": 11421999.5
                                }
                            ]
                        },
                        {
                            "date": "31/12/2023",
                            "collateral_name": "Land9",
                            "type": "Collateral Land & Building",
                            "grade": 4,
                            "collateral_value": 11738764.5,
                            "hc_collateral_value": 5869382.25,
                            "customers": [
                                {
                                    "customer_name": "Qatar Islamic Bank",
                                    "customer_exposure": 108752312,
                                    "customer_hc_collateral": 5869382.25
                                }
                            ]
                        }
                    ]
                }
            }
        },
        404: {
            "model": ErrorResponse,
            "description": "No data found for the specified type and date",
            "content": {
                "application/json": {
                    "example": {
                        "code": 404,
                        "message": "No data found for specified type and date.",
                        "details": None
                    }
                }
            }
        },
        422: {
            "model": ErrorResponse,
            "description": "Validation error due to invalid input",
            "content": {
                "application/json": {
                    "example": {
                        "code": 422,
                        "message": "Invalid date format: '31-04-2024'. Please use 'DD/MM/YYYY'.",
                        "details": None
                    }
                }
            }
        },
        500: {
            "model": ErrorResponse,
            "description": "Internal server error",
            "content": {
                "application/json": {
                    "example": {
                        "code": 500,
                        "message": "Internal server error",
                        "details": "Unexpected error occurred while processing the request."
                    }
                }
            }
        }
    }
)
def get_top_collaterals(
    type: str = Query(..., description="Collateral type (e.g., 'collateral_land_&_building')"),
    date_filter: str = Query(..., description="Date in DD/MM/YYYY format (e.g., '31/12/2023')"),
    top_n: Optional[int] = Query(None, description="Maximum number of top items to return")
):
    ...

    """
    Retrieve the top collateral items based on type, date, and an optional limit.
    """
    try:
        # Call the risk_model method to get the top collaterals
        result = risk_model.get_top_collaterals(
            collateral_type=type,
            date_filter=date_filter,
            top_n=top_n
        )

        # Check if the result is empty
        if not result:
            raise HTTPException(
                status_code=404,
                detail=ErrorResponse(
                    code=404,
                    message="No data found for specified type and date."
                ).dict()
            )

        # Return the result; FastAPI will validate and serialize it to List[CollateralItem]
        return convert_keys_to_snake_case(result)

    except ValueError as ve:
        # Handle validation errors (e.g., invalid type or date format)
        raise HTTPException(
            status_code=422,
            detail=ErrorResponse(code=422, message=str(ve)).dict()
        )
    except Exception as e:
        # Log unexpected errors and return a 500 response
        logger.error(f"Unexpected error in /api/top_collaterals: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=ErrorResponse(
                code=500,
                message="Internal server error",
                details=str(e)
            ).dict()
        )
    
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {
                "error": "No 'additional_provision_at_<x>_percent' columns found in the dataset."
            }
        }

@app.get(
    "/additional_provision_percent",
    response_model=List[int],
    responses={
        200: {
            "description": "Returns a list of available additional provision percentage thresholds.",
            "content": {
                "application/json": {
                    "example": [5, 10, 15, 20, 25, 30]
                }
            }
        },
        404: {
            "model": ErrorResponse,
            "description": "Source data or required columns not found.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "No 'additional_provision_at_<x>_percent' columns found in the dataset."
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Unexpected internal error.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "An unexpected error occurred: 'int() argument must be a string, a bytes-like object or a number'"
                    }
                }
            }
        }
    },
    summary="Get Available Additional Provision Percentages",
    description="Returns a sorted list of additional provision percentages."
)
def additional_provision_percent():
    try:
        return risk_model.get_available_additional_provision_percent()
    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")
    
class IncrementalProvisionResponse(BaseModel):
    incremental_provision: float
    filters: Optional[Dict[str, str]] = None

    class Config:
        json_schema_extra = {
            "example": {
                "filters": {
                    "sector": "Retail",
                    "group": "1"
                },
                "incremental_provision": 7342983.52
            }
        }

class ErrorResponse(BaseModel):
    error: str

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Invalid filter format: 'sectorRetail'. Expected 'field:value'."
            }
        }
   
@app.get(
    "/incremental_provision",
    response_model=IncrementalProvisionResponse,
    responses={
        200: {
            "description": "Returns the computed incremental provision.",
            "content": {
                "application/json": {
                    "examples": {
                        "Default Provision": {
                            "summary": "Using only base provision % (no overrides)",
                            "value": {
                                "incremental_provision": 7342983.52,
                                "filters": "null"
                            }
                        },
                        "With Dimension Filter": {
                            "summary": "Filtered by sector and group",
                            "value": {
                                "incremental_provision": 2219832.14,
                                "filters":{
                                "sector": "Retail",
                                "group": "1"
                                }
                            }
                        }
                    }
                }
            }
        },
        404: {
            "model": ErrorResponse,
            "description": "Source data or relevant records not found.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "No data found for the given filters and date."
                    }
                }
            }
        },
        422: {
            "description": "Validation error — invalid fields, filter format, or date format.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "date_filter"],
                                "msg": "Invalid date format: '32/13/2024'. Please use 'dd/mm/yyyy' format.",
                                "type": "value_error.date"
                            },
                            {
                                "loc": ["query", "dimension_filters"],
                                "msg": "Invalid filter format: 'sectorRetail'. Expected 'field:value'.",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "customize_additional_provision_percent"],
                                "msg": "Invalid format for 'customize_additional_provision_percent'. Use format like '1:2,3:5'.",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "additional_provision_percent"],
                                "msg": "'additional_provision_at_15.0_percent' column not found in dataset.",
                                "type": "value_error.custom"
                            }
                        ]
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Unexpected internal error.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "An unexpected error occurred: 'int' object is not subscriptable"
                    }
                }
            }
        }
    },
    summary="Calculate Incremental Provision",
    description="Computes the provision using a default provision percentage or per-customer override, with optional filters."
)
def incremental_provision(
    date_filter: str = Query(..., description="Date in 'dd/mm/yyyy' format"),
    additional_provision_percent: int = Query(..., description="Global additional provision percent to apply"),
    customize_additional_provision_percent: Optional[str] = Query(
        None, description="Optional overrides per-customer in 'cust_id:percent' format, e.g. '1:2,3:5'"
    ),
    dimension_filters: Optional[str] = Query(
        None, description="Optional filters like 'sector:Retail,group:1'"
    )
):
    try:
        return risk_model.calculate_incremental_provision(
            date_filter=date_filter,
            additional_provision_percent=additional_provision_percent,
            customize_additional_provision_percent=customize_additional_provision_percent,
            dimension_filters=dimension_filters
        )
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

class TotalRevisedProvisionResponse(BaseModel):
    total_revised_provision: float
    filters: Optional[Dict[str, str]] = None

    class Config:
        json_schema_extra = {
            "example": {
                "filters": {
                    "sector": "Retail",
                    "group": "1"
                },
                "total_revised_provision": 9834721.38
            }
        }

class ErrorResponse(BaseModel):
    error: str

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Column 'additional_provision_at_10_percent' not found in data."
            }
        }

@app.get(
    "/total_revised_provision",
    response_model=TotalRevisedProvisionResponse,
    responses={
        200: {
            "description": "Returns total revised provision after applying the base and optional overrides.",
            "content": {
                "application/json": {
                    "examples": {
                        "Without Filters": {
                            "summary": "Total provision without filters",
                            "value": {
                                "total_revised_provision": 9821356.72,
                                "filters": "null"
                            }
                        },
                        "With Sector Filter": {
                            "summary": "Total provision for Retail sector, group 1",
                            "value": {
                                "total_revised_provision": 3412874.55,
                                "filters": {
                                "sector": "Retail",
                                "group": "1"
                                },
                            }
                        }
                    }
                }
            }
        },
        404: {
            "model": ErrorResponse,
            "description": "Source data or filtered results not found.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "No data available after applying filters."
                    }
                }
            }
        },
        422: {
            "description": "Validation errors — bad date, fields, or filter/override formats.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "date_filter"],
                                "msg": "Invalid date format: '35/01/2024'. Please use 'dd/mm/yyyy' format.",
                                "type": "value_error.date"
                            },
                            {
                                "loc": ["query", "dimension_filters"],
                                "msg": "Invalid filter format: 'sectorRetail'. Expected 'field:value'.",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "customize_additional_provision_percent"],
                                "msg": "Invalid format for 'customize_additional_provision_percent'. Expected '1:5,2:7,...'",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "additional_provision_percent"],
                                "msg": "Column 'additional_provision_at_10_percent' not found in data.",
                                "type": "value_error.custom"
                            }
                        ]
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Unexpected internal processing error.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "An unexpected error occurred: 'NoneType' object is not subscriptable"
                    }
                }
            }
        }
    },
    summary="Get Total Revised Provision",
    description="Computes the total revised provision including the base provision percent and any per-customer overrides, with optional dimension filters."
)
def total_revised_provision(
    date_filter: str = Query(..., description="Date in 'dd/mm/yyyy' format"),
    additional_provision_percent: int = Query(..., description="Additional provision percent for all customers"),
    customize_additional_provision_percent: Optional[str] = Query(
        None, description="Overrides as 'cust_id:percent', e.g. '1:5,2:7'"
    ),
    dimension_filters: Optional[str] = Query(
        None, description="Comma-separated filters like 'sector:Retail,group:1'"
    )
):
    try:
        return risk_model.get_total_revised_provision(
            date_filter=date_filter,
            additional_provision_percent=additional_provision_percent,
            customize_additional_provision_percent=customize_additional_provision_percent,
            dimension_filters=dimension_filters
        )
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

class RevisedProvisionPercentageResponse(BaseModel):
    revised_provision_percentage: Optional[float]
    filters: Optional[Dict[str, str]] = None

    class Config:
        json_schema_extra = {
            "example": {
                "filters": {
                    "sector": "Retail",
                    "group": "1"
                },
                "revised_provision_percentage": 4.52
            }
        }

class ErrorResponse(BaseModel):
    error: str

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Invalid format for 'customize_additional_provision_percent'. Use 'cust_id:percent' pairs like '1:2,5:10'."
            }
        }

@app.get(
    "/revised_provision_percentage",
    response_model=RevisedProvisionPercentageResponse,
    responses={
        200: {
            "description": "Returns the percentage of provision over exposure after applying provision logic.",
            "content": {
                "application/json": {
                    "examples": {
                        "Basic Result": {
                            "summary": "Without filters",
                            "value": {
                                "revised_provision_percentage": 3.75
                            }
                        },
                        "With Filters": {
                            "summary": "Sector,group-wise filtered result",
                            "value": {
                                "revised_provision_percentage": 4.52,
                                "filters": {
                                "sector": "Retail",
                                "group": "1"
                                }
                            }
                        }
                    }
                }
            }
        },
        404: {
            "model": ErrorResponse,
            "description": "Source data or filtered results not found.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "No data available after applying filters."
                    }
                }
            }
        },
        422: {
            "description": "Validation errors — bad date, field names, or override format.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "date_filter"],
                                "msg": "Invalid date format: '31/13/2024'. Please use 'dd/mm/yyyy' format.",
                                "type": "value_error.date"
                            },
                            {
                                "loc": ["query", "dimension_filters"],
                                "msg": "Invalid filter format: 'sectorRetail'. Expected 'field:value'.",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "customize_additional_provision_percent"],
                                "msg": "Invalid format for 'customize_additional_provision_percent'. Use 'cust_id:percent' pairs like '1:2,5:10'.",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "additional_provision_percent"],
                                "msg": "Column 'additional_provision_at_10.5_percent' not found in dataset.",
                                "type": "value_error.custom"
                            }
                        ]
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Unexpected internal processing error.",
            "content": {
                "application/json": {
                    "example": {
                        "error": "An unexpected error occurred: 'NoneType' object is not subscriptable"
                    }
                }
            }
        }
    },
    summary="Get Revised Provision Percentage",
    description="Computes the revised provision as a percentage of exposure. Supports base and per-customer override provision logic, with optional filters."
)
def revised_provision_percentage(
    date_filter: str = Query(..., description="Date in dd/mm/yyyy format"),
    additional_provision_percent: int = Query(..., description="Default additional provision percent to apply"),
    customize_additional_provision_percent: Optional[str] = Query(
        None, description="Custom additional provision % per cust_id in format: 1:2,5:10"
    ),
    dimension_filters: Optional[str] = Query(
        None, description="Comma-separated filters in format: sector:Retail,group:1"
    )
):
    try:
        result = risk_model.get_revised_provision_percentage(
            date_filter=date_filter,
            additional_provision_percent=additional_provision_percent,
            customize_additional_provision_percent=customize_additional_provision_percent,
            dimension_filters=dimension_filters
        )
        return result

    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")
    
class CustomerProvisionSummaryResponse(RootModel[List[Dict[str, Union[str, int, float]]]]):
    class Config:
        json_schema_extra = {
            "example": [
                {
                    "cust_id": "C001",
                    "cust_name": "Acme Corp",
                    "exposure": 100000,
                    "provision": 2500,
                    "rating": 3,
                    "provision_revised": 3500.0,
                    "provision_percentage_revised": 3.5,
                    "additional_provision_percentage": 5
                },
                {
                    "cust_id": "C002",
                    "cust_name": "Beta Ltd",
                    "exposure": 120000,
                    "provision": 3000,
                    "rating": 2,
                    "provision_revised": 4500.0,
                    "provision_percentage_revised": 3.75,
                    "additional_provision_percentage": 7
                }
            ]
        }

# --- Error Model ---
class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Invalid field name(s): Exposure Amt. Use lowercase with underscores (e.g., 'exposure')"
            }
        }

@app.get(
    "/customer_provision_details",
    response_model=CustomerProvisionSummaryResponse,
    responses={
        200: {
            "description": "Returns customer-level attributes with provision calculations",
            "content": {
                "application/json": {
                    "examples": {
                        "Without Filters": {
                            "summary": "Unfiltered provision results",
                            "value": [
                                {
                                    "cust_id": "C001",
                                    "cust_name": "Acme Corp",
                                    "exposure": 100000,
                                    "provision": 2500,
                                    "rating": 3,
                                    "provision_revised": 3500.0,
                                    "provision_percentage_revised": 3.5,
                                    "additional_provision_percentage": 5
                                },
                                {
                                    "cust_id": "C002",
                                    "cust_name": "Beta Ltd",
                                    "exposure": 120000,
                                    "provision": 3000,
                                    "rating": 2,
                                    "provision_revised": 4500.0,
                                    "provision_percentage_revised": 3.75,
                                    "additional_provision_percentage": 7
                                }
                            ]
                        },
                        "With Filters": {
                            "summary": "Results with sector and group filters",
                            "value": [
                                {
                                    "sector": "Retail",
                                    "group": "2"
                                },
                                {
                                    "cust_id": "C003",
                                    "cust_name": "Gamma Inc",
                                    "exposure": 80000,
                                    "provision": 1800,
                                    "rating": 4,
                                    "provision_revised": 2400.0,
                                    "provision_percentage_revised": 3.0,
                                    "additional_provision_percentage": 5
                                }
                            ]
                        }
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Unexpected internal error or bad input"
        },
        404: {
            "model": ErrorResponse,
            "description": "No data found after applying filters"
        },
        422: {
            "description": "Validation error — incorrect fields or formats",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "attributes"],
                                "msg": "Invalid field name(s): Exposure. Use lowercase with underscores (e.g., 'exposure').",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "customer_fields"],
                                "msg": "Invalid field name(s): Cust ID. Use lowercase with underscores (e.g., 'cust_id').",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "date_filter"],
                                "msg": "Invalid date format: '31/13/2024'. Please use 'dd/mm/yyyy' format.",
                                "type": "value_error.date"
                            },
                            {
                                "loc": ["query", "customize_additional_provision_percent"],
                                "msg": "Invalid format for 'customize_additional_provision_percent'. Use 'cust_id:percent' pairs like '1:2,5:10'.",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "additional_provision_percent"],
                                "msg": "Column 'additional_provision_at_10.5_percent' not found in dataset.",
                                "type": "value_error.custom"
                            },
                            {
                                "loc": ["query", "dimension_filters"],
                                "msg": "Invalid filter format: 'sectorRetail'. Expected 'field:value'.",
                                "type": "value_error.custom"
                            }
                        ]
                    }
                }
            }
        }
    },
    summary="Get Customer Provision Summary",
    description="Returns customer fields and attributes with calculated provision revised and provision charge revised using a global or customized provision percent."
)
def customer_provision_details(
    attributes: str = Query(..., description="Comma-separated metrics to include (e.g., 'exposure,provision,rating')"),
    customer_fields: str = Query(..., description="Comma-separated ID fields (e.g., 'cust_id,cust_name')"),
    date_filter: str = Query(..., description="Date in dd/mm/yyyy format"),
    additional_provision_percent: int = Query(..., description="Global additional provision percent to apply (1-100)"),
    customize_additional_provision_percent: Optional[str] = Query(None, description="Overrides per customer in 'cust_id:percent' format, e.g., '1:5,3:7'"),
    dimension_filters: Optional[str] = Query(None, description="Optional filters like 'sector:Retail,group:1'")
):
    try:
        attr_list = [a.strip() for a in attributes.split(",") if a.strip()]
        cust_fields_list = [c.strip() for c in customer_fields.split(",") if c.strip()]
        validate_field_names(attr_list, "attributes")
        validate_field_names(cust_fields_list, "customer_fields")
        if dimension_filters:
            filter_fields = [f.split(":")[0] for f in dimension_filters.split(",") if ":" in f]
            validate_field_names(filter_fields, "dimension_filters")

        return risk_model.get_customer_provision_details(
            attributes=attr_list,
            customer_fields=cust_fields_list,
            date_filter=date_filter,
            additional_provision_percent=additional_provision_percent,
            customize_additional_provision_percent=customize_additional_provision_percent,
            dimension_filters=dimension_filters
        )

    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except FileNotFoundError as fnf:
        raise HTTPException(status_code=404, detail=str(fnf))
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", port=8000, reload=True)
