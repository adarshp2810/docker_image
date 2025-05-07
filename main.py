import os
import re
import calendar
import logging
from enum import Enum
from datetime import datetime, date
from typing import List, Optional, Union, Dict, Literal ,Any

import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel, Field, RootModel
from dateutil.relativedelta import relativedelta

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

def load_data(folder: str, normalize_cols=True):
    if not os.path.exists(folder):
        raise FileNotFoundError(f"Data folder '{folder}' does not exist. Please ensure 'Sample_Bank_Data' is in the repository root.")
    
    all_data = {"fact_risk": [], "customer": [], "risk_limit": []}
    customer_loaded = False
    rating_loaded = False
    customer_df = None
    rating_df = None

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

                if not customer_loaded and "CUSTOMER" in xls.sheet_names:
                    df_cust = xls.parse("CUSTOMER")
                    if normalize_cols:
                        df_cust.columns = [str(c).strip().lower().replace(" ", "_") for c in df_cust.columns]
                    customer_df = df_cust
                    customer_loaded = True

                if not rating_loaded:
                    for sheet in xls.sheet_names:
                        if sheet.strip().lower().startswith("rating and pds"):
                            df_rating = xls.parse(sheet)
                            if normalize_cols:
                                df_rating.columns = [str(c).strip().lower().replace(" ", "_") for c in df_rating.columns]
                            rating_df = df_rating
                            rating_loaded = True
                            break

                if "Risk Limit" in xls.sheet_names:
                    rl = xls.parse("Risk Limit")
                    if normalize_cols:
                        rl = clean_column_names(rl)
                    rl['effective_date'] = eff_date.strftime('%d/%m/%Y')
                    all_data["risk_limit"].append(rl)

            except Exception as e:
                logger.error(f"Error loading {filename}: {e}")
    if rating_df is None:
        print("Warning: Rating sheet not found in any of the files.")
    merged_data = {
        "fact_risk": pd.concat(all_data["fact_risk"], ignore_index=True) if all_data["fact_risk"] else None,
        "customer": customer_df if customer_df is not None else pd.DataFrame(),
        "risk_limit": pd.concat(all_data["risk_limit"], ignore_index=True) if all_data["risk_limit"] else None,
        "rating": rating_df if rating_df is not None else pd.DataFrame()
    }
    
    if merged_data["fact_risk"] is not None:
        logger.info(f"Fact risk data loaded with {len(merged_data['fact_risk'])} rows, dates: {merged_data['fact_risk']['date'].unique()}")
    if merged_data["risk_limit"] is not None:
        logger.info(f"Risk limit data loaded with {len(merged_data['risk_limit'])} rows, dates: {merged_data['risk_limit']['effective_date'].unique()}")
    
    return merged_data["customer"], merged_data["fact_risk"], merged_data["risk_limit"], merged_data["rating"]

class RiskDataModel:
    def __init__(self, customer_df, fact_df, rl_df, rating_df):
        self.df_fact_risk = fact_df
        self.df_customer = customer_df
        self.rl_df = rl_df
        self.df_rating = rating_df

        if self.df_customer is not None and "cust_name" in self.df_customer.columns:
            self.df_customer = self.df_customer.drop(columns=["cust_name"])

        self._join_data()

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

    def get_distinct_values(self, column_name):
        if self.df_joined is None:
            raise ValueError("No joined data available")
        if column_name not in self.df_joined.columns:
            raise ValueError(f"Column '{column_name}' not found in the dataset.")

        distinct_vals = self.df_joined[column_name].dropna().unique()
        try:
            distinct_vals = sorted(distinct_vals)
        except Exception:
            distinct_vals = list(distinct_vals)
        return distinct_vals

    def get_sum_by_dimension(self, fact_fields, group_by_fields=None, date_filter=None, dimension_filter_field=None, dimension_filter_value=None):
        if self.df_joined is None:
            return {"error": "No joined data available"}
        df = self.df_joined.copy()

        if date_filter:
            df = df[df["date"] == date_filter]

        if dimension_filter_field and dimension_filter_value:
            if dimension_filter_field not in df.columns:
                return {"error": f"Column '{dimension_filter_field}' not found in the dataset."}
            if dimension_filter_field == "group":
                df = df[df[dimension_filter_field] == int(dimension_filter_value)]
            else:
                df = df[df[dimension_filter_field] == dimension_filter_value]

        numerical_fields = [f for f in fact_fields if pd.api.types.is_numeric_dtype(df[f])]
        result = []

        if group_by_fields:
            agg_df = df.groupby(group_by_fields)[numerical_fields].sum().reset_index()
            for field in numerical_fields:
                agg_df[field] = round(agg_df[field], 0)
            result = agg_df.to_dict(orient="records")
            if dimension_filter_field and dimension_filter_value:
                result.insert(0, {dimension_filter_field: dimension_filter_value})
        else:
            sum_series = df[numerical_fields].sum()
            sum_series = sum_series.round(0).astype(int)
            result = sum_series.to_dict()
            if dimension_filter_field and dimension_filter_value:
                result = {dimension_filter_field: dimension_filter_value, **result}

        return result

    def get_avg_by_dimension(self, fact_fields, group_by_fields=None, date_filter=None, dimension_filter_field=None, dimension_filter_value=None):
        if self.df_joined is None:
            return {"error": "No joined data available"}
        df = self.df_joined.copy()

        if date_filter:
            df = df[df["date"] == date_filter]

        if dimension_filter_field and dimension_filter_value:
            if dimension_filter_field not in df.columns:
                return {"error": f"Column '{dimension_filter_field}' not found in the dataset."}
            if dimension_filter_field == "group":
                df = df[df[dimension_filter_field] == int(dimension_filter_value)]
            else:
                df = df[df[dimension_filter_field] == dimension_filter_value]

        numerical_fields = [f for f in fact_fields if pd.api.types.is_numeric_dtype(df[f])]
        result = []

        if group_by_fields:
            df[group_by_fields] = df[group_by_fields].fillna('NA')
            agg_df = df.groupby(group_by_fields)[numerical_fields].mean().reset_index()
            for field in numerical_fields:
                agg_df[field] = agg_df[field].fillna(0).replace([float('inf'), -float('inf')], 0).round(0).astype(int)
            result = agg_df.to_dict(orient="records")
            if dimension_filter_field and dimension_filter_value:
                result.insert(0, {dimension_filter_field: dimension_filter_value})
        else:
            avg_series = df[numerical_fields].mean()
            avg_series = avg_series.fillna(0).replace([float('inf'), -float('inf')], 0).round(0).astype(int)
            result = avg_series.to_dict()
            if dimension_filter_field and dimension_filter_value:
                result = {dimension_filter_field: dimension_filter_value, **result}

        return result

    def count_distinct(self, dimension, date_filter=None, dimension_filter_field=None, dimension_filter_value=None):
        if self.df_joined is None:
            return {"error": "No joined data available"}
        df = self.df_joined.copy()

        if date_filter:
            df = df[df["date"] == date_filter]

        if dimension_filter_field and dimension_filter_value:
            if dimension_filter_field not in df.columns:
                return {"error": f"Column '{dimension_filter_field}' not found in the dataset."}
            if dimension_filter_field == "group":
                df = df[df[dimension_filter_field] == int(dimension_filter_value)]
            else:
                df = df[df[dimension_filter_field] == dimension_filter_value]

        distinct_count = df[dimension].dropna().nunique()
        result = {"count": distinct_count}
        if dimension_filter_field and dimension_filter_value:
            result = {dimension_filter_field: dimension_filter_value, **result}
        return result

    def get_concentration(self, fact_fields, group_by_fields=None, date_filter=None, top_n=10, dimension_filter_field=None, dimension_filter_value=None):
        if self.df_joined is None:
            return {"error": "No joined data available"}
        df = self.df_joined.copy()

        if date_filter:
            df = df[df["date"] == date_filter]

        if dimension_filter_field and dimension_filter_value:
            if dimension_filter_field not in df.columns:
                return {"error": f"Column '{dimension_filter_field}' not found in the dataset."}
            if dimension_filter_field == "group":
                df = df[df[dimension_filter_field] == int(dimension_filter_value)]
            else:
                df = df[df[dimension_filter_field] == dimension_filter_value]

        fact_field_1 = fact_fields[0]
        fact_field_2 = fact_fields[1] if len(fact_fields) > 1 else fact_fields[0]

        if group_by_fields:
            top_n_df = df.groupby(group_by_fields).agg({fact_field_1: "sum", fact_field_2: "sum"}).reset_index()
            top_n_df = top_n_df.sort_values(fact_field_1, ascending=False).head(top_n)
            top_n_value_1 = top_n_df[fact_field_1].sum()
            top_n_value_2 = df[fact_field_2].sum()
        else:
            top_n_value_1 = df[fact_field_1].sum()
            top_n_value_2 = df[fact_field_2].sum()

        concentration = (top_n_value_1 / top_n_value_2) * 100 if top_n_value_2 > 0 else 0
        result = {
            fact_field_1: round(float(top_n_value_1), 0),
            "concentration_percentage": f"{round(concentration, 0)}%"
        }
        if dimension_filter_field and dimension_filter_value:
            result = {dimension_filter_field: dimension_filter_value, **result}
        return result

    def get_portfolio_trend_summary(self, fact_fields, date_filter, period_type="M", lookback=5):
        if self.df_joined is None:
            return {"error": "No joined data available"}
        df = self.df_joined.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)

        selected_date = pd.to_datetime(date_filter, dayfirst=True)
        df["period"] = df["date"].dt.to_period(period_type)
        df["period_str"] = df["period"].dt.strftime('%b, %Y')

        period_list = [(selected_date - relativedelta(months=i if period_type == "M" else i * 3)).to_period(period_type) for i in range(lookback + 1)]
        period_strs = [p.strftime('%b, %Y') for p in period_list]
        df = df[df["period"].isin(period_list)]

        results = []
        for p in period_list:
            p_str = p.strftime('%b, %Y')
            df_p = df[df["period"] == p]
            row = {"period": p_str}
            for field in fact_fields:
                row[field] = round(df_p[field].sum(), 0) if field in df_p.columns else None
            if "rating" in df_p.columns:
                avg_rating = df_p["rating"].mean()
                row["avg_rating_score"] = round(avg_rating, 1) if pd.notna(avg_rating) else None
            else:
                row["avg_rating_score"] = None
            row["total_customers"] = df_p["cust_id"].nunique()
            results.append(row)

        results.sort(key=lambda x: datetime.strptime(x["period"], "%b, %Y"))
        for row in results:
            for key, val in row.items():
                if isinstance(val, (np.integer, np.floating)):
                    row[key] = val.item()
        return results

    def get_segment_distribution(self, fact_field, dimension_field, date_filter=None, start=1, end=20, interval=10, others=True, dimension_filter_field=None, dimension_filter_value=None):
        if self.df_joined is None:
            return {"error": "No joined data available"}
        df = self.df_joined.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)

        if date_filter:
            target_date = pd.to_datetime(date_filter, dayfirst=True)
            df = df[df["date"] == target_date]

        if dimension_filter_field and dimension_filter_value:
            if dimension_filter_field not in df.columns:
                return {"error": f"Column '{dimension_filter_field}' not found in the dataset."}
            if dimension_filter_field == "group":
                df = df[df[dimension_filter_field] == int(dimension_filter_value)]
            else:
                df = df[df[dimension_filter_field] == dimension_filter_value]

        if fact_field not in df.columns or dimension_field not in df.columns:
            return {"error": "Invalid input or no data for the given date."}

        df_ranked = df.groupby(dimension_field)[fact_field].sum().sort_values(ascending=False).reset_index()
        total_fact_field = df_ranked[fact_field].sum()
        segments = []

        if dimension_filter_field and dimension_filter_value:
            segments.append({dimension_filter_field: dimension_filter_value})

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
            segment_df = df_ranked.iloc[start - 1:end]
            segment_total = segment_df[fact_field].sum()
            segment_percentage = (segment_total / total_fact_field) * 100
            segments.append({
                "segment": f"Top {start}-{end}",
                fact_field: int(segment_total),
                "percentage": f"{round(segment_percentage, 1)}%"
            })
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

    def get_ranked_entities_with_others(self, fact_field, dimension_field, date_filter=None, start=1, end=10, others_option=False, dimension_filter_field=None, dimension_filter_value=None):
        if self.df_joined is None:
            return {"error": "No joined data available"}
        df = self.df_joined.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)

        if date_filter:
            target_date = pd.to_datetime(date_filter, dayfirst=True)
            df = df[df["date"] == target_date]

        if dimension_filter_field and dimension_filter_value:
            if dimension_filter_field not in df.columns:
                return {"error": f"Column '{dimension_filter_field}' not found in the dataset."}
            if dimension_filter_field == "group":
                df = df[df[dimension_filter_field] == int(dimension_filter_value)]
            else:
                df = df[df[dimension_filter_field] == dimension_filter_value]

        if df.empty or fact_field not in df.columns or dimension_field not in df.columns:
            return {"error": "Invalid input or no data for the given date."}

        ranked_entities = df.groupby(dimension_field)[fact_field].sum().sort_values(ascending=False).reset_index()
        selected_entities = ranked_entities.iloc[start - 1:end]
        total_selected_value = selected_entities[fact_field].sum()

        if others_option:
            others = ranked_entities.iloc[end:]
            others_value = others[fact_field].sum()
            others_percentage = round((others_value / others_value) * 100, 1) if others_value > 0 else 0
            return [{
                "segment": "Others",
                fact_field: int(others_value),
                "percentage": f"{others_percentage}%"
            }]

        result = []
        if dimension_field == "cust_id":
            cust_id_to_name = self.df_joined[["cust_id", "cust_name"]].drop_duplicates().set_index("cust_id")["cust_name"].to_dict()

        for _, row in selected_entities.iterrows():
            value = row[dimension_field]
            fact_val = round(float(row[fact_field]))
            percent = round((fact_val / total_selected_value) * 100, 1) if total_selected_value > 0 else 0
            if dimension_field == "cust_id":
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
    dimension_filter_value: Optional[str] = None
    ):
        df = self.df_joined.copy()

        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        if date_filter:
            df = df[df["date"] == pd.to_datetime(date_filter, dayfirst=True)]

        if dimension_filter_field and dimension_filter_value:
            if dimension_filter_field not in df.columns:
                return {"error": f"Column {dimension_filter_field} not found in the dataset."}
            if dimension_filter_field == "group":
                df = df[df[dimension_filter_field] == int(dimension_filter_value)]
            else:
                df = df[df[dimension_filter_field] == str(dimension_filter_value)]

        if df.empty or fact_field not in df.columns or dimension_field_to_rank not in df.columns:
            return {"error": "Invalid input or no data for the given date."}

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
        df = self.df_joined.copy()

        if date_filter:
            df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
            df = df[df["date"] == pd.to_datetime(date_filter, dayfirst=True)]

        if dimension_filter_field and dimension_filter_value:
            if dimension_filter_field not in df.columns:
                return {"error": f"Column {dimension_filter_field} not found in the dataset."}
            if dimension_filter_field == "group":
                df = df[df[dimension_filter_field] == int(dimension_filter_value)]
            else:
                df = df[df[dimension_filter_field] == str(dimension_filter_value)]

        if df.empty or fact_field not in df.columns or dimension_field not in df.columns:
            return {"error": "Invalid input or no data for the given date."}

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
        dimension_filter_value: Optional[str] = None
    ):
        user_date = pd.to_datetime(date, format='%d/%m/%Y')
        user_month_year = user_date.strftime('%b %y')  

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

        df = self.df_joined.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)

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
    start_rank: int = 1,
    end_rank: int = 10,
    period_type: str = 'Q',
    lookback: int = 5,
    dimension_field: str = 'rating',
    dimension_filter_field: Optional[str] = None,
    dimension_filter_value: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        # Validate rank range
        if end_rank < start_rank:
            return [{"error": f"end_rank ({end_rank}) must be >= start_rank ({start_rank})."}]

        df = self.df_joined.copy()

        # Normalize inputs
        fact_field = fact_field.lower().strip()
        dim_rank = dimension_field_to_rank.lower().strip()
        dim_field = dimension_field.lower().strip()
        filt_field = dimension_filter_field.lower().strip() if dimension_filter_field else None

        # Validate fields
        for fld in [fact_field, dim_rank, dim_field]:
            if fld not in df.columns:
                return [{"error": f"'{fld}' not found; available: {df.columns.tolist()}"}]
        if dim_rank == 'cust id' and 'cust name' not in df.columns:
            return [{"error": "cust name required when ranking by cust id."}]

        logger.info(f"Fact field '{fact_field}' dtype: {df[fact_field].dtype}")
        logger.info(f"Sample values: {df[fact_field].head(5).tolist()}")

        # Check for duplicates
        dup_count = df.duplicated(subset=['cust_id', 'date']).sum()
        if dup_count > 0:
            logger.warning(f"Found {dup_count} duplicate cust id-date combinations.")

        # Apply optional filter
        if filt_field and dimension_filter_value is not None:
            if filt_field not in df.columns:
                return [{"error": f"Filter field '{filt_field}' not found."}]
            if filt_field == "group":
                df = df[df[filt_field] == int(dimension_filter_value)]
            else:
                df = df[df[filt_field].astype(str) == str(dimension_filter_value)]
            logger.info(f"After filter ({filt_field}={dimension_filter_value}): {len(df)} records")
            if df.empty:
                return [{"error": "No data after applying filter. Check values."}]

        # Calculate periods and filter
        periods = self._calculate_periods(date, lookback, period_type)
        df['period'] = pd.to_datetime(df['date'], errors="coerce", dayfirst=True).dt.strftime('%b %y')
        df_periods = df[df['period'].isin(periods)]
        logger.info(f"After period filter ({periods}): {len(df_periods)} records")
        if df_periods.empty:
            return [{"error": f"No data for periods: {periods}. Check date or data availability."}]

        # Ensure fact_field numeric
        df_periods[fact_field] = pd.to_numeric(df_periods[fact_field], errors='coerce')
        logger.info(f"Fact field '{fact_field}' null count: {df_periods[fact_field].isna().sum()}")

        # Compute dense rank per period
        df_periods['rank'] = df_periods.groupby('period')[fact_field] \
                                    .rank(ascending=False, method='dense')

        # Identify primary period and select window
        primary = periods[0]
        df_primary = df_periods[df_periods['period'] == primary]
        logger.info(f"Primary period '{primary}': {len(df_primary)} records")
        df_selected = df_primary[
            (df_primary['rank'] >= start_rank) &
            (df_primary['rank'] <= end_rank)
        ]
        logger.info(f"Selected {len(df_selected)} records in rank {start_rank}-{end_rank}")
        if df_selected.empty:
            return [{"error": f"No customers in rank range {start_rank}-{end_rank} for {primary}."}]

        # Order keys by rank in primary period
        order_keys = df_selected.sort_values('rank')[dim_rank].unique().tolist()
        logger.info(f"Ordered dimension keys: {order_keys}")

        # Build full history for selected keys
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
customer_df, fact_df, rl_df, rating_df = load_data(DATA_FOLDER)
risk_model = RiskDataModel(customer_df, fact_df, rl_df, rating_df)
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
        
class DistinctValuesResponse(BaseModel):
    column: str = Field(..., description="The name of the column queried", example="staging")
    values: List[Union[str, int, float]] = Field(..., description="List of distinct (non-null) values from the specified column", example=["1", "2", "3B"])
    class Config:
        json_schema_extra = {
            "example": {
                "column": "staging",
                "values": ["1", "2", "3B"]
            }
        }

class DistinctValuesErrorResponse(BaseModel):
    error: str = Field(..., description="Description of the error encountered during processing", example="Column 'xyz' not found in the dataset.")

@app.get(
        "/distinct_values",
        response_model=DistinctValuesResponse,
        responses={
        200: {"description": "Successfully retrieved distinct values."},
        400: {"model": DistinctValuesErrorResponse, "description": "Bad request — column not found or internal error."},
        422: {
            "description": "Validation error due to missing or incorrect parameters.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "column"],
                                "msg": "field required",
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
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

# --- Success Model: Grouped Results ---
class GroupedSumRecord(RootModel[List[Dict[str, Union[str, int, float]]]]):

    class Config:
        json_schema_extra = {
            "examples": [
                {
                    "sector": "Retail",
                    "exposure": 150000.0,
                    "provision": 12000.0
                },
                {
                    "sector": "Finance",
                    "exposure": 200000.0,
                    "provision": 18000.0
                }
            ]
        }

# --- Success Model: Ungrouped Sum (single-row dict with optional dimension field) ---
class UngroupedSumResponse(RootModel[Dict[str, Union[str, int, float]]]):
    class Config:
        json_schema_extra = {
            "example": {
                "group": "2",
                "exposure": 340000.0,
                "provision": 28000.0
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
                    "Grouped Result": {
                        "summary": "Grouped by sector",
                        "value": [
                            {"sector": "Retail", "exposure": 150000.0, "provision": 12000.0},
                            {"sector": "Finance", "exposure": 200000.0, "provision": 18000.0}
                        ]
                    },
                    "Ungrouped Result with Dimension Filter": {
                        "summary": "Flat total with dimension filter (e.g., group=2)",
                        "value": {
                            "group": "2",
                            "exposure": 340000.0,
                            "provision": 28000.0
                        }
                    },
                    "Ungrouped Result without Dimension Filter": {
                        "summary": "Flat total with no dimension filter",
                        "value": {
                            "exposure": 250000.0,
                            "provision": 10000.0
                        }
                    }
                }
            }
        }
    },
    400: {
        "model": ErrorResponse,
        "description": "Bad request or processing error"
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
                            "loc": ["query", "group_by_fields"],
                            "msg": "str type expected",
                            "type": "type_error.str"
                        },
                        {
                            "loc": ["query", "date_filter"],
                            "msg": "invalid date format, expected 'dd/mm/yyyy'",
                            "type": "value_error.date"
                        },
                        {
                            "loc": ["query", "dimension_filter_field"],
                            "msg": "str type expected",
                            "type": "type_error.str"
                        },
                        {
                            "loc": ["query", "dimension_filter_value"],
                            "msg": "str type expected",
                            "type": "type_error.str"
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
    description="Aggregates one or more numeric fact fields, optionally grouped by dimensions and filtered by date or a dimension value."
)
def get_sum_by_dimension(
    fact_fields: str = Query(..., description="Comma-separated list of fact fields to aggregate, e.g., 'exposure,provision'"),
    group_by_fields: str = Query(None, description="Comma-separated list of fields to group by, e.g., 'cust_id'"),
    date_filter: Optional[str] = Query(None, description="Date in dd/mm/yyyy format"),
    dimension_filter_field: Optional[str] = Query(None, description="Field name to filter the data by, e.g., 'sector'"),
    dimension_filter_value: Optional[str] = Query(None, description="Value of the dimension field to filter by, e.g., 'finance'")
):
    try:
        fact_fields_list  = [field.strip() for field in fact_fields.split(',')]  # Parse fact_fields as a list
        group_by_fields_list = [field.strip() for field in group_by_fields.split(',')] if group_by_fields else None

        validate_field_names(fact_fields_list, "fact_fields")
        validate_field_names(group_by_fields_list, "group_by_fields")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")
        result = risk_model.get_sum_by_dimension(
            fact_fields=fact_fields_list,
            group_by_fields=group_by_fields_list if group_by_fields else None,
            date_filter=date_filter,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value
        )
        if isinstance(result, dict) and "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- Success Response Model ---
class GroupedAvgRecord(RootModel[List[Dict[str, Union[str, int, float]]]]):
    class Config:
        json_schema_extra = {
            "examples": [
                {
                    "sector": "Retail",
                    "exposure": 150000.0,
                    "provision": 12000.0
                },
                {
                    "sector": "Finance",
                    "exposure": 200000.0,
                    "provision": 18000.0
                }
            ]
        }

class UngroupedAvgResponse(RootModel[Dict[str, Union[str, int, float]]]):
    class Config:
        json_schema_extra = {
            "example": {
                "group": "2",
                "exposure": 340000.0,
                "provision": 28000.0
            }
        }

# --- Error Response Model ---
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
                                {"sector": "Retail", "exposure": 150000.0, "provision": 12000.0},
                                {"sector": "Finance", "exposure": 200000.0, "provision": 18000.0}
                            ]
                        },
                        "Ungrouped with Filter": {
                            "summary": "Flat average with dimension filter",
                            "value": {
                                "group": "2",
                                "exposure": 340000.0,
                                "provision": 28000.0
                            }
                        },
                        "Ungrouped Total Average": {
                            "summary": "Flat average with no filters",
                            "value": {
                                "exposure": 250000.0,
                                "provision": 10000.0
                            }
                        }
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Invalid column or internal error"
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
                                "loc": ["query", "group_by_fields"],
                                "msg": "str type expected",
                                "type": "type_error.str"
                            },
                            {
                                "loc": ["query", "date_filter"],
                                "msg": "invalid date format, expected 'dd/mm/yyyy'",
                                "type": "value_error.date"
                            },
                            {
                                "loc": ["query", "dimension_filter_field"],
                                "msg": "str type expected",
                                "type": "type_error.str"
                            },
                            {
                                "loc": ["query", "dimension_filter_value"],
                                "msg": "str type expected",
                                "type": "type_error.str"
                            },
                            {
                                "loc": ["query", "fact_fields"],
                                "msg": "Invalid field name(s): Exposure, Cust Name. Use lowercase with underscores (e.g., 'cust_name')",
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
        group_by_fields_list = [g.strip() for g in group_by_fields.split(',')] if group_by_fields else []

        # Field naming convention checks
        validate_field_names(fact_fields_list, "fact_fields")
        validate_field_names(group_by_fields_list, "group_by_fields")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")

        result = risk_model.get_avg_by_dimension(
            fact_fields=fact_fields_list,
            group_by_fields=group_by_fields_list if group_by_fields else None,
            date_filter=date_filter,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value
        )

        if isinstance(result, dict) and "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        return result

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

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
                    "example": {
                        "sector": "Retail",
                        "count": 42
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Invalid dimension or processing error."
        },
        422: {
            "description": "Validation error — missing field, wrong type, or incorrect format.",
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
                                "msg": "invalid date format, expected 'dd/mm/yyyy'",
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
                                "loc": ["query", "dimension_filter_field"],
                                "msg": "str type expected",
                                "type": "type_error.str"
                            },
                            {
                                "loc": ["query", "dimension_filter_value"],
                                "msg": "str type expected",
                                "type": "type_error.str"
                            }
                        ]
                    }
                }
            }
        }
    },
    summary="Count Distinct Values in a Dimension",
    description="Returns the number of distinct non-null values in a specified dimension column, with optional date and dimension filtering."
)
def count_distinct_values(
    dimension: str = Query(..., description="Dimension field name to count unique values from (e.g., 'cust_id')"),
    date_filter: Optional[str] = Query(None, description="Filter records for a specific date (format: dd/mm/yyyy)"),
    dimension_filter_field: Optional[str] = Query(None, description="Optional field name to apply as a filter (e.g., 'sector')"),
    dimension_filter_value: Optional[str] = Query(None, description="Value for the dimension filter (e.g., 'Retail')")
):
    try:
        validate_field_names([dimension], "dimension")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")

        result = risk_model.count_distinct(
            dimension,
            date_filter,
            dimension_filter_field,
            dimension_filter_value
        )

        if isinstance(result, dict) and "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        return result

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Success Response Model ---
class ConcentrationResponse(RootModel[Dict[str, Union[str, float, int]]]):
    class Config:
        json_schema_extra = {
            "example": {
                "exposure": 1200000.0,
                "concentration_percentage": "63%"
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
                                "sector": "Retail",
                                "exposure": 1200000.0,
                                "concentration_percentage": "63%"
                            }
                        },
                        "Without Filter": {
                            "summary": "No dimension filter applied",
                            "value": {
                                "exposure": 1800000.0,
                                "concentration_percentage": "52%"
                            }
                        }
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Invalid input or internal processing error"
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
                                "msg": "invalid date format, expected 'dd/mm/yyyy'",
                                "type": "value_error.date"
                            },
                            {
                                "loc": ["query", "top_n"],
                                "msg": "value is not a valid integer",
                                "type": "type_error.integer"
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
        group_by_fields_list = [g.strip() for g in group_by_fields.split(',')] if group_by_fields else []

        validate_field_names(fact_fields_list, "fact_fields")
        validate_field_names(group_by_fields_list, "group_by_fields")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")

        result = risk_model.get_concentration(
            fact_fields=fact_fields_list,
            group_by_fields=group_by_fields_list if group_by_fields else None,
            date_filter=date_filter,
            top_n=top_n,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value
        )

        if isinstance(result, dict) and "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        return result

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

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
        400: {
            "model": ErrorResponse,
            "description": "Invalid processing logic or missing input"
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
                                "loc": ["query", "date_filter"],
                                "msg": "invalid date format, expected 'dd/mm/yyyy'",
                                "type": "value_error.date"
                            },
                            {
                                "loc": ["query", "period_type"],
                                "msg": "unexpected value; allowed: 'M', 'Q'",
                                "type": "value_error.enum"
                            },
                            {
                                "loc": ["query", "lookback"],
                                "msg": "value is not a valid integer",
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
    lookback: int = Query(5, description="Number of periods (months or quarters) to look back")
):
    try:
        fact_field_list = [field.strip() for field in fact_fields.split(",") if field.strip()]

        validate_field_names(fact_field_list, "fact_fields")

        # Manual validation for date format
        if not re.match(r"^\d{2}/\d{2}/\d{4}$", date_filter):
            raise HTTPException(
                status_code=422,
                detail=[
                    {
                        "loc": ["query", "date_filter"],
                        "msg": "invalid date format, expected 'dd/mm/yyyy'",
                        "type": "value_error.date"
                    }
                ]
            )

        result = risk_model.get_portfolio_trend_summary(
            fact_fields=fact_field_list,
            date_filter=date_filter,
            period_type=period_type,
            lookback=lookback
        )

        if isinstance(result, dict) and "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

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
                                {"sector": "Retail"},
                                {"segment": "Top 1–10", "exposure": 800000, "percentage": "50.0%"},
                                {"segment": "Top 11–20", "exposure": 600000, "percentage": "37.5%"},
                                {"segment": "Others", "exposure": 200000, "percentage": "12.5%"}
                            ]
                        }
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Invalid column or internal processing error"
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
                            {"loc": ["query", "date_filter"], "msg": "invalid date format, expected 'dd/mm/yyyy'", "type": "value_error.date"},
                            {"loc": ["query", "start"], "msg": "value is not a valid integer", "type": "type_error.integer"},
                            {"loc": ["query", "end"], "msg": "value is not a valid integer", "type": "type_error.integer"},
                            {"loc": ["query", "interval"], "msg": "value is not a valid integer", "type": "type_error.integer"},
                            {"loc": ["query", "others"], "msg": "value could not be parsed to a boolean", "type": "type_error.bool"},
                            {"loc": ["query", "dimension_filter_field"], "msg": "Invalid field name(s): Sector Name. Use lowercase with underscores", "type": "value_error.custom"}
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
    dimension_filter_value: Optional[str] = Query(None, description="Value to filter on (e.g., 'Retail')")
):
    try:
        validate_field_names([fact_field], "fact_field")
        validate_field_names([dimension_field], "dimension_field")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")

        if date_filter and not re.match(r"^\d{2}/\d{2}/\d{4}$", date_filter):
            raise HTTPException(
                status_code=422,
                detail=[{
                    "loc": ["query", "date_filter"],
                    "msg": "invalid date format, expected 'dd/mm/yyyy'",
                    "type": "value_error.date"
                }]
            )

        result = risk_model.get_segment_distribution(
            fact_field=fact_field,
            dimension_field=dimension_field,
            date_filter=date_filter,
            start=start,
            end=end,
            interval=interval,
            others=others,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value
        )

        if isinstance(result, dict) and "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

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
                            "summary": "Filtered by sector = Retail",
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
                            {"loc": ["query", "fact_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "dimension_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "fact_field"], "msg": "Invalid field name(s): Exposure Amt. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_field"], "msg": "Invalid field name(s): Cust ID. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "date_filter"], "msg": "invalid date format, expected 'dd/mm/yyyy'", "type": "value_error.date"},
                            {"loc": ["query", "start"], "msg": "value is not a valid integer", "type": "type_error.integer"},
                            {"loc": ["query", "end"], "msg": "value is not a valid integer", "type": "type_error.integer"},
                            {"loc": ["query", "others_option"], "msg": "value could not be parsed to a boolean", "type": "type_error.bool"},
                            {"loc": ["query", "dimension_filter_field"], "msg": "Invalid field name(s): Segment Name. Use lowercase with underscores", "type": "value_error.custom"}
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
    dimension_filter_value: Optional[str] = Query(None, description="Value for dimension filter (e.g., 'Retail')")
):
    try:
        validate_field_names([fact_field], "fact_field")
        validate_field_names([dimension_field], "dimension_field")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")

        if date_filter and not re.match(r"^\d{2}/\d{2}/\d{4}$", date_filter):
            raise HTTPException(
                status_code=422,
                detail=[{
                    "loc": ["query", "date_filter"],
                    "msg": "invalid date format, expected 'dd/mm/yyyy'",
                    "type": "value_error.date"
                }]
            )

        result = risk_model.get_ranked_entities_with_others(
            fact_field=fact_field,
            dimension_field=dimension_field,
            date_filter=date_filter,
            start=start,
            end=end,
            others_option=others_option,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value
        )

        if isinstance(result, dict) and "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- Success Response Model ---
class RankedDistributionResponse(RootModel[List[Dict[str, Union[str, int]]]]):
    class Config:
        json_schema_extra = {
            "example": [
                {"group": "Group A", "exposure": 500000, "percentage": "25%"},
                {"group": "Group B", "exposure": 300000, "percentage": "15%"},
                {"group": "Group C", "exposure": 200000, "percentage": "10%"}
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
                                {"group": "Group A", "exposure": 500000, "percentage": "25%"},
                                {"group": "Group B", "exposure": 300000, "percentage": "15%"},
                                {"group": "Group C", "exposure": 200000, "percentage": "10%"}
                            ]
                        },
                        "With Dimension Filter": {
                            "summary": "Filtered by sector = Retail",
                            "value": [
                                {"sector": "Retail"},
                                {"group": "Group A", "exposure": 400000, "percentage": "40%"},
                                {"group": "Group B", "exposure": 300000, "percentage": "30%"},
                                {"group": "Group C", "exposure": 200000, "percentage": "20%"}
                            ]
                        }
                    }
                }
            }
        },
        400: {
            "model": ErrorResponse,
            "description": "Invalid input or internal error"
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
                            {"loc": ["query", "date_filter"], "msg": "invalid date format, expected 'dd/mm/yyyy'", "type": "value_error.date"},
                            {"loc": ["query", "start_rank"], "msg": "value is not a valid integer", "type": "type_error.integer"},
                            {"loc": ["query", "end_rank"], "msg": "value is not a valid integer", "type": "type_error.integer"},
                            {"loc": ["query", "others_option"], "msg": "value could not be parsed to a boolean", "type": "type_error.bool"},
                            {"loc": ["query", "dimension_filter_field"], "msg": "Invalid field name(s): Segment Name. Use lowercase with underscores", "type": "value_error.custom"}
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

        if date_filter and not re.match(r"^\d{2}/\d{2}/\d{4}$", date_filter):
            raise HTTPException(
                status_code=422,
                detail=[{
                    "loc": ["query", "date_filter"],
                    "msg": "invalid date format, expected 'dd/mm/yyyy'",
                    "type": "value_error.date"
                }]
            )

        result = risk_model.get_ranked_distribution_by_grouping(
            fact_field=fact_field,
            dimension_field_to_rank=dimension_field_to_rank,
            group_by_field=group_by_field,
            start_rank=start_rank,
            end_rank=end_rank,
            others_option=others_option,
            date_filter=date_filter,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value
        )

        if isinstance(result, dict) and "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
# --- Success Response Model ---
class PercentDistributionResponse(RootModel[List[Dict[str, Union[str, int]]]]):
    class Config:
        json_schema_extra = {
            "example": [
                {"sector": "Retail", "percentage": "40%"},
                {"sector": "Finance", "percentage": "30%"},
                {"sector": "Healthcare", "percentage": "15%"}
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
                                {"sector": "Retail", "percentage": "40%"},
                                {"sector": "Finance", "percentage": "30%"},
                                {"sector": "Healthcare", "percentage": "15%"}
                            ]
                        },
                        "With Dimension Filter": {
                            "summary": "Filtered by group = 1",
                            "value": [
                                {"group": "1"},
                                {"sector": "Retail", "percentage": "60%"},
                                {"sector": "Finance", "percentage": "40%"}
                            ]
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
                            {"loc": ["query", "fact_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "dimension_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "fact_field"], "msg": "Invalid field name(s): Exposure Amt. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_field"], "msg": "Invalid field name(s): Sector Name. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "date_filter"], "msg": "invalid date format, expected 'dd/mm/yyyy'", "type": "value_error.date"},
                            {"loc": ["query", "dimension_filter_field"], "msg": "Invalid field name(s): Industry Type. Use lowercase with underscores", "type": "value_error.custom"}
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

        if date_filter and not re.match(r"^\d{2}/\d{2}/\d{4}$", date_filter):
            raise HTTPException(
                status_code=422,
                detail=[{
                    "loc": ["query", "date_filter"],
                    "msg": "invalid date format, expected 'dd/mm/yyyy'",
                    "type": "value_error.date"
                }]
            )

        result = risk_model.get_perc_distribution_by_field(
            fact_field=fact_field,
            dimension_field=dimension_field,
            date_filter=date_filter,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value
        )

        if isinstance(result, dict) and "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- Success Response Model ---
class PercentageTrendResponse(RootModel[List[Dict[str, Union[str, int]]]]):
    class Config:
        json_schema_extra = {
            "example": [
                {"period": "Jan 24", "Retail": "40%", "Finance": "35%", "Others": "25%"},
                {"period": "Feb 24", "Retail": "38%", "Finance": "37%", "Others": "25%"}
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
                                {"period": "Jan 24", "Retail": "40%", "Finance": "35%", "Others": "25%"},
                                {"period": "Feb 24", "Retail": "38%", "Finance": "37%", "Others": "25%"}
                            ]
                        },
                        "With Dimension Filter": {
                            "summary": "Filtered by group = 1",
                            "value": [
                                {"group": "1"},
                                {"period": "Jan 24", "Retail": "50%", "Finance": "30%", "Others": "20%"},
                                {"period": "Feb 24", "Retail": "48%", "Finance": "32%", "Others": "20%"}
                            ]
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
                            {"loc": ["query", "fact_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "dimension_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "fact_field"], "msg": "Invalid field name(s): Exposure Amt. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_field"], "msg": "Invalid field name(s): Cust ID. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "date"], "msg": "invalid date format, expected 'dd/mm/yyyy'", "type": "value_error.date"},
                            {"loc": ["query", "period_type"], "msg": "unexpected value; allowed: 'M', 'Q'", "type": "value_error.enum"},
                            {"loc": ["query", "lookback_range"], "msg": "value is not a valid integer", "type": "type_error.integer"},
                            {"loc": ["query", "dimension_filter_field"], "msg": "Invalid field name(s): Sector Name. Use lowercase with underscores", "type": "value_error.custom"}
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

        if not re.match(r"^\d{2}/\d{2}/\d{4}$", date):
            raise HTTPException(
                status_code=422,
                detail=[{
                    "loc": ["query", "date"],
                    "msg": "invalid date format, expected 'dd/mm/yyyy'",
                    "type": "value_error.date"
                }]
            )

        result = risk_model.get_percentage_trend_by_field(
            fact_field=fact_field,
            dimension_field=dimension_field,
            date=date,
            period_type=period_type,
            lookback_range=lookback_range,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value
        )

        if isinstance(result, dict) and "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- Success Response Model ---
class RankedDataByPeriodResponse(RootModel[List[Dict[str, Any]]]):
    class Config:
        json_schema_extra = {
            "example": [
                {
                    "Customer ID": 12345,
                    "Customer Name": "John Doe",
                    "Periods": [
                        {"Period": "Jan 24", "exposure": 100000, "Rank": 1, "rating": "1"},
                        {"Period": "Feb 24", "exposure": 120000, "Rank": 1, "rating": "10"}
                    ]
                },
                {
                    "Customer ID": 67890,
                    "Customer Name": "Jane Smith",
                    "Periods": [
                        {"Period": "Jan 24", "exposure": 80000, "Rank": 2, "rating": "8"},
                        {"Period": "Feb 24", "exposure": 95000, "Rank": 2, "rating": "5"}
                    ]
                }
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
    "/ranked_data_by_period",
    response_model=RankedDataByPeriodResponse,
    responses={
        200: {
            "description": "Returns ranked data by period for a given fact field and dimension.",
            "content": {
                "application/json": {
                    "examples": {
                        "Without Filter": {
                            "summary": "Basic ranked data by period",
                            "value": [
                                {
                                    "Customer ID": 12345,
                                    "Customer Name": "John Doe",
                                    "Periods": [
                                        {"Period": "Jan 24", "exposure": 100000, "Rank": 1, "rating": "1"},
                                        {"Period": "Feb 24", "exposure": 120000, "Rank": 1, "rating": "10"}
                                    ]
                                },
                                {
                                    "Customer ID": 67890,
                                    "Customer Name": "Jane Smith",
                                    "Periods": [
                                        {"Period": "Jan 24", "exposure": 80000, "Rank": 2, "rating": "8"},
                                        {"Period": "Feb 24", "exposure": 95000, "Rank": 2, "rating": "5"}
                                    ]
                                }
                            ]
                        },
                        "With Dimension Filter": {
                            "summary": "Filtered by sector = Retail",
                            "value": [
                                {
                                    "Customer ID": 12345,
                                    "Customer Name": "John Doe",
                                    "sector": "Retail",
                                    "Periods": [
                                        {"Period": "Jan 24", "exposure": 100000, "Rank": 1, "rating": "1"},
                                        {"Period": "Feb 24", "exposure": 120000, "Rank": 1, "rating": "10"}
                                    ]
                                }
                            ]
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
                            {"loc": ["query", "fact_field"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "dimension_field_to_rank"], "msg": "field required", "type": "value_error.missing"},
                            {"loc": ["query", "fact_field"], "msg": "Invalid field name(s): Exposure Amt. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "dimension_field_to_rank"], "msg": "Invalid field name(s): Cust ID. Use lowercase with underscores", "type": "value_error.custom"},
                            {"loc": ["query", "date"], "msg": "invalid date format, expected 'dd/mm/yyyy'", "type": "value_error.date"},
                            {"loc": ["query", "period_type"], "msg": "unexpected value; allowed: 'M', 'Q'", "type": "value_error.enum"},
                            {"loc": ["query", "start_rank"], "msg": "value is not a valid integer", "type": "type_error.integer"},
                            {"loc": ["query", "end_rank"], "msg": "value is not a valid integer", "type": "type_error.integer"},
                            {"loc": ["query", "dimension_filter_field"], "msg": "Invalid field name(s): Segment Name. Use lowercase with underscores", "type": "value_error.custom"}
                        ]
                    }
                }
            }
        }
    },
    summary="Get Ranked Data by Period",
    description="Returns ranked data for a given fact field, dimension, and periods (monthly or quarterly) with optional filters."
)
def ranked_data_by_period(
    fact_field: str = Query(..., description="Fact field to aggregate (e.g., exposure)"),
    dimension_field_to_rank: str = Query(..., description="Dimension field to rank by (e.g., 'cust_id')"),
    date: str = Query(..., description="Date (dd/mm/yyyy) to calculate the trend from"),
    start_rank: int = Query(1, ge=1, description="Start rank for the period"),
    end_rank: int = Query(10, ge=1, description="End rank for the period"),
    period_type: str = Query('Q', description="'M' for monthly, 'Q' for quarterly"),
    lookback: int = Query(5, ge=1, description="Lookback range (number of periods to look back)"),
    dimension_field: str = Query('rating', description="Dimension field to include (e.g., rating)"),
    dimension_filter_field: Optional[str] = Query(None, description="Field to filter by (optional)"),
    dimension_filter_value: Optional[str] = Query(None, description="Value to filter by (optional)")
):
    try:
        validate_field_names([fact_field], "fact_field")
        validate_field_names([dimension_field_to_rank], "dimension_field_to_rank")
        validate_field_names([dimension_field], "dimension_field")
        if dimension_filter_field:
            validate_field_names([dimension_filter_field], "dimension_filter_field")

        validate_period_type(period_type)

        if not re.match(r"^\d{2}/\d{2}/\d{4}$", date):
            raise HTTPException(
                status_code=422,
                detail=[{
                    "loc": ["query", "date"],
                    "msg": "invalid date format, expected 'dd/mm/yyyy'",
                    "type": "value_error.date"
                }]
            )

        result = risk_model.get_ranked_data_by_period(
            fact_field=fact_field,
            dimension_field_to_rank=dimension_field_to_rank,
            date=date,
            start_rank=start_rank,
            end_rank=end_rank,
            period_type=period_type,
            lookback=lookback,
            dimension_field=dimension_field,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value
        )

        if isinstance(result, dict) and "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", port=8000, reload=True)
