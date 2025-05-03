import os
import re
import calendar
import logging
from enum import Enum
from datetime import datetime, date
from typing import List, Optional

import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel
from dateutil.relativedelta import relativedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def load_data(folder: str, normalize_cols=True):
    """
    Read all .xlsx in folder, return:
      - customer_df
      - fact_df
      - rl_df (None if no Risk Limit sheets)
    """
    if not os.path.exists(folder):
        raise FileNotFoundError(f"Data folder '{folder}' does not exist. Please ensure 'Sample_Bank_Data' is in the repository root.")
    
    all_data = {"fact_risk": [], "customer": [], "risk_limit": []}
    customer_loaded = False

    for filename in os.listdir(folder):
        if filename.endswith(".xlsx") and not filename.startswith("~$"):
            file_path = os.path.join(folder, filename)
            try:
                eff_date = parse_effective_date(file_path)
                xls = pd.ExcelFile(file_path)
                logger.info(f"Processing file: {filename} with effective date {eff_date}")

                # Load fact risk
                if "fact risk" in xls.sheet_names:
                    df_fact = xls.parse("fact risk")
                    if normalize_cols:
                        df_fact = clean_column_names(df_fact)
                    if "date" in df_fact.columns:
                        df_fact["date"] = pd.to_datetime(df_fact["date"], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y')
                    for col in df_fact.columns:
                        if not pd.api.types.is_numeric_dtype(df_fact[col]):
                            df_fact[col] = df_fact[col].astype("object")
                    df_fact["source_file"] = filename
                    all_data["fact_risk"].append(df_fact)

                # Load customer only once
                if not customer_loaded and "CUSTOMER" in xls.sheet_names:
                    df_cust = xls.parse("CUSTOMER")
                    if normalize_cols:
                        df_cust = clean_column_names(df_cust)
                    all_data["customer"].append(df_cust)
                    customer_loaded = True

                # Load Risk Limit
                if "Risk Limit" in xls.sheet_names:
                    rl = xls.parse("Risk Limit")
                    if normalize_cols:
                        rl = clean_column_names(rl)
                    rl['effective_date'] = eff_date.strftime('%d/%m/%Y')
                    all_data["risk_limit"].append(rl)

            except Exception as e:
                logger.error(f"Error loading {filename}: {e}")

    merged_data = {
        "fact_risk": pd.concat(all_data["fact_risk"], ignore_index=True) if all_data["fact_risk"] else None,
        "customer": pd.concat(all_data["customer"], ignore_index=True).drop_duplicates(subset=['cust_id']) if all_data["customer"] else None,
        "risk_limit": pd.concat(all_data["risk_limit"], ignore_index=True) if all_data["risk_limit"] else None
    }
    
    # Log loaded data summary
    if merged_data["fact_risk"] is not None:
        logger.info(f"Fact risk data loaded with {len(merged_data['fact_risk'])} rows, dates: {merged_data['fact_risk']['date'].unique()}")
    if merged_data["risk_limit"] is not None:
        logger.info(f"Risk limit data loaded with {len(merged_data['risk_limit'])} rows, dates: {merged_data['risk_limit']['effective_date'].unique()}")
    
    return merged_data["customer"], merged_data["fact_risk"], merged_data["risk_limit"]

# ─── Risk Data Model ───────────────────────────────────────────────────────────
class RiskDataModel:
    def __init__(self, customer_df, fact_df, rl_df):
        self.df_fact_risk = fact_df
        self.df_customer = customer_df
        self.rl_df = rl_df

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

    # Commented-out methods
    def get_top_n_sum(self, fact_fields, group_by_fields, date_filter=None, top_n=5):
        df = self.df_joined.copy()

        if date_filter:
            df = df[df["date"] == date_filter]

        numerical_fields = [field for field in fact_fields if pd.api.types.is_numeric_dtype(df[field])]
        categorical_fields = [field for field in fact_fields if not pd.api.types.is_numeric_dtype(df[field])]

        fact_field_to_rank = fact_fields[0] if isinstance(fact_fields, list) else fact_fields
        df["sum_fact"] = df[fact_field_to_rank]

        grouped = df.groupby(group_by_fields).agg({
            "sum_fact": "max",
            **{field: "max" for field in numerical_fields if field != fact_field_to_rank}
        }).reset_index()

        top = grouped.sort_values("sum_fact", ascending=False).head(top_n)
        top.rename(columns={"sum_fact": fact_field_to_rank}, inplace=True)

        other_fields = [f for f in fact_fields if f != fact_field_to_rank]

        for idx, row in top.iterrows():
            customer = row[group_by_fields[0]]
            max_fact_value = row[fact_field_to_rank]

            matching_row = df[(df[group_by_fields[0]] == customer) & (df[fact_field_to_rank] == max_fact_value)]

            for field in other_fields:
                if pd.api.types.is_numeric_dtype(df[field]):
                    top.at[idx, field] = round(matching_row[field].sum(), 0)
                else:
                    top.at[idx, field] = matching_row[field].iloc[0]

        return top

    def get_top_n_trend_by_period(self, fact_field, dimension, date_filter, top_n=10, period_type="M", lookback=5, dimension_filter_field=None, dimension_filter_value=None, attribute_field=None):
        df = self.df_joined.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)

        selected_date = pd.to_datetime(date_filter, dayfirst=True)
        selected_period = selected_date.to_period(period_type)

        df["period"] = df["date"].dt.to_period(period_type)
        df["month_year"] = df["period"].dt.strftime('%b %y')

        period_list = [(selected_date - relativedelta(months=i if period_type == "M" else i*3)).to_period(period_type) for i in range(lookback + 1)]
        period_strs = [p.strftime('%b %y') for p in period_list]

        if dimension_filter_field and dimension_filter_value:
            df = df[df[dimension_filter_field] == dimension_filter_value]
            top_dimensions = df[dimension].unique().tolist()
        else:
            df_selected_period = df[df["period"] == selected_period]
            top_dimensions = (
                df_selected_period.groupby(dimension)[fact_field]
                .sum()
                .nlargest(top_n)
                .index
                .tolist()
            )
            df = df[df[dimension].isin(top_dimensions)]

        df = df[df["period"].isin(period_list)]

        cust_id_to_name = (
            self.df_joined[["cust_id", "cust_name"]]
            .drop_duplicates()
            .set_index("cust_id")["cust_name"]
            .to_dict()
        )

        output = []

        for cust in top_dimensions:
            cust_row = {"cust_name": cust_id_to_name.get(cust, f"ID:{cust}")}

            for p in period_list:
                p_str = p.strftime('%b %y')
                df_p = df[(df["period"] == p)]

                if not df_p.empty:
                    cust_value = df_p[df_p[dimension] == cust][fact_field].sum()

                    if cust_value != 0:
                        cust_row[f"{p_str}_Exposure"] = round(cust_value, 0)

                        ranks = (
                            df_p.groupby(dimension)[fact_field]
                            .sum()
                            .rank(method="min", ascending=False)
                        )
                        cust_row[f"{p_str}_Rank"] = int(ranks.get(cust, np.nan))

                        if attribute_field and attribute_field in df_p.columns:
                            matching_attr = df_p[df_p[dimension] == cust][attribute_field]
                            cust_row[f"{p_str}_Rating"] = matching_attr.iloc[0] if not matching_attr.empty else None
                        else:
                            cust_row[f"{p_str}_Rating"] = None
                    else:
                        cust_row[f"{p_str}_Exposure"] = 0
                        cust_row[f"{p_str}_Rank"] = None
                        cust_row[f"{p_str}_Rating"] = None
                else:
                    cust_row[f"{p_str}_Exposure"] = 0
                    cust_row[f"{p_str}_Rank"] = None
                    cust_row[f"{p_str}_Rating"] = None

            output.append(cust_row)

        ordered_output = []
        for row in output:
            ordered_row = {"cust_name": row["cust_name"]}
            for p in period_list:
                p_str = p.strftime('%b %y')
                ordered_row[f"{p_str}_Rating"] = row.get(f"{p_str}_Rating")
                ordered_row[f"{p_str}_Exposure"] = row.get(f"{p_str}_Exposure")
                ordered_row[f"{p_str}_Rank"] = row.get(f"{p_str}_Rank")
            ordered_output.append(ordered_row)

        final_output = []
        for row in ordered_output:
            clean_row = {}
            for k, v in row.items():
                if isinstance(v, (np.integer, np.int64, np.int32)):
                    clean_row[k] = int(v)
                elif isinstance(v, (np.floating, np.float64, np.float32)):
                    clean_row[k] = float(v)
                elif isinstance(v, np.bool_):
                    clean_row[k] = bool(v)
                else:
                    clean_row[k] = v
            final_output.append(clean_row)

        return final_output

# ─── Breach Calculation ────────────────────────────────────────────────────────
def calculate_breaches(requested_date: date, page: int, size: int, customer_df, fact_df, rl_df):
    if fact_df is None or customer_df is None:
        raise HTTPException(400, "Missing required data")
    if rl_df is None:
        raise HTTPException(400, "Risk Limit data required for breach calculations")

    # Convert requested_date to string format for comparison
    date_str = requested_date.strftime('%d/%m/%Y')
    logger.info(f"Calculating breaches for date: {date_str}")

    # Filter exposures
    exposures = fact_df[fact_df['date'] == date_str].copy()
    if exposures.empty:
        available_dates = fact_df['date'].unique().tolist() if fact_df is not None else []
        logger.warning(f"No exposure data found for {date_str}. Available dates: {available_dates}")
        raise HTTPException(
            404,
            f"No exposures for date {date_str}. Available dates: {', '.join(available_dates) or 'none'}"
        )

    # Filter risk limits
    limits = rl_df[rl_df['effective_date'] == date_str].copy()
    if limits.empty:
        available_dates = rl_df['effective_date'].unique().tolist() if rl_df is not None else []
        logger.warning(f"No risk limit data found for {date_str}. Available dates: {available_dates}")
        raise HTTPException(
            404,
            f"No risk limits for date {date_str}. Available dates: {', '.join(available_dates) or 'none'}"
        )

    # Prepare limit data
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

    # Merge exposures with customer data
    exposures = exposures.drop(columns=['cust_name', 'group'], errors='ignore')
    exposures = exposures.merge(customer_df[['cust_id', 'cust_name', 'sector', 'group_id']], on='cust_id', how='left')

    # Customer-level breaches
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

    # Sector-level breaches
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

    # Group-level breaches
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

# ─── FastAPI App ──────────────────────────────────────────────────────────────
app = FastAPI(title="Unified Risk API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# OpenAPI Customization
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

# Load data once at startup
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FOLDER = os.path.join(SCRIPT_DIR, "Sample_Bank_Data")
logger.info(f"Looking for data in: {DATA_FOLDER}")
customer_df, fact_df, rl_df = load_data(DATA_FOLDER)
risk_model = RiskDataModel(customer_df, fact_df, rl_df)

# Breaches Endpoint
@app.get("/breaches", response_model=BreachesResponse)
def get_breaches(
    date: str = Query(..., description="DD/MM/YYYY"),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1),
    level: Optional[BreachLevel] = Query(None, description="Filter by breach level: customer, sector, or group")
):
    try:
        req_date = datetime.strptime(date, "%d/%m/%Y").date()
    except ValueError:
        raise HTTPException(400, "Date must be DD/MM/YYYY")

    try:
        full = calculate_breaches(req_date, page, size, customer_df, fact_df, rl_df)
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
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error processing breaches for date {date}: {str(e)}")
        raise HTTPException(500, f"Internal server error: {str(e)}")

# Analytics Endpoints
@app.get("/api/distinct_values")
def get_distinct_values(column: str = Query(..., description="Field name like 'staging', 'date', 'cust_name'")):
    try:
        result = risk_model.get_distinct_values(column_name=column)
        return {column: result}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/sum_by_dimension")
def get_sum_by_dimension(
    fact_fields: str = Query(..., description="Comma-separated list of fact fields to aggregate, e.g., 'exposure,provision'"),
    group_by_fields: str = Query(None, description="Comma-separated list of fields to group by, e.g., 'cust_id'"),
    date_filter: Optional[str] = Query(None, description="Date in dd/mm/yyyy format"),
    dimension_filter_field: Optional[str] = Query(None, description="Field name to filter the data by, e.g., 'sector'"),
    dimension_filter_value: Optional[str] = Query(None, description="Value of the dimension field to filter by, e.g., 'finance'")
):
    try:
        fact_fields = [field.strip() for field in fact_fields.split(',')]
        group_by_fields = [field.strip() for field in group_by_fields.split(',')] if group_by_fields else None
        result = risk_model.get_sum_by_dimension(
            fact_fields=fact_fields,
            group_by_fields=group_by_fields,
            date_filter=date_filter,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value
        )
        return result
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/avg_by_dimension")
def get_avg_by_dimension(
    fact_fields: str = Query(..., description="Comma-separated list of fact fields to calculate average for, e.g., 'exposure,provision'"),
    group_by_fields: str = Query(None, description="Comma-separated list of fields to group by, e.g., 'cust_id'"),
    date_filter: Optional[str] = Query(None, description="Date in dd/mm/yyyy format"),
    dimension_filter_field: Optional[str] = Query(None, description="Field name to filter the data by, e.g., 'sector'"),
    dimension_filter_value: Optional[str] = Query(None, description="Value of the dimension field to filter by, e.g., 'finance'")
):
    try:
        fact_fields = [field.strip() for field in fact_fields.split(',')]
        group_by_fields = [field.strip() for field in group_by_fields.split(',')] if group_by_fields else None
        result = risk_model.get_avg_by_dimension(
            fact_fields=fact_fields,
            group_by_fields=group_by_fields,
            date_filter=date_filter,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value
        )
        return result
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/count_distinct")
def count_distinct_values(
    dimension: str = Query(..., description="Dimension field name, e.g., 'cust_id'"),
    date_filter: str = Query(None, description="Date in dd/mm/yyyy format"),
    dimension_filter_field: str = Query(None, description="Field name to filter by, e.g., 'sector'"),
    dimension_filter_value: str = Query(None, description="Value of the filter field to filter by")
):
    try:
        result = risk_model.count_distinct(dimension, date_filter, dimension_filter_field, dimension_filter_value)
        return result
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/get_concentration")
def get_concentration(
    fact_fields: str = Query(..., description="Comma-separated fact fields to be used for concentration calculation"),
    group_by_fields: Optional[str] = Query(None, description="Comma-separated group by fields"),
    date_filter: str = Query(None, description="Date in dd/mm/yyyy format"),
    top_n: int = Query(10, description="Top N entities to be considered"),
    dimension_filter_field: str = Query(None, description="Dimension field to filter by"),
    dimension_filter_value: str = Query(None, description="Value for the dimension field filter")
):
    try:
        fact_fields_list = fact_fields.split(",")
        group_by_fields_list = group_by_fields.split(",") if group_by_fields else None
        result = risk_model.get_concentration(
            fact_fields=fact_fields_list,
            group_by_fields=group_by_fields_list,
            date_filter=date_filter,
            top_n=top_n,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value
        )
        return result
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/portfolio_trend")
def portfolio_trend(
    fact_fields: str = Query(..., description="Fact fields to aggregate, e.g. exposure,direct_exposure"),
    date_filter: str = Query(..., description="End date in dd/mm/yyyy format"),
    period_type: str = Query("M", description="M for Month, Q for Quarter"),
    lookback: int = Query(5, description="Number of past periods to include")
):
    try:
        fact_field_list = [field.strip() for field in fact_fields.split(",") if field.strip()]
        result = risk_model.get_portfolio_trend_summary(
            fact_fields=fact_field_list,
            date_filter=date_filter,
            period_type=period_type,
            lookback=lookback
        )
        return result
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/segment_distribution")
def segment_distribution(
    fact_field: str = Query(..., description="Field to aggregate (e.g., exposure)"),
    dimension_field: str = Query(..., description="Field to rank by (e.g., cust_id)"),
    date_filter: Optional[str] = Query(None, description="Date filter in dd/mm/yyyy format"),
    start: int = Query(1, description="Start rank for top N"),
    end: Optional[int] = Query(20, description="End rank for top N (optional)"),
    interval: Optional[int] = Query(10, description="Interval for grouping (optional)"),
    others: bool = Query(True, description="Group remaining entities as 'Others'"),
    dimension_filter_field: Optional[str] = Query(None, description="Field to filter by (optional)"),
    dimension_filter_value: Optional[str] = Query(None, description="Value to filter by (optional)")
):
    try:
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
        return result
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/ranked_entities_with_others")
def get_ranked_entities_with_others(
    fact_field: str,
    dimension_field: str,
    date_filter: Optional[str] = None,
    start: int = 1,
    end: Optional[int] = 10,
    others_option: bool = False,
    dimension_filter_field: Optional[str] = None,
    dimension_filter_value: Optional[str] = None
):
    try:
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
        return result
    except Exception as e:
        return {"error": str(e)}

# Commented-out endpoints
@app.get("/api/top_n_sum")
def top_n_sum(
    fact_fields: str = Query(...),
    group_by_fields: str = Query(...),
    top_n: int = 5,
    date_filter: Optional[str] = None
):
    fact_fields_list = [field.strip() for field in fact_fields.split(',')]
    group_by_fields_list = [field.strip() for field in group_by_fields.split(',')]
    result = risk_model.get_top_n_sum(
        fact_fields_list, group_by_fields_list, date_filter, top_n
    )
    return result.to_dict(orient="records")

@app.get("/api/top_n_trend")
def top_n_trend(
    fact_field: str = Query(..., description="Fact field to aggregate (e.g., exposure)"),
    dimension: str = Query(..., description="Field to find top N or full list (e.g., cust_id)"),
    date_filter: str = Query(..., description="Reference date (dd/mm/yyyy)"),
    top_n: int = Query(10, description="Top N to fetch (ignored if dimension filter given)"),
    period_type: str = Query("M", description="M for Month, Q for Quarter"),
    lookback: int = Query(5, description="Periods to go back"),
    dimension_filter_field: Optional[str] = Query(None, description="Optional dimension filter field (e.g., sector)"),
    dimension_filter_value: Optional[str] = Query(None, description="Optional dimension filter value (e.g., Banking)"),
    attribute_field: Optional[str] = Query(None, description="Optional attribute field to display (e.g., rating)")
):
    try:
        result = risk_model.get_top_n_trend_by_period(
            fact_field=fact_field,
            dimension=dimension,
            date_filter=date_filter,
            top_n=top_n,
            period_type=period_type,
            lookback=lookback,
            dimension_filter_field=dimension_filter_field,
            dimension_filter_value=dimension_filter_value,
            attribute_field=attribute_field
        )
        return result
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", port=8000, reload=True)
