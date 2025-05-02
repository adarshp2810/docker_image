import os
import pandas as pd
from typing import Dict, Any, List, Optional
from dateutil.relativedelta import relativedelta
from datetime import datetime

def load_all_excels(path: str, normalize_cols=True) -> Dict[str, pd.DataFrame]:
    all_data = {"fact_risk": [], "rating": []}
    customer_loaded = False
    customer_df = None

    for filename in os.listdir(path):
        if filename.endswith(".xlsx") and not filename.startswith("~$"):
            file_path = os.path.join(path, filename)
            xls = pd.ExcelFile(file_path)

            # Load fact risk
            if "fact risk" in xls.sheet_names:
                df_fact = xls.parse("fact risk")
                if normalize_cols:
                    df_fact.columns = [str(c).strip().lower().replace(" ", "_") for c in df_fact.columns]
                if "date" in df_fact.columns:
                    df_fact["date"] = pd.to_datetime(df_fact["date"], errors="coerce", dayfirst=True).dt.strftime('%d/%m/%Y')
                for col in df_fact.columns:
                    if not pd.api.types.is_numeric_dtype(df_fact[col]):
                        df_fact[col] = df_fact[col].astype("object")
                df_fact["source_file"] = filename
                all_data["fact_risk"].append(df_fact)

            # Load customer once
            if not customer_loaded and "CUSTOMER" in xls.sheet_names:
                df_cust = xls.parse("CUSTOMER")
                if normalize_cols:
                    df_cust.columns = [str(c).strip().lower().replace(" ", "_") for c in df_cust.columns]
                customer_df = df_cust
                customer_loaded = True

    return {
        "fact_risk": pd.concat(all_data["fact_risk"], ignore_index=True),
        "customer": customer_df
    }

class RiskDataModel:
    def __init__(self, dataframes: dict):
        self.df_fact_risk = dataframes.get("fact_risk")
        self.df_customer = dataframes.get("customer")
        if "cust_name" in self.df_customer.columns:
            self.df_customer = self.df_customer.drop(columns=["cust_name"])
        self._join_data()

    def _join_data(self):
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
        df = self.df_joined.copy()
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' not found in the dataset.")
        vals = df[column_name].dropna().unique()
        try:
            return sorted(vals)
        except:
            return list(vals)

    def get_sum_by_dimension(self, fact_fields, group_by_fields=None,
                             date_filter=None, dimension_filter_field=None,
                             dimension_filter_value=None):
        df = self.df_joined.copy()
        if date_filter:
            df = df[df["date"] == date_filter]
        if dimension_filter_field and dimension_filter_value:
            df = df[df[dimension_filter_field] == dimension_filter_value]
        numerical = [f for f in fact_fields if pd.api.types.is_numeric_dtype(df[f])]
        if group_by_fields:
            agg = df.groupby(group_by_fields)[numerical].sum().reset_index()
            agg[numerical] = agg[numerical].round(0)
            result = agg.to_dict(orient="records")
        else:
            s = df[numerical].sum().round(0).astype(int)
            result = s.to_dict()
        return result

# instantiate for routers to import
dfs = load_all_excels("./Sample_Bank_Data")
risk_model = RiskDataModel(dfs)
