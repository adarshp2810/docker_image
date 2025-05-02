from fastapi import FastAPI, Query
from typing import List, Optional
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from datetime import datetime
import os

app = FastAPI(title="Risk Query API")

# -------------------------------
# Load and Merge Excel Files
# -------------------------------
def load_all_excels(path: str, normalize_cols=True):
    all_data = {"fact_risk": [], "rating": []}
    customer_loaded = False
    customer_df = None

    for filename in os.listdir(path):
        if filename.endswith(".xlsx") and not filename.startswith("~$"):
            file_path = os.path.join(path, filename)
            try:
                xls = pd.ExcelFile(file_path)

                # Load fact risk from all files
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

                # Load customer only once
                if not customer_loaded and "CUSTOMER" in xls.sheet_names:
                    df_cust = xls.parse("CUSTOMER")
                    if normalize_cols:
                        df_cust.columns = [str(c).strip().lower().replace(" ", "_") for c in df_cust.columns]
                    customer_df = df_cust
                    customer_loaded = True

            except Exception as e:
                print(f"Error loading {filename}: {e}")
    
    merged_data = {
        "fact_risk": pd.concat(all_data["fact_risk"], ignore_index=True),
        "customer": customer_df
    }
    return merged_data

data_folder = './Sample_Bank_Data'  
dfs = load_all_excels(data_folder)

# -------------------------------
# Risk Data Model Class
# -------------------------------
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

        distinct_vals = df[column_name].dropna().unique()

        # Sort if values are comparable (like strings, dates)
        try:
            distinct_vals = sorted(distinct_vals)
        except Exception:
            distinct_vals = list(distinct_vals)

        return distinct_vals
    
    # def get_top_n_sum(self, fact_fields, group_by_fields, date_filter=None, top_n=5):
    #     df = self.df_joined.copy()

    #     if date_filter:
    #         df = df[df["date"] == date_filter]

    #     numerical_fields = [field for field in fact_fields if pd.api.types.is_numeric_dtype(df[field])]
    #     categorical_fields = [field for field in fact_fields if not pd.api.types.is_numeric_dtype(df[field])]

    #     # Rank by the selected fact field 
    #     fact_field_to_rank = fact_fields[0] if isinstance(fact_fields, list) else fact_fields
    #     df["sum_fact"] = df[fact_field_to_rank]  

    #     # Group by the selected dimension(s)
    #     grouped = df.groupby(group_by_fields).agg({
    #         "sum_fact": "max",  
    #         **{field: "max" for field in numerical_fields if field != fact_field_to_rank}  
    #     }).reset_index()

    #     top = grouped.sort_values("sum_fact", ascending=False).head(top_n)
    #     top.rename(columns={"sum_fact": fact_field_to_rank}, inplace=True)

    #     other_fields = [f for f in fact_fields if f != fact_field_to_rank]

    #     for idx, row in top.iterrows():
    #         customer = row[group_by_fields[0]]  
    #         max_fact_value = row[fact_field_to_rank]  

    #         matching_row = df[(df[group_by_fields[0]] == customer) & (df[fact_field_to_rank] == max_fact_value)]

    #         # For each numerical or categorical field, align the value with the selected fact field
    #         for field in other_fields:
    #             if pd.api.types.is_numeric_dtype(df[field]):
    #                 top.at[idx, field] = round(matching_row[field].sum(), 0)
    #             else:
    #                 top.at[idx, field] = matching_row[field].iloc[0]  

    #     return top 
    
    def get_sum_by_dimension(self, fact_fields, group_by_fields=None, date_filter=None, dimension_filter_field=None, dimension_filter_value=None):
        df = self.df_joined.copy()

        # Apply date filter if provided
        if date_filter:
            df = df[df["date"] == date_filter]

        # Apply dimension filter if provided
        if dimension_filter_field and dimension_filter_value:
            if dimension_filter_field not in df.columns:
                return {"error": f"Column '{dimension_filter_field}' not found in the dataset."}
            df = df[df[dimension_filter_field] == dimension_filter_value]

        # Identify numerical fact fields
        numerical_fields = [f for f in fact_fields if pd.api.types.is_numeric_dtype(df[f])]

        # Prepare the output result
        result = []

        # Perform aggregation if group_by_fields are provided
        if group_by_fields:
            agg_df = df.groupby(group_by_fields)[numerical_fields].sum().reset_index()
            for field in numerical_fields:
                agg_df[field] = round(agg_df[field], 0)
            result = agg_df.to_dict(orient="records")

            if dimension_filter_field and dimension_filter_value:
                result.insert(0, {dimension_filter_field: dimension_filter_value})
            
        else:
            # If no group_by_fields are provided, return the sum of all fact fields
            sum_series = df[numerical_fields].sum()
            sum_series = sum_series.round(0).astype(int)
            result = sum_series.to_dict()

            # If dimension filter is provided, add it as the first element in the result
            if dimension_filter_field and dimension_filter_value:
                result = {dimension_filter_field: dimension_filter_value, **result}
            

        return result

    def get_avg_by_dimension(self, fact_fields, group_by_fields=None, date_filter=None, dimension_filter_field=None, dimension_filter_value=None):
        df = self.df_joined.copy()

        # Apply date filter if provided
        if date_filter:
            df = df[df["date"] == date_filter]

        # Apply dimension filter if provided
        if dimension_filter_field and dimension_filter_value:
            if dimension_filter_field not in df.columns:
                return {"error": f"Column '{dimension_filter_field}' not found in the dataset."}
            df = df[df[dimension_filter_field] == dimension_filter_value]

        numerical_fields = [f for f in fact_fields if pd.api.types.is_numeric_dtype(df[f])]

        # Prepare the output result
        result = []

        # Perform aggregation if group_by_fields are provided
        if group_by_fields:
            agg_df = df.groupby(group_by_fields)[numerical_fields].mean().reset_index()
            for field in numerical_fields:
                agg_df[field] = round(agg_df[field], 0)
            result = agg_df.to_dict(orient="records")

            # If dimension filter is provided, add it as the first element in the result list
            if dimension_filter_field and dimension_filter_value:
                result.insert(0, {dimension_filter_field: dimension_filter_value})

        else:
            # If no group_by_fields are provided, return the average of all fact fields
            avg_series = df[numerical_fields].mean()
            avg_series = avg_series.round(0).astype(int)
            result = avg_series.to_dict()

            # If dimension filter is provided, add it as the first element in the result
            if dimension_filter_field and dimension_filter_value:
                result = {dimension_filter_field: dimension_filter_value, **result}

        return result
    
    def count_distinct(self, dimension, date_filter=None, dimension_filter_field=None, dimension_filter_value=None):
        df = self.df_joined.copy()

        if date_filter:
            df = df[df["date"] == date_filter]

        # Apply dimension filter if provided
        if dimension_filter_field and dimension_filter_value:
            if dimension_filter_field not in df.columns:
                return {"error": f"Column {dimension_filter_field} not found in the dataset."}
            df = df[df[dimension_filter_field] == dimension_filter_value]

        distinct_count = df[dimension].dropna().nunique()

        result = {"count": distinct_count}

        # If dimension filter field and value are provided, add them to the result as the first element
        if dimension_filter_field and dimension_filter_value:
            result = {dimension_filter_field: dimension_filter_value, **result}

        return result
  
    def get_concentration(self, fact_fields, group_by_fields=None, date_filter=None, top_n=10, dimension_filter_field=None, dimension_filter_value=None):
        df = self.df_joined.copy()

        if date_filter:
            df = df[df["date"] == date_filter]
        
        if dimension_filter_field and dimension_filter_value:
            if dimension_filter_field not in df.columns:
                return {"error": f"Column {dimension_filter_field} not found in the dataset."}
            df = df[df[dimension_filter_field] == dimension_filter_value]

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
        "concentration_percentage": round(concentration, 0)
        }

        if dimension_filter_field and dimension_filter_value:
            result = {dimension_filter_field: dimension_filter_value, **result}

        return result
    
    # def get_top_n_trend_by_period(self, fact_field, dimension, date_filter, top_n=10, period_type="M", lookback=5):
    #     df = self.df_joined.copy()
    #     df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)

    #     # Parse and convert date
    #     selected_date = pd.to_datetime(date_filter, dayfirst=True)
    #     selected_period = selected_date.to_period(period_type)

    #     # Generate the list of periods: selected + previous
    #     period_list = [(selected_date - relativedelta(months=i if period_type == "M" else i * 3)).to_period(period_type) for i in range(lookback + 1)]
    #     period_strs = [p.strftime('%b, %Y') for p in period_list]

    #     # Add period column to dataframe
    #     df["period"] = df["date"].dt.to_period(period_type)
    #     df["month_year"] = df["date"].dt.to_period(period_type).dt.strftime('%b, %Y')

    #     # Get Top N by fact field for selected period
    #     top_df = df[df["period"] == selected_period]
    #     top_dimensions = (
    #         top_df.groupby(dimension)[fact_field]
    #         .sum()
    #         .nlargest(top_n)
    #         .index
    #         .tolist()
    #     )

    #     # Filter for top dimensions and relevant periods
    #     df_filtered = df[df[dimension].isin(top_dimensions) & df["period"].isin(period_list)]

    #     # Pivot the data
    #     pivot_df = (
    #         df_filtered.pivot_table(
    #             index=dimension,
    #             columns="month_year",
    #             values=fact_field,
    #             aggfunc="sum"
    #         )
    #         .fillna(0)
    #     )

    #     # Keep only desired columns in correct order
    #     available_cols = [col for col in period_strs if col in pivot_df.columns]
    #     pivot_df = pivot_df[available_cols]
    #     pivot_df = pivot_df.round(0)
    #     if available_cols:
    #         pivot_df = pivot_df.sort_values(by=available_cols[0], ascending=False)

    #     cust_id_to_name = (
    #     df[["cust_id", "cust_name"]]
    #     .drop_duplicates()
    #     .set_index("cust_id")["cust_name"]
    #     .to_dict()
    #     )
    #     pivot_df.index = pivot_df.index.map(lambda cid: cust_id_to_name.get(cid, f"ID:{cid}"))
    #     pivot_df.index.name = "cust_name"
        
    #     return pivot_df.reset_index().to_dict(orient="records")

    # def get_top_n_trend_by_period(
    # self,
    # fact_field,
    # dimension,
    # date_filter,
    # top_n=10,
    # period_type="M",
    # lookback=5,
    # dimension_filter_field=None,
    # dimension_filter_value=None,
    # attribute_field=None
    # ):
    #     df = self.df_joined.copy()
    #     df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)

    #     selected_date = pd.to_datetime(date_filter, dayfirst=True)
    #     selected_period = selected_date.to_period(period_type)

    #     df["period"] = df["date"].dt.to_period(period_type)
    #     df["month_year"] = df["period"].dt.strftime('%b %y')

    #     period_list = [(selected_date - relativedelta(months=i if period_type == "M" else i*3)).to_period(period_type) for i in range(lookback + 1)]
    #     period_strs = [p.strftime('%b %y') for p in period_list]

    #     # Optional dimension filtering
    #     if dimension_filter_field and dimension_filter_value:
    #         df = df[df[dimension_filter_field] == dimension_filter_value]
    #         top_dimensions = df[dimension].unique().tolist()
    #     else:
    #         df_selected_period = df[df["period"] == selected_period]
    #         top_dimensions = (
    #             df_selected_period.groupby(dimension)[fact_field]
    #             .sum()
    #             .nlargest(top_n)
    #             .index
    #             .tolist()
    #         )
    #         df = df[df[dimension].isin(top_dimensions)]

    #     df = df[df["period"].isin(period_list)]

    #     cust_id_to_name = (
    #         self.df_joined[["cust_id", "cust_name"]]
    #         .drop_duplicates()
    #         .set_index("cust_id")["cust_name"]
    #         .to_dict()
    #     )

    #     output = []

    #     for cust in top_dimensions:
    #         cust_row = {"cust_name": cust_id_to_name.get(cust, f"ID:{cust}")}

    #         for p in period_list:
    #             p_str = p.strftime('%b %y')
    #             df_p = df[(df["period"] == p)]

    #             if not df_p.empty:
    #                 # Calculate fact field sum for customer
    #                 cust_value = df_p[df_p[dimension] == cust][fact_field].sum()

    #                 if cust_value != 0:
    #                     cust_row[f"{p_str}_Exposure"] = round(cust_value, 0)

    #                     # Calculate ranks correctly inside this month
    #                     ranks = (
    #                         df_p.groupby(dimension)[fact_field]
    #                         .sum()
    #                         .rank(method="min", ascending=False)
    #                     )
    #                     cust_row[f"{p_str}_Rank"] = int(ranks.get(cust, np.nan))

    #                     # Attribute (rating etc.)
    #                     if attribute_field and attribute_field in df_p.columns:
    #                         matching_attr = df_p[df_p[dimension] == cust][attribute_field]
    #                         cust_row[f"{p_str}_Rating"] = matching_attr.iloc[0] if not matching_attr.empty else None
    #                     else:
    #                         cust_row[f"{p_str}_Rating"] = None
    #                 else:
    #                     cust_row[f"{p_str}_Exposure"] = 0
    #                     cust_row[f"{p_str}_Rank"] = None
    #                     cust_row[f"{p_str}_Rating"] = None
    #             else:
    #                 cust_row[f"{p_str}_Exposure"] = 0
    #                 cust_row[f"{p_str}_Rank"] = None
    #                 cust_row[f"{p_str}_Rating"] = None

    #         output.append(cust_row)

    #     ordered_output = []
    #     for row in output:
    #         ordered_row = {"cust_name": row["cust_name"]}
    #         for p in period_list:
    #             p_str = p.strftime('%b %y')
    #             ordered_row[f"{p_str}_Rating"] = row.get(f"{p_str}_Rating")
    #             ordered_row[f"{p_str}_Exposure"] = row.get(f"{p_str}_Exposure")
    #             ordered_row[f"{p_str}_Rank"] = row.get(f"{p_str}_Rank")
    #         ordered_output.append(ordered_row)

    #     # Final cast numpy types to native Python
    #     final_output = []
    #     for row in ordered_output:
    #         clean_row = {}
    #         for k, v in row.items():
    #             if isinstance(v, (np.integer, np.int64, np.int32)):
    #                 clean_row[k] = int(v)
    #             elif isinstance(v, (np.floating, np.float64, np.float32)):
    #                 clean_row[k] = float(v)
    #             elif isinstance(v, np.bool_):
    #                 clean_row[k] = bool(v)
    #             else:
    #                 clean_row[k] = v
    #         final_output.append(clean_row)

    #     return final_output
  
    def get_portfolio_trend_summary(self, fact_fields, date_filter, period_type="M", lookback=5):
        df = self.df_joined.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)

        # Setup periods
        selected_date = pd.to_datetime(date_filter, dayfirst=True)
        df["period"] = df["date"].dt.to_period(period_type)
        df["period_str"] = df["period"].dt.strftime('%b, %Y')

        period_list = [(selected_date - relativedelta(months=i if period_type == "M" else i * 3)).to_period(period_type) for i in range(lookback + 1)]
        period_strs = [p.strftime('%b, %Y') for p in period_list]

        df = df[df["period"].isin(period_list)]

        # Create base output dictionary
        results = []

        for p in period_list:
            p_str = p.strftime('%b, %Y')
            df_p = df[df["period"] == p]

            # Aggregate numeric fact fields
            row = {
                "period": p_str
            }
            for field in fact_fields:
                if field in df_p.columns:
                    row[field] = round(df_p[field].sum(), 0)
                else:
                    row[field] = None

            # Average rating logic
            if "rating" in df_p.columns:
                avg_rating = df_p["rating"].mean()
                row["avg_rating_score"] = round(avg_rating, 1) if pd.notna(avg_rating) else None
            else:
                row["avg_rating_score"] = None

            # Total unique customers
            row["total_customers"] = df_p["cust_id"].nunique()

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
        dimension_filter_value: Optional[str] = None
    ):
        df = self.df_joined.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)

        # Apply date filter
        if date_filter:
            target_date = pd.to_datetime(date_filter, dayfirst=True)
            df = df[df["date"] == target_date]

        # Apply dimension filter if provided
        if dimension_filter_field and dimension_filter_value:
            if dimension_filter_field not in df.columns:
                return {"error": f"Column {dimension_filter_field} not found in the dataset."}
            df = df[df[dimension_filter_field] == dimension_filter_value]

        # Ensure valid fact_field and dimension_field
        if fact_field not in df.columns or dimension_field not in df.columns:
            return {"error": "Invalid input or no data for the given date."}

        # Rank by fact_field (e.g., exposure)
        df_ranked = (
            df.groupby(dimension_field)[fact_field]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )

        total_fact_field = df_ranked[fact_field].sum()

        # Apply segmentation logic based on start, end, and interval
        segments = []

        if dimension_filter_field and dimension_filter_value:
            segments.append({
                dimension_filter_field: dimension_filter_value
            })
        
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
                    "percentage": round(segment_percentage, 1)
                })
            
            # Handle the "Others" segment
            if others:
                others_df = df_ranked.iloc[end:]
                others_total = others_df[fact_field].sum()
                others_percentage = (others_total / total_fact_field) * 100
                segments.append({
                    "segment": "Others",
                    fact_field: int(others_total),
                    "percentage": round(others_percentage, 1)
                })

        else:
            # If no interval is provided, just use start and end for the segment
            segment_df = df_ranked.iloc[start - 1:end]
            segment_total = segment_df[fact_field].sum()
            segment_percentage = (segment_total / total_fact_field) * 100
            segments.append({
                "segment": f"Top {start}-{end}",
                fact_field: int(segment_total),
                "percentage": round(segment_percentage, 1)
            })
            
            # Handle the "Others" segment
            if others:
                others_df = df_ranked.iloc[end:]
                others_total = others_df[fact_field].sum()
                others_percentage = (others_total / total_fact_field) * 100
                segments.append({
                    "segment": "Others",
                    fact_field: int(others_total),
                    "percentage": round(others_percentage, 1)
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
    dimension_filter_value: Optional[str] = None
    ):
        df = self.df_joined.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)

        if date_filter:
            target_date = pd.to_datetime(date_filter, dayfirst=True)
            df = df[df["date"] == target_date]

        if dimension_filter_field and dimension_filter_value:
            if dimension_filter_field not in df.columns:
                return {"error": f"Column {dimension_filter_field} not found in the dataset."}
            df = df[df[dimension_filter_field] == dimension_filter_value]

        if df.empty or fact_field not in df.columns or dimension_field not in df.columns:
            return {"error": "Invalid input or no data for the given date."}

        # Rank entities by the provided fact field
        ranked_entities = (
            df.groupby(dimension_field)[fact_field]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )

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


risk_model = RiskDataModel(dfs)

# -------------------------------
# FastAPI Endpoints
# -------------------------------
@app.get("/api/distinct_values")
def get_distinct_values(
    column: str = Query(..., description="Field name like 'staging', 'date', 'cust_name'")
):
    try:
        result = risk_model.get_distinct_values(column_name=column)
        return {column: result}
    except Exception as e:
        return {"error": str(e)}

# @app.get("/api/top_n_sum")
# def top_n_sum(
#     fact_fields: str = Query(...),  
#     group_by_fields: str = Query(...),  
#     top_n: int = 5,  
#     date_filter: Optional[str] = None  
# ):
#     # Convert comma-separated string inputs into lists
#     fact_fields_list = [field.strip() for field in fact_fields.split(',')]
#     group_by_fields_list = [field.strip() for field in group_by_fields.split(',')]

#     result = risk_model.get_top_n_sum(
#         fact_fields_list, group_by_fields_list, date_filter, top_n
#     )
#     return result.to_dict(orient="records")

@app.get("/api/sum_by_dimension")
def get_sum_by_dimension(
    fact_fields: str = Query(..., description="Comma-separated list of fact fields to aggregate, e.g., 'exposure,provision'"),
    group_by_fields: str = Query(None, description="Comma-separated list of fields to group by, e.g., 'cust_id'"),
    date_filter: Optional[str] = Query(None, description="Date in dd/mm/yyyy format"),
    dimension_filter_field: Optional[str] = Query(None, description="Field name to filter the data by, e.g., 'sector'"),
    dimension_filter_value: Optional[str] = Query(None, description="Value of the dimension field to filter by, e.g., 'finance'")
):
    try:
        fact_fields = [field.strip() for field in fact_fields.split(',')]  # Parse fact_fields as a list
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
        fact_fields = [field.strip() for field in fact_fields.split(',')]  # Parse fact_fields as a list
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
    fact_fields: str = Query(..., description="Comma-separated fact fields to be used for concentration calculation."),
    group_by_fields: Optional[str] = Query(None, description="Comma-separated group by fields."),
    date_filter: str = Query(None, description="Date in dd/mm/yyyy format."),
    top_n: int = Query(10, description="Top N entities to be considered."),
    dimension_filter_field: str = Query(None, description="Dimension field to filter by."),
    dimension_filter_value: str = Query(None, description="Value for the dimension field filter.")
):
    try:
        # Convert comma-separated fact_fields and group_by_fields to lists
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


# @app.get("/api/top_n_trend")
# def top_n_trend(
#     fact_field: str = Query(..., description="Fact field to use, e.g., exposure"),
#     dimension: str = Query(..., description="Dimension to group by, e.g., cust_id"),
#     date_filter: str = Query(..., description="Date in dd/mm/yyyy format"),
#     top_n: int = Query(10, description="Top N dimensions to fetch"),
#     period_type: str = Query("M", description="M for Monthly, Q for Quarterly"),
#     lookback: int = Query(5, description="Number of previous periods to include")
# ):
#     try:
#         result = risk_model.get_top_n_trend_by_period(
#             fact_field=fact_field,
#             dimension=dimension,
#             date_filter=date_filter,
#             top_n=top_n,
#             period_type=period_type,
#             lookback=lookback
#         )
#         return result
#     except Exception as e:
#         return {"error": str(e)}

# @app.get("/api/top_n_trend")
# def top_n_trend(
#     fact_field: str = Query(..., description="Fact field to aggregate (e.g., exposure)"),
#     dimension: str = Query(..., description="Field to find top N or full list (e.g., cust_id)"),
#     date_filter: str = Query(..., description="Reference date (dd/mm/yyyy)"),
#     top_n: int = Query(10, description="Top N to fetch (ignored if dimension filter given)"),
#     period_type: str = Query("M", description="M for Month, Q for Quarter"),
#     lookback: int = Query(5, description="Periods to go back"),
#     dimension_filter_field: Optional[str] = Query(None, description="Optional dimension filter field (e.g., sector)"),
#     dimension_filter_value: Optional[str] = Query(None, description="Optional dimension filter value (e.g., Banking)"),
#     attribute_field: Optional[str] = Query(None, description="Optional attribute field to display (e.g., rating)")
# ):
#     try:
#         result = risk_model.get_top_n_trend_by_period(
#             fact_field=fact_field,
#             dimension=dimension,
#             date_filter=date_filter,
#             top_n=top_n,
#             period_type=period_type,
#             lookback=lookback,
#             dimension_filter_field=dimension_filter_field,
#             dimension_filter_value=dimension_filter_value,
#             attribute_field=attribute_field
#         )
#         return result
#     except Exception as e:
#         return {"error": str(e)}
    
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
