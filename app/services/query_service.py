import os
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime
from dateutil.relativedelta import relativedelta

class RiskDataModelService:
    def __init__(self, data_folder: str):
        self.data = self._load_all_excels(data_folder)
        self.model = self._init_model(self.data)

    def _load_all_excels(self, path: str) -> Dict[str, pd.DataFrame]:
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
                    df_fact.columns = [str(c).strip().lower().replace(" ", "_") for c in df_fact.columns]
                    if "date" in df_fact.columns:
                        df_fact["date"] = pd.to_datetime(df_fact["date"], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y')
                    for col in df_fact.columns:
                        if not pd.api.types.is_numeric_dtype(df_fact[col]):
                            df_fact[col] = df_fact[col].astype("object")
                    df_fact["source_file"] = filename
                    all_data["fact_risk"].append(df_fact)

                # Load customer once
                if not customer_loaded and "CUSTOMER" in xls.sheet_names:
                    df_cust = xls.parse("CUSTOMER")
                    df_cust.columns = [str(c).strip().lower().replace(" ", "_") for c in df_cust.columns]
                    customer_df = df_cust
                    customer_loaded = True

        merged = {
            "fact_risk": pd.concat(all_data["fact_risk"], ignore_index=True),
            "customer": customer_df
        }
        return merged

    def _init_model(self, data: Dict[str, pd.DataFrame]):
        # inline your RiskDataModel class here, or import if you split
        from app.services.query_service_impl import RiskDataModel
        return RiskDataModel(data)

    def get_distinct_values(self, column: str) -> List[Any]:
        return self.model.get_distinct_values(column)

    def get_sum_by_dimension(
        self,
        fact_fields: List[str],
        group_by_fields: Optional[List[str]] = None,
        date_filter: Optional[str] = None,
        dimension_filter_field: Optional[str] = None,
        dimension_filter_value: Optional[str] = None,
    ) -> Any:
        return self.model.get_sum_by_dimension(
            fact_fields, group_by_fields, date_filter,
            dimension_filter_field, dimension_filter_value
        )

