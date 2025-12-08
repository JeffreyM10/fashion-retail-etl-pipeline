import pandas as pd

def clean_fashion_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply basic cleaning for the fashion sales dataset:
      - strip whitespace from string columns
      - standardize payment_method values
      - normalize item names
    """
    df = df.copy()

    # 1) Strip whitespace from all string-like columns
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in str_cols:
        df[col] = df[col].astype("string").str.strip()

    # 2) Normalize payment method values
    if "payment method" in df.columns:
        pm = (
            df["payment method"]
            .astype("string")
            .str.strip()
            .str.lower()
        )
        df["payment method"] = pm.map({
            "cash": "Cash",
            "credit card": "Credit Card",
        }).fillna(df["payment method"])

    # 3) Normalize item names
    if "item purchased" in df.columns:
        df["item purchased"] = (
            df["item purchased"]
            .astype("string")
            .str.strip()
            .str.title()
        )

    return df
