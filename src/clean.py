import pandas as pd


def clean_fashion_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply cleaning and standardization to the Fashion Retail dataset.

    - Strip whitespace from string-like columns
    - Normalize payment method values
    - Title-case item names
    - Drop duplicate rows based on a business key so UPSERT is safe
    """
    df = df.copy()

    # Normalize column names we expect to exist after validation
    # (these should already be present thanks to your validation step)
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    df[str_cols] = df[str_cols].apply(lambda col: col.astype("string").str.strip())

    # Normalize payment method to a consistent set of values
    if "payment method" in df.columns:
        df["payment method"] = (
            df["payment method"]
            .astype("string")
            .str.strip()
            .str.title()
        )
        # Optional strict mapping
        mapping = {
            "Cash": "Cash",
            "Credit Card": "Credit Card",
            "Creditcard": "Credit Card",
        }
        df["payment method"] = df["payment method"].map(mapping).fillna(df["payment method"])

    # Normalize item purchased for cleaner analytics (e.g., "Jeans", "Handbag")
    if "item purchased" in df.columns:
        df["item purchased"] = (
            df["item purchased"]
            .astype("string")
            .str.strip()
            .str.title()
        )

    # Deduplicate by a "business key" so the same customer-item-date
    # only appears once per batch (last one wins)
    key_cols = ["customer reference id", "item purchased", "date purchase"]
    existing_key_cols = [c for c in key_cols if c in df.columns]
    if len(existing_key_cols) == len(key_cols):
        df = df.drop_duplicates(subset=key_cols, keep="last")
    else:
        # Fallback: drop perfect duplicates if key columns aren't all present
        df = df.drop_duplicates(keep="last")

    return df
