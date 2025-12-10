# tests/test_clean.py

import pandas as pd
from src.clean import clean_fashion_sales



def test_clean_fashion_sales_strips_whitespace_and_normalizes_case():
    """
    - Leading/trailing spaces in string columns are removed
    - item purchased is title-cased
    - payment method is normalized and mapped
    """
    df = pd.DataFrame(
        {
            "customer reference id": [4018],
            "item purchased": ["  handbag "],
            "purchase amount (usd)": [4619.0],
            "date purchase": ["2023-02-05"],
            "review rating": [4.5],
            "payment method": ["  creditcard  "],
        }
    )

    clean_df = clean_fashion_sales(df)
    row = clean_df.iloc[0]

    # item purchased: stripped + title-cased
    assert row["item purchased"] == "Handbag"

    # payment method: stripped, title-cased, then mapped "Creditcard" -> "Credit Card"
    assert row["payment method"] == "Credit Card"

    # original numeric columns are untouched
    assert row["purchase amount (usd)"] == 4619.0
    assert row["review rating"] == 4.5


def test_clean_fashion_sales_deduplicates_on_business_key():
    """
    If two rows have the same (customer reference id, item purchased, date purchase),
    only the last one should remain (keep='last').
    """
    df = pd.DataFrame(
        {
            "customer reference id": [4018, 4018],
            "item purchased": ["Handbag", "handbag"],
            "purchase amount (usd)": [4619.0, 9999.0],  # different amounts
            "date purchase": ["2023-02-05", "2023-02-05"],
            "review rating": [4.5, 3.0],
            "payment method": ["Cash", "cash"],
        }
    )

    clean_df = clean_fashion_sales(df)

    # Dedup by (customer reference id, item purchased, date purchase) after cleaning
    # Both rows collapse into one; "last" row wins
    assert len(clean_df) == 1
    row = clean_df.iloc[0]

    # item purchased normalized
    assert row["item purchased"] == "Handbag"

    # last row's values should win for non-key fields
    assert row["purchase amount (usd)"] == 9999.0
    assert row["review rating"] == 3.0
    assert row["payment method"] == "Cash"  # normalized from "cash"


def test_clean_fashion_sales_fallback_dedup_when_key_incomplete():
    """
    If one of the key columns is missing, it should fall back to drop perfect duplicates.
    """
    df = pd.DataFrame(
        {
            # missing "date purchase" on purpose
            "customer reference id": [4018, 4018],
            "item purchased": ["Handbag", "Handbag"],
            "purchase amount (usd)": [4619.0, 4619.0],
            "review rating": [4.5, 4.5],
            "payment method": ["Cash", "Cash"],
        }
    )

    clean_df = clean_fashion_sales(df)

    # Because the full key (customer, item, date) isn't present,
    # function falls back to drop_duplicates(keep='last')
    assert len(clean_df) == 1
    row = clean_df.iloc[0]

    assert row["customer reference id"] == 4018
    assert row["item purchased"] == "Handbag"
    assert row["purchase amount (usd)"] == 4619.0
    assert row["review rating"] == 4.5
    assert row["payment method"] == "Cash"
