import pandas as pd
from src.validate import (
    check_missing_columns,
    apply_schema_casts,
    apply_business_rules,
)


def test_check_missing_columns_returns_missing_set():
    # DataFrame has 2 columns, schema expects 3
    df = pd.DataFrame({
        "customer reference id": [1, 2],
        "item purchased": ["Jeans", "Hat"],
    })

    schema = {
        "customer reference id": {"type": "int"},
        "item purchased": {"type": "str"},
        "purchase amount (usd)": {"type": "float"},
    }

    missing = check_missing_columns(df, schema)

    # We expect "purchase amount (usd)" to be missing
    assert "purchase amount (usd)" in missing
    assert len(missing) == 1



def test_apply_schema_casts_splits_valid_and_invalid_rows():
    df = pd.DataFrame({
        "customer reference id": ["4018", "ABC", "4019"],
        "item purchased": ["Handbag", "Jeans", "Tank Top"],
        "purchase amount (usd)": ["4619.0", "100.0", None],
        "date purchase": ["2023-02-05", "not_a_date", "2023-03-23"],
        "review rating": ["4.5", "3.2", "bad"],
    })

    schema = {
        "customer reference id": "int",
        "item purchased": "str",
        "purchase amount (usd)": "float",
        "date purchase": "datetime",
        "review rating": "float",
    }

    valid_df, reject_df = apply_schema_casts(df, schema)

    # Only the first row should be fully valid:
    # - "4018" -> int
    # - "4619.0" -> float
    # - "2023-02-05" -> datetime
    # - "4.5" -> float
    assert len(valid_df) == 1
    row = valid_df.iloc[0]
    assert row["customer reference id"] == 4018
    assert row["purchase amount (usd)"] == 4619.0
    assert str(row["date purchase"]).startswith("2023-02-05")
    assert row["review rating"] == 4.5

    # The other 2 rows should be rejected
    assert len(reject_df) == 2



def test_apply_business_rules_flags_invalid_rows_and_keeps_valid():
    df = pd.DataFrame({
        "purchase amount (usd)": [100.0, -50.0, 200.0, 300.0, 400.0],
        "review rating": [None, 3.0, 6.0, 4.0, 4.5],
        "payment method": ["Cash", "Cash", "Debit", "Credit Card", "Cash"],
        "item purchased": ["Jeans", "Jeans", "Jeans", "Jeans", "   "],
    })

    valid_df, reject_df = apply_business_rules(df)

    # Row-by-row intent:
    # row 0: valid (amount>0, rating None allowed, payment Cash, item non-empty)
    # row 1: invalid (negative amount)
    # row 2: invalid (rating > 5 AND payment 'Debit' not allowed)
    # row 3: valid (all good)
    # row 4: invalid (blank item)

    # We expect 2 valid rows: index 0 and 3
    assert len(valid_df) == 2
    assert valid_df["purchase amount (usd)"].tolist() == [100.0, 300.0]

    # And 3 rejected rows
    assert len(reject_df) == 3

