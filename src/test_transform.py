import pandas as pd
from src.transform import to_dim_customer, to_dim_item


def test_to_dim_customer_drops_nulls_and_duplicates_and_renames():
    df = pd.DataFrame(
        {
            "customer reference id": [101, 101, None, 202],
            "other_col": ["x", "y", "z", "w"],
        }
    )

    dim_customer = to_dim_customer(df)

    # Column is renamed correctly
    assert list(dim_customer.columns) == ["customer_reference_id"]

    # Nulls dropped, duplicates removed
    assert sorted(dim_customer["customer_reference_id"].tolist()) == [101, 202]

def test_to_dim_item_drops_nulls_and_duplicates_and_renames():
    df = pd.DataFrame(
        {
            "item purchased": ["Jeans", "Jeans", None, "Hat"],
            "other_col": [1, 2, 3, 4],
        }
    )

    dim_item = to_dim_item(df)

    # Column is renamed correctly
    assert list(dim_item.columns) == ["item_name"]

    # Nulls dropped, duplicates removed
    assert sorted(dim_item["item_name"].tolist()) == ["Hat", "Jeans"]
