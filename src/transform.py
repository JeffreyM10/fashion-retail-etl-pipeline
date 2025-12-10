import pandas as pd

#drop the duplicates before we insert the data to the DB
def to_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df[["customer reference id"]]
        .dropna()
        .drop_duplicates()
        .rename(columns={"customer reference id": "customer_reference_id"})
    )

def to_dim_item(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df[["item purchased"]]
        .dropna()
        .drop_duplicates()
        .rename(columns={"item purchased": "item_name"})
    )
