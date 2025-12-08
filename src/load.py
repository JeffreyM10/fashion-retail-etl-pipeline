import pandas as pd
import json
from sqlalchemy import create_engine
from src.config import get_db_url

# Mapping from DataFrame column names -> DB column names
FASHION_COL_RENAME = {
    "customer reference id": "customer_reference_id",
    "item purchased": "item_purchased",
    "purchase amount (usd)": "purchase_amount_usd",
    "date purchase": "date_purchase",
    "review rating": "review_rating",
    "payment method": "payment_method",
}

def load_fashion_sales(df: pd.DataFrame, table_name: str):
    """
    Load the validated fashion sales DataFrame into PostgreSQL.
    - Renames columns to match the stg_fashion_sales table.
    - Appends rows using pandas.to_sql.
    """
    if df.empty:
        print("   No rows to load, skipping DB insert.")
        return

    # 1) Rename columns to match DB table names
    df_to_load = df.rename(columns=FASHION_COL_RENAME)

    # 2) Get DB URL and create SQLAlchemy engine
    db_url = get_db_url()
    engine = create_engine(db_url)

    # 3) Write to Postgres
    #    if_exists='append' will insert new rows
    df_to_load.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",   # send many rows per INSERT
        chunksize=1000,
    )

    print(f"   Loaded {len(df_to_load)} rows into {table_name}")

def load_rejects(df: pd.DataFrame, source_name: str, reason: str):
    """
    Load rejected rows into stg_rejects.
    Converts row values into Python-native types so JSON serialization works.
    """
    if df.empty:
        return

    records = []
    for rec in df.to_dict(orient="records"):

        # Convert each value in the row
        clean_rec = {}
        for key, val in rec.items():
            if isinstance(val, pd.Timestamp):
                clean_rec[key] = val.isoformat()   # convert datetime -> string
            elif pd.isna(val):
                clean_rec[key] = None              # NaNs are not JSON
            else:
                clean_rec[key] = val

        records.append(
            {
                "source_name": source_name,
                "raw_payload": json.dumps(clean_rec),
                "reason": reason,
            }
        )

    rejects_df = pd.DataFrame(records)

    db_url = get_db_url()
    engine = create_engine(db_url)

    rejects_df.to_sql(
        name="stg_rejects",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    print(f"   Logged {len(rejects_df)} rejected rows to stg_rejects ({reason})")

