import json
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
from sqlalchemy import text
from src.config import get_db_url
from src.logs.logging_config import *

logger = get_logger(__name__)

# Mapping from CSV column names -> DB column names
FASHION_COL_RENAME = {
    "customer reference id": "customer_reference_id",
    "item purchased": "item_purchased",
    "purchase amount (usd)": "purchase_amount_usd",
    "date purchase": "date_purchase",
    "review rating": "review_rating",
    "payment method": "payment_method",
}


def get_engine():
    db_url = get_db_url()
    return create_engine(db_url)


def load_rejects(reject_df: pd.DataFrame, source_name: str, reason: str) -> None:
    """
    Store rejected rows in a stg_rejects table with:
      - source_name
      - reason
      - payload (JSON)
      - rejected_at timestamp
    """
    if reject_df.empty:
        logger.info("   No rejected rows to log.")
        return

    engine = get_engine()

    # Turn each row into a JSON string (timestamps / etc. handled with default=str)
    payloads = [
        json.dumps(row, default=str) for row in reject_df.to_dict(orient="records")
    ]

    log_df = pd.DataFrame(
        {
            "source_name": source_name,
            "reason": reason,
            "payload": payloads,
            "rejected_at": datetime.utcnow(),
        }
    )

    log_df.to_sql(
        name="stg_rejects",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )

    logger.info(f"   Logged {len(log_df)} rejected rows to stg_rejects ({reason}).")


def upsert_dataframe(df: pd.DataFrame, table_name: str, pk_cols: list[str]) -> None:
    """
    Generic batch UPSERT into PostgreSQL using ON CONFLICT DO UPDATE.

    Assumes:
      - df columns already match DB column names
      - The DB table has a PRIMARY KEY or UNIQUE constraint on pk_cols
      - df has already been cleaned / deduplicated on pk_cols in the transform step
    """
    if df.empty:
        logger.debug(f"   No rows to upsert into {table_name}.")
        return

    engine = get_engine()

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    # Only keep columns that actually exist in the DB table
    table_cols = [c.name for c in table.columns]
    used_cols = [c for c in df.columns if c in table_cols]
    if not used_cols:
        raise ValueError(f"No overlapping columns between DataFrame and table {table_name}")

    trimmed_df = df[used_cols].copy()
    rows = trimmed_df.to_dict(orient="records")

    stmt = insert(table).values(rows)

    # Update all non-PK columns on conflict
    update_cols = {
        c.name: c
        for c in table.columns
        if c.name in used_cols and c.name not in pk_cols
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=pk_cols,
        set_=update_cols,
    )

    #This context opens a transaction and COMMITs when the block exits
    with engine.begin() as conn:
        conn.execute(stmt)

    logger.debug(f"   UPSERTED {len(trimmed_df)} rows into {table_name}.")



def load_fashion_sales_upsert(df: pd.DataFrame, table_name: str) -> None:
    """
    Loader for the Fashion Retail dataset.

    - Renames from CSV-style column names to DB column names
    - Performs batch UPSERT into the given table using a composite key:
      (customer_reference_id, item_purchased, date_purchase)
    """
    if df.empty:
        logger.debug("   No rows to load (fashion sales).")
        return

    # Rename columns to match DB schema
    db_df = df.rename(columns=FASHION_COL_RENAME)

    # Composite business key for a "sale" (must match DB PRIMARY KEY definition)
    pk_cols = ["customer_reference_id", "item_purchased", "date_purchase"]

    upsert_dataframe(db_df, table_name, pk_cols)


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

    logger.debug(f"   Logged {len(rejects_df)} rejected rows to stg_rejects ({reason})")
    



