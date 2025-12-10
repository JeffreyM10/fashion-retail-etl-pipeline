import pandas as pd
""""Validate will look at the data that is load 
to see if the is valid (follow the schema rules) """ 

#Check if there are missing cols but subtracting df cols from schema
def check_missing_columns(df, schema):
    required = schema.keys() # getting the schema keys which are the cols needed
    missing = required - df.columns # casting them to set because sets have operations as such 
    
    #own logic if it has missing coloumns name them if not then just return None as a string
    #if list(missing) != []:
    #    return list(missing)
    #else:
    #    return "None"
    return missing

import pandas as pd


def apply_schema_casts(df, schema: dict):
    """
    Cast columns in df to the types defined in schema.
    Returns (valid_df, reject_df):
      - valid_df: rows where all non-string columns cast successfully
      - reject_df: rows where at least one non-string column failed casting
    """
    df = df.copy()
    
    # 1. Try to cast each column based on schema
    for col, type_name in schema.items():
        if col not in df.columns:
            continue  # missing columns already handled elsewhere
        
        series = df[col]

        if type_name == "int":
            casted = pd.to_numeric(series, errors="coerce").astype("Int64") #int64 to mark null values as NaN
        elif type_name == "float":
            casted = pd.to_numeric(series, errors="coerce")
        elif type_name == "datetime":
            casted = pd.to_datetime(series, errors="coerce")
        elif type_name == "str":
            casted = series.astype("string")
        else:
            # unknown type, leave as is
            casted = series

        df[col] = casted

    # 2. Build a mask of bad rows:
    # any row where a non-string column is NaN after casting = invalid
    non_str_cols = [c for c, t in schema.items() if t in ("int", "float", "datetime")]
    if non_str_cols:
        bad_mask = df[non_str_cols].isna().any(axis=1)
    else:
        bad_mask = pd.Series(False, index=df.index)

    reject_df = df[bad_mask].reset_index(drop=True)
    valid_df = df[~bad_mask].reset_index(drop=True)

    return valid_df, reject_df


def apply_business_rules(df):
    df = df.copy()
    # Start with all rows marked as good (False = not bad)
    bad_mask = pd.Series(False, index=df.index)

    # 1) purchase amount (usd) must be >= 0
    if "purchase amount (usd)" in df.columns:
        bad_mask |= df["purchase amount (usd)"] < 0

    # 2) review rating must be between 0 and 5, but allow nulls
    if "review rating" in df.columns:
        rating = df["review rating"]
        invalid_rating = rating.notna() & ((rating < 0) | (rating > 5))
        bad_mask |= invalid_rating

    # 3) payment method must be either 'Cash' or 'Credit Card'
    if "payment method" in df.columns:
        allowed = ["Cash", "Credit Card"]
        bad_mask |= ~df["payment method"].isin(allowed)
        
    # 4) item purchased cannot be empty/blank
    if "item purchased" in df.columns:
        empty_item = (
            df["item purchased"].isna()
            | (df["item purchased"].astype("string").str.strip() == "")
        )
        bad_mask |= empty_item

    reject_df = df[bad_mask].reset_index(drop=True)
    valid_df = df[~bad_mask].reset_index(drop=True)

    return valid_df, reject_df