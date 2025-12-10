import json
import pandas as pd
import src.load as load


def test_load_fashion_sales_upsert_empty_df_skips_upsert(monkeypatch):
    """If the DataFrame is empty, upsert_dataframe should not be called."""
    called = {}

    def fake_upsert(df, table_name, pk_cols):
        called["called"] = True  # should NOT be set

    monkeypatch.setattr(load, "upsert_dataframe", fake_upsert)

    empty_df = pd.DataFrame()

    load.load_fashion_sales_upsert(empty_df, table_name="stg_fashion_sales")

    assert "called" not in called


def test_load_fashion_sales_upsert_renames_and_calls_upsert(monkeypatch):
    """Non-empty DF is renamed and passed to upsert_dataframe with correct PK cols."""
    captured = {}

    def fake_upsert(df, table_name, pk_cols):
        captured["df"] = df
        captured["table_name"] = table_name
        captured["pk_cols"] = pk_cols

    monkeypatch.setattr(load, "upsert_dataframe", fake_upsert)

    df = pd.DataFrame(
        {
            "customer reference id": [1],
            "item purchased": ["Jeans"],
            "purchase amount (usd)": [100.0],
            "date purchase": [pd.Timestamp("2025-01-01")],
            "review rating": [4.5],
            "payment method": ["Cash"],
        }
    )

    load.load_fashion_sales_upsert(df, table_name="stg_fashion_sales")

    # Assert upsert was called
    assert "df" in captured

    out_df = captured["df"]
    assert captured["table_name"] == "stg_fashion_sales"

    # Columns should be renamed using FASHION_COL_RENAME
    assert set(out_df.columns) >= {
        "customer_reference_id",
        "item_purchased",
        "purchase_amount_usd",
        "date_purchase",
        "review_rating",
        "payment_method",
    }

    # PK columns passed into upsert
    assert captured["pk_cols"] == [
        "customer_reference_id",
        "item_purchased",
        "date_purchase",
    ]


def test_load_rejects_empty_df_returns_quickly(monkeypatch):
    """If rejects DF is empty, we should not attempt to write anything."""
    called = {}

    def fake_to_sql(self, *args, **kwargs):
        called["called"] = True

    # Patching DataFrame.to_sql at the class level
    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=True)

    empty_df = pd.DataFrame()

    load.load_rejects(empty_df, source_name="test_source", reason="test_reason")

    # Should never call to_sql for an empty df
    assert "called" not in called


def test_load_rejects_builds_json_payload_and_calls_to_sql(monkeypatch):
    """load_rejects should normalize values, build JSON payload, and call to_sql."""
    class DummyEngine:
        pass

    # Avoid touching real config/DB
    monkeypatch.setattr(load, "get_db_url", lambda: "postgresql://user:pass@localhost/db")
    monkeypatch.setattr(load, "create_engine", lambda url: DummyEngine())

    captured = {}

    def fake_to_sql(self, name, con, if_exists, index, method, chunksize):
        captured["df"] = self.copy()
        captured["name"] = name
        captured["con"] = con
        captured["if_exists"] = if_exists
        captured["index"] = index
        captured["method"] = method
        captured["chunksize"] = chunksize

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=True)

    df = pd.DataFrame(
        {
            "customer reference id": [1, 2],
            "purchase date": [pd.Timestamp("2025-01-01"), pd.NaT],
            "some_value": [10.0, None],
        }
    )

    load.load_rejects(df, source_name="fashion_sales_csv", reason="bad_data")

    # Ensure to_sql was called
    assert "df" in captured

    out_df = captured["df"]
    assert captured["name"] == "stg_rejects"
    assert isinstance(captured["con"], DummyEngine)
    assert captured["if_exists"] == "append"
    assert captured["index"] is False
    assert captured["method"] == "multi"

    # We should have one row per input row
    assert out_df["source_name"].tolist() == ["fashion_sales_csv", "fashion_sales_csv"]
    assert out_df["reason"].tolist() == ["bad_data", "bad_data"]

    # raw_payload should be valid JSON with normalized values
    payload0 = json.loads(out_df["raw_payload"].iloc[0])
    assert payload0["customer reference id"] == 1
    # Timestamp converted to ISO string
    assert payload0["purchase date"].startswith("2025-01-01")

    payload1 = json.loads(out_df["raw_payload"].iloc[1])
    # NaT/None become null in JSON
    assert payload1["purchase date"] is None
    assert payload1["some_value"] is None
