"""
ml_analysis.py

Lightweight ML module built on top of the ETL Data Ingestion Sub-System.

- Reads cleaned data from stg_fashion_sales
- Creates a binary target: low_review (rating <= 2)
- Detects simple purchase amount outliers
- Trains:
    * LogisticRegression
    * RandomForestClassifier
- Prints metrics and feature importances
"""

import os

import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import StandardScaler

from dotenv import load_dotenv

# Load .env once at import time
load_dotenv()


def get_db_url() -> str:
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL not set. Please create a .env file with DB_URL.")
    return db_url


def get_engine():
    return create_engine(get_db_url())


def load_silver_data() -> pd.DataFrame:
    """
    Load the cleaned / silver-layer data from stg_fashion_sales.
    """
    engine = get_engine()
    query = "SELECT * FROM stg_fashion_sales"
    df = pd.read_sql(query, engine)
    return df


def detect_outliers(df: pd.DataFrame, z_thresh: float = 3.0) -> pd.DataFrame:
    """
    Simple z-score based outlier detection on purchase_amount_usd.
    Flags rows where |z| > z_thresh.
    """
    if "purchase_amount_usd" not in df.columns:
        print("[outliers] Column purchase_amount_usd not found; skipping outlier detection.")
        return pd.DataFrame()

    amounts = df["purchase_amount_usd"].astype(float)
    mean = amounts.mean()
    std = amounts.std(ddof=0)

    if std == 0:
        print("[outliers] Std dev is 0; no variability in purchase_amount_usd.")
        return pd.DataFrame()

    z_scores = (amounts - mean) / std
    outlier_mask = z_scores.abs() > z_thresh

    outliers = df.loc[outlier_mask].copy()
    outliers["z_score"] = z_scores[outlier_mask]

    return outliers.sort_values("z_score", key=lambda s: s.abs(), ascending=False)


def build_features_and_target(df: pd.DataFrame):
    """
    Prepare features X and target y for classification:
    Target: low_review = 1 if review_rating <= 2, else 0
    Features:
        - purchase_amount_usd (numeric)
        - payment_method (one-hot)
        - item_purchased (one-hot)
    """
    # Drop rows without ratings or amounts
    df = df.dropna(subset=["review_rating", "purchase_amount_usd"])
    if df.empty:
        raise ValueError("No rows left after dropping missing review_rating/purchase_amount_usd.")

    # Target
    df["low_review"] = (df["review_rating"] <= 2).astype(int)

    feature_cols = ["purchase_amount_usd", "payment_method", "item_purchased"]
    missing = [c for c in feature_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected feature columns: {missing}")

    feat_df = df[feature_cols].copy()

    # One-hot encode categorical features
    feat_df = pd.get_dummies(
        feat_df,
        columns=["payment_method", "item_purchased"],
        drop_first=True,
    )

    X = feat_df
    y = df["low_review"]

    return X, y, feat_df.columns.tolist()


def train_and_evaluate_models(X, y, feature_names):
    """
    Train LogisticRegression and RandomForestClassifier,
    print metrics & feature importances.
    """
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y
    )

    # Scale numeric features for Logistic Regression
    scaler = StandardScaler(with_mean=False)  # with_mean=False for sparse matrices
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    print("\n=== Logistic Regression (predict low_review) ===")
    log_reg = LogisticRegression(max_iter=1000)
    log_reg.fit(X_train_scaled, y_train)
    y_pred_lr = log_reg.predict(X_test_scaled)
    print(classification_report(y_test, y_pred_lr, digits=3))

    coef_series = pd.Series(log_reg.coef_[0], index=feature_names)
    print("\nTop positive coefficients (more likely low_review):")
    print(coef_series.sort_values(ascending=False).head(5))
    print("\nTop negative coefficients (less likely low_review):")
    print(coef_series.sort_values(ascending=True).head(5))

    print("\n=== Random Forest Classifier (predict low_review) ===")
    rf = RandomForestClassifier(
        n_estimators=200,
        max_depth=None,
        random_state=42,
        n_jobs=-1,
    )
    rf.fit(X_train, y_train)
    y_pred_rf = rf.predict(X_test)
    print(classification_report(y_test, y_pred_rf, digits=3))

    importances = pd.Series(rf.feature_importances_, index=feature_names)
    importances = importances.sort_values(ascending=False)
    print("\nTop feature importances (Random Forest):")
    print(importances.head(10))

    print("\nConfusion matrix - Logistic Regression:")
    print(confusion_matrix(y_test, y_pred_lr))

    print("\nConfusion matrix - Random Forest:")
    print(confusion_matrix(y_test, y_pred_rf))


def main():
    print("=== ML Analysis: Predicting Low Reviews from stg_fashion_sales ===")
    df = load_silver_data()
    print(f"[info] Loaded {len(df)} rows from stg_fashion_sales")

    print("\n=== Outlier Detection on purchase_amount_usd ===")
    outliers = detect_outliers(df, z_thresh=3.0)
    if outliers.empty:
        print("[outliers] No strong outliers detected.")
    else:
        print(f"[outliers] Found {len(outliers)} outlier rows (showing top 5):")
        display_cols = [
            c
            for c in outliers.columns
            if c
            in (
                "customer_reference_id",
                "item_purchased",
                "purchase_amount_usd",
                "review_rating",
                "payment_method",
                "z_score",
            )
        ]
        print(outliers[display_cols].head(5))

    print("\n=== Building features and target (low_review) ===")
    X, y, feature_names = build_features_and_target(df)
    print(
        f"[info] Feature matrix shape: {X.shape}, "
        f"Target positive rate (low_review=1): {y.mean():.3f}"
    )

    train_and_evaluate_models(X, y, feature_names)

    print("\n=== ML analysis complete ===")


if __name__ == "__main__":
    main()
