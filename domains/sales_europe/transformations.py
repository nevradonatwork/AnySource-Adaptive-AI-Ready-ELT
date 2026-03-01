"""
domains/sales_europe/transformations.py

Pure transformation functions — no DB calls, no file I/O, no global state.
All functions are unit-testable without mocking.
"""

import pandas as pd


def apply(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all transformations. Called by core/staging.py before writing to stg_orders."""
    df = _strip_whitespace(df)
    df = _normalise_region(df)
    df = _normalise_email(df)
    df = _coerce_numeric_columns(df)
    return df


def _strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading/trailing whitespace from all string columns."""
    str_cols = df.select_dtypes(include=["object", "str"]).columns
    df = df.copy()
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())
    return df


def _normalise_region(df: pd.DataFrame) -> pd.DataFrame:
    """Uppercase the region column."""
    if "region" in df.columns:
        df = df.copy()
        df["region"] = df["region"].str.upper()
    return df


def _normalise_email(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase the customer_email column."""
    if "customer_email" in df.columns:
        df = df.copy()
        df["customer_email"] = df["customer_email"].str.lower()
    return df


def _coerce_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Cast quantity, unit_price, and amount to numeric, coercing errors to NaN."""
    df = df.copy()
    for col in ["quantity", "unit_price", "amount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df
