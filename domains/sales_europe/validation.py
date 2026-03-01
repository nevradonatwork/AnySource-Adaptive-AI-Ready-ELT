"""
domains/sales_europe/validation.py

Column presence checks and business rules for the sales_europe domain.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS: list[str] = [
    "order_id", "customer_id", "customer_email",
    "region", "order_date", "quantity", "unit_price", "amount",
]

VALID_REGIONS: set[str] = {"EU", "US", "APAC", "LATAM"}


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the DataFrame.
    Raises ValueError for missing required columns.
    Logs warnings for business rule violations (non-blocking).
    Returns the DataFrame unchanged.
    """
    _check_required_columns(df)
    _apply_business_rules(df)
    return df


def _check_required_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _apply_business_rules(df: pd.DataFrame) -> None:
    if "amount" in df.columns:
        bad = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
        non_positive = (bad <= 0).sum()
        if non_positive:
            logger.warning(
                "Business rule violation: %d row(s) have amount <= 0", non_positive
            )

    if "quantity" in df.columns:
        bad = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
        non_positive = (bad <= 0).sum()
        if non_positive:
            logger.warning(
                "Business rule violation: %d row(s) have quantity <= 0", non_positive
            )

    if "region" in df.columns:
        unknown = (~df["region"].str.upper().isin(VALID_REGIONS)).sum()
        if unknown:
            logger.warning(
                "Business rule violation: %d row(s) have an unrecognised region", unknown
            )
