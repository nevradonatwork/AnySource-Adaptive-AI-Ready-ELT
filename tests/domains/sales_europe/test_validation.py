"""Tests for domains/sales_europe/validation.py"""

import logging

import pandas as pd
import pytest

from domains.sales_europe.validation import validate, REQUIRED_COLUMNS, VALID_REGIONS


def _good_df():
    return pd.DataFrame({
        "order_id":       ["1001", "1002"],
        "customer_id":    ["101",  "102"],
        "customer_email": ["a@b.com", "c@d.com"],
        "region":         ["EU",   "US"],
        "order_date":     ["2025-01-05", "2025-01-06"],
        "product_id":     ["PRD-A", "PRD-B"],
        "quantity":       [2.0, 1.0],
        "unit_price":     [49.99, 199.00],
        "amount":         [99.98, 199.00],
    })


def test_validate_passes_clean_data():
    df = _good_df()
    result = validate(df)
    assert len(result) == 2


def test_validate_returns_same_dataframe():
    df = _good_df()
    result = validate(df)
    assert result is df


def test_validate_raises_on_missing_required_column():
    df = _good_df().drop(columns=["amount"])
    with pytest.raises(ValueError, match="Missing required columns"):
        validate(df)


def test_validate_raises_listing_all_missing():
    df = _good_df().drop(columns=["order_id", "customer_id"])
    with pytest.raises(ValueError) as exc_info:
        validate(df)
    msg = str(exc_info.value)
    assert "order_id" in msg
    assert "customer_id" in msg


def test_validate_warns_on_zero_amount(caplog):
    df = _good_df()
    df.loc[0, "amount"] = 0.0
    with caplog.at_level(logging.WARNING):
        validate(df)
    assert any("amount" in r.message for r in caplog.records)


def test_validate_warns_on_negative_amount(caplog):
    df = _good_df()
    df.loc[0, "amount"] = -5.0
    with caplog.at_level(logging.WARNING):
        validate(df)
    assert any("amount" in r.message for r in caplog.records)


def test_validate_warns_on_zero_quantity(caplog):
    df = _good_df()
    df.loc[1, "quantity"] = 0.0
    with caplog.at_level(logging.WARNING):
        validate(df)
    assert any("quantity" in r.message for r in caplog.records)


def test_validate_warns_on_unknown_region(caplog):
    df = _good_df()
    df.loc[0, "region"] = "MOON"
    with caplog.at_level(logging.WARNING):
        validate(df)
    assert any("region" in r.message for r in caplog.records)


def test_validate_does_not_raise_on_bad_business_rule():
    """Business rule violations must log warnings, not raise exceptions."""
    df = _good_df()
    df.loc[0, "amount"] = -999.0
    validate(df)  # should not raise


def test_valid_regions_constant():
    assert VALID_REGIONS == {"EU", "US", "APAC", "LATAM"}
