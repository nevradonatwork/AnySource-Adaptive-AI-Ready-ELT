"""Tests for domains/sales_europe/transformations.py"""

import pandas as pd
import pytest

from domains.sales_europe.transformations import (
    apply,
    _strip_whitespace,
    _normalise_region,
    _normalise_email,
    _coerce_numeric_columns,
)


def _sample_df():
    return pd.DataFrame({
        "order_id":       ["1001"],
        "customer_id":    ["101"],
        "customer_email": ["Alice@Example.COM"],
        "region":         ["  eu  "],
        "order_date":     ["2025-01-05"],
        "product_id":     ["PRD-A"],
        "quantity":       ["2"],
        "unit_price":     ["49.99"],
        "amount":         ["99.98"],
    })


# --- _strip_whitespace ---

def test_strip_whitespace_removes_leading_trailing():
    df = pd.DataFrame({"region": ["  EU  "], "amount": ["99.98"]})
    result = _strip_whitespace(df)
    assert result["region"].iloc[0] == "EU"


def test_strip_whitespace_does_not_modify_non_string_columns():
    df = pd.DataFrame({"amount": [99.98]})
    result = _strip_whitespace(df)
    assert result["amount"].iloc[0] == 99.98


def test_strip_whitespace_does_not_mutate_input():
    df = pd.DataFrame({"region": ["  EU  "]})
    original = df["region"].iloc[0]
    _strip_whitespace(df)
    assert df["region"].iloc[0] == original


# --- _normalise_region ---

def test_normalise_region_uppercases():
    df = pd.DataFrame({"region": ["eu", "us", "Apac"]})
    result = _normalise_region(df)
    assert list(result["region"]) == ["EU", "US", "APAC"]


def test_normalise_region_noop_when_column_absent():
    df = pd.DataFrame({"order_id": ["1"]})
    result = _normalise_region(df)
    assert "region" not in result.columns


# --- _normalise_email ---

def test_normalise_email_lowercases():
    df = pd.DataFrame({"customer_email": ["Alice@Example.COM", "BOB@TEST.ORG"]})
    result = _normalise_email(df)
    assert list(result["customer_email"]) == ["alice@example.com", "bob@test.org"]


def test_normalise_email_noop_when_column_absent():
    df = pd.DataFrame({"order_id": ["1"]})
    result = _normalise_email(df)
    assert "customer_email" not in result.columns


# --- _coerce_numeric_columns ---

def test_coerce_numeric_converts_string_to_numeric():
    df = pd.DataFrame({"quantity": ["3"], "unit_price": ["49.99"], "amount": ["149.97"]})
    result = _coerce_numeric_columns(df)
    # pd.to_numeric may return int or float depending on value — check for numeric kind
    assert result["quantity"].dtype.kind in ("i", "f", "u")
    assert result["amount"].iloc[0] == pytest.approx(149.97)


def test_coerce_numeric_produces_nan_for_invalid_values():
    df = pd.DataFrame({"amount": ["not-a-number"]})
    result = _coerce_numeric_columns(df)
    import math
    assert math.isnan(result["amount"].iloc[0])


# --- apply (full pipeline) ---

def test_apply_strips_whitespace():
    df = _sample_df()
    result = apply(df)
    assert result["region"].iloc[0] == "EU"


def test_apply_uppercases_region():
    df = _sample_df()
    result = apply(df)
    assert result["region"].iloc[0] == result["region"].iloc[0].upper()


def test_apply_lowercases_email():
    df = _sample_df()
    result = apply(df)
    assert result["customer_email"].iloc[0] == "alice@example.com"


def test_apply_coerces_amount_to_numeric():
    df = _sample_df()
    result = apply(df)
    assert isinstance(result["amount"].iloc[0], float)


def test_apply_does_not_drop_rows():
    df = _sample_df()
    result = apply(df)
    assert len(result) == len(df)
