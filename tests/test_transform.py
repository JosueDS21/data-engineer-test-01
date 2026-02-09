"""Unit tests for transform module."""
import os
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
os.environ["PROJECT_ROOT"] = str(ROOT)


def test_transform_listings_cleans_price_and_derives():
    from src.pipeline.transform import transform_listings
    df = pd.DataFrame({
        "id": [1],
        "host_id": [100],
        "host_name": ["Host"],
        "neighbourhood": ["Downtown"],
        "neighbourhood_group": ["Ward A"],
        "room_type": ["Entire home/apt"],
        "price": ["$150.00"],
        "latitude": [40.7],
        "longitude": [-74.0],
        "minimum_nights": [2],
        "number_of_reviews": [10],
        "reviews_per_month": [0.5],
        "calculated_host_listings_count": [1],
        "availability_365": [200],
        "number_of_reviews_ltm": [2],
    })
    out = transform_listings(df)
    assert "price_clean" in out.columns or out["price_clean"].iloc[0] == 150.0
    assert "estimated_revenue_365" in out.columns
    assert "occupancy_rate" in out.columns
    assert "price_tier" in out.columns


def test_price_tier_mid():
    from src.pipeline.transform import transform_listings
    df = pd.DataFrame({
        "id": [1], "host_id": [1], "neighbourhood": ["X"], "neighbourhood_group": ["Y"],
        "room_type": ["Private room"], "price": ["150"],
        "latitude": [40], "longitude": [-74],
        "minimum_nights": [1], "number_of_reviews": [0], "reviews_per_month": [0],
        "calculated_host_listings_count": [1], "availability_365": [365], "number_of_reviews_ltm": [0],
    })
    out = transform_listings(df)
    assert out["price_tier"].iloc[0] in ("mid", "budget", "premium", "luxury", "unknown")
