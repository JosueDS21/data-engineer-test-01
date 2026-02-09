"""Unit tests for extract module."""
import os
from pathlib import Path

import pandas as pd
import pytest

# Project root
ROOT = Path(__file__).resolve().parents[1]
os.environ["PROJECT_ROOT"] = str(ROOT)


def test_read_listings_csv_missing_file():
    from src.pipeline.extract import read_listings_csv
    with pytest.raises(FileNotFoundError):
        read_listings_csv("data", "nonexistent.csv")


def test_extract_returns_frames_when_data_exists():
    """If data/listings.csv and data/reviews.csv exist, extract returns two DataFrames."""
    from src.pipeline.extract import extract, load_config
    config = load_config()
    data_dir = config["paths"]["data_dir"]
    listings_path = ROOT / data_dir / config["paths"]["listings_file"]
    reviews_path = ROOT / data_dir / config["paths"]["reviews_file"]
    if not listings_path.exists() or not reviews_path.exists():
        pytest.skip("Data files not found; create data/listings.csv and data/reviews.csv")
    listings_df, reviews_df = extract(config)
    assert isinstance(listings_df, pd.DataFrame)
    assert isinstance(reviews_df, pd.DataFrame)
    assert "id" in listings_df.columns or len(listings_df.columns) >= 5
    assert "listing_id" in reviews_df.columns or "date" in reviews_df.columns or len(reviews_df.columns) >= 1
