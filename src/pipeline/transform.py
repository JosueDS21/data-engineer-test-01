"""
Transform: Clean and standardize staging data, derive features.
Reads from staging tables (after validate); returns cleaned DataFrames for load.
- Normalize price (strip $ and commas, cast to numeric).
- Derive estimated_revenue_365, occupancy_rate, price_tier.
- Standardize text (strip), handle nulls.
"""
import os
from pathlib import Path

import pandas as pd

_PROJECT_ROOT = os.getenv("PROJECT_ROOT") or Path(__file__).resolve().parents[2]


def load_config() -> dict:
    import yaml
    config_path = Path(_PROJECT_ROOT) / "src" / "config" / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _parse_price(price_ser: pd.Series) -> pd.Series:
    """Parse price: strip $ and commas, convert to numeric; invalid -> NaN."""
    if price_ser.isna().all():
        return price_ser
    s = price_ser.astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False)
    return pd.to_numeric(s, errors="coerce")


def _price_tier(price: float, tier_bounds: dict) -> str:
    if price is None or (isinstance(price, float) and (price != price or price < 0)):
        return "unknown"
    for tier, bound in [("budget", tier_bounds.get("budget", 100)), ("mid", tier_bounds.get("mid", 200)),
                       ("premium", tier_bounds.get("premium", 500)), ("luxury", tier_bounds.get("luxury", 999999))]:
        if price <= bound:
            return tier
    return "luxury"


def transform_listings(staging_listings_df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
    """Clean and derive columns for listings. Returns DataFrame with numeric price and derived fields."""
    cfg = config or load_config()
    df = staging_listings_df.copy()

    # Normalize price
    if "price" in df.columns:
        df["price_clean"] = _parse_price(df["price"])
    else:
        df["price_clean"] = None

    # Numeric columns: coerce and fill NaN where needed for derivation
    for col in ["minimum_nights", "number_of_reviews", "reviews_per_month", "calculated_host_listings_count",
                "availability_365", "number_of_reviews_ltm"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Latitude / longitude
    if "latitude" in df.columns:
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    if "longitude" in df.columns:
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    # Derived: estimated_revenue_365 = price * (availability_365 / 365)
    avail = df["availability_365"].fillna(0)
    df["estimated_revenue_365"] = (df["price_clean"] * avail / 365.0).round(2)

    # Occupancy rate: booked days / 365 = 1 - availability_365/365
    df["occupancy_rate"] = (1 - avail / 365.0).round(4)
    df.loc[df["occupancy_rate"] < 0, "occupancy_rate"] = 0
    df.loc[df["occupancy_rate"] > 1, "occupancy_rate"] = 1

    # Price tier
    tier_bounds = cfg.get("pricing", {}).get("tier_bounds", {"budget": 100, "mid": 200, "premium": 500, "luxury": 999999})
    df["price_tier"] = df["price_clean"].apply(lambda p: _price_tier(p, tier_bounds))

    # Standardize text (strip)
    for col in ["name", "host_name", "neighbourhood_group", "neighbourhood", "room_type", "license"]:
        if col in df.columns and df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip().replace("nan", None)

    return df


def transform_reviews(staging_reviews_df: pd.DataFrame) -> pd.DataFrame:
    """Clean reviews: ensure listing_id and date, parse date."""
    df = staging_reviews_df.copy()
    if "listing_id" in df.columns:
        df["listing_id"] = pd.to_numeric(df["listing_id"], errors="coerce")
    if "date" in df.columns:
        df["review_date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def read_staging(conn) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read staging_listings and staging_reviews into DataFrames."""
    listings_df = pd.read_sql("SELECT * FROM staging_listings", conn)
    reviews_df = pd.read_sql("SELECT * FROM staging_reviews", conn)
    return listings_df, reviews_df


def run_transform(conn, config: dict | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Read staging from conn, clean and derive, return (listings_clean_df, reviews_clean_df).
    listings_clean_df has: id, name, host_id, host_name, neighbourhood_group, neighbourhood, latitude, longitude,
    room_type, price_clean (numeric), minimum_nights, number_of_reviews, last_review, reviews_per_month,
    calculated_host_listings_count, availability_365, number_of_reviews_ltm, license,
    estimated_revenue_365, occupancy_rate, price_tier.
    """
    cfg = config or load_config()
    listings_df, reviews_df = read_staging(conn)
    listings_clean = transform_listings(listings_df, cfg)
    reviews_clean = transform_reviews(reviews_df)
    return listings_clean, reviews_clean
