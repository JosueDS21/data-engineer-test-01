"""
Extract: Read CSV files and stage raw data into staging tables.
Handles encoding (UTF-8, UTF-8-BOM) and writes to staging_listings / staging_reviews with load_id.
"""
import os
from pathlib import Path

import pandas as pd

from src.utils.db_helpers import run_sql, run_many

# Allow running from project root or from src/pipeline
_PROJECT_ROOT = os.getenv("PROJECT_ROOT") or Path(__file__).resolve().parents[2]


def _data_path(relative_path: str) -> Path:
    return Path(_PROJECT_ROOT) / relative_path


def load_config() -> dict:
    import yaml
    config_path = Path(_PROJECT_ROOT) / "src" / "config" / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def read_listings_csv(data_dir: str, listings_file: str, encoding: str = "utf-8", fallback_encoding: str = "utf-8-sig") -> pd.DataFrame:
    """Read listings CSV; try fallback encoding if first fails."""
    path = _data_path(os.path.join(data_dir, listings_file))
    if not path.exists():
        raise FileNotFoundError(f"Listings file not found: {path}")
    try:
        df = pd.read_csv(path, encoding=encoding, low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding=fallback_encoding, low_memory=False)
    return df


def read_reviews_csv(data_dir: str, reviews_file: str, encoding: str = "utf-8", fallback_encoding: str = "utf-8-sig") -> pd.DataFrame:
    """Read reviews CSV; try fallback encoding if first fails."""
    path = _data_path(os.path.join(data_dir, reviews_file))
    if not path.exists():
        raise FileNotFoundError(f"Reviews file not found: {path}")
    try:
        df = pd.read_csv(path, encoding=encoding, low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding=fallback_encoding, low_memory=False)
    return df


def extract(config: dict | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Read listings and reviews CSVs from configured paths.
    Returns (listings_df, reviews_df). Does not write to DB; staging is done in orchestrator.
    """
    cfg = config or load_config()
    paths = cfg["paths"]
    enc = cfg.get("encoding", {})
    listings_df = read_listings_csv(
        paths["data_dir"],
        paths["listings_file"],
        encoding=enc.get("default", "utf-8"),
        fallback_encoding=enc.get("fallback", "utf-8-sig"),
    )
    reviews_df = read_reviews_csv(
        paths["data_dir"],
        paths["reviews_file"],
        encoding=enc.get("default", "utf-8"),
        fallback_encoding=enc.get("fallback", "utf-8-sig"),
    )
    return listings_df, reviews_df


def stage_to_sql(conn, listings_df: pd.DataFrame, reviews_df: pd.DataFrame, load_id: str) -> None:
    """
    Clear staging tables and load the two DataFrames into staging_listings and staging_reviews.
    Uses parameterized bulk execution (no low-level driver objects in this module).
    """
    run_sql(conn, "DELETE FROM staging_listings")
    run_sql(conn, "DELETE FROM staging_reviews")

    listings_df = listings_df.copy()
    listings_df["load_id"] = load_id
    listings_df["row_num"] = range(1, len(listings_df) + 1)

    cols_listings = [
        "id", "name", "host_id", "host_name", "neighbourhood_group", "neighbourhood",
        "latitude", "longitude", "room_type", "price", "minimum_nights", "number_of_reviews",
        "last_review", "reviews_per_month", "calculated_host_listings_count",
        "availability_365", "number_of_reviews_ltm", "license", "load_id", "row_num",
    ]
    for c in cols_listings:
        if c not in listings_df.columns and c in ("load_id", "row_num"):
            continue
        if c not in listings_df.columns:
            listings_df[c] = None

    use_listings = [c for c in cols_listings if c in listings_df.columns]
    insert_listings_sql = """
        INSERT INTO staging_listings ( id, name, host_id, host_name, neighbourhood_group, neighbourhood,
            latitude, longitude, room_type, price, minimum_nights, number_of_reviews,
            last_review, reviews_per_month, calculated_host_listings_count,
            availability_365, number_of_reviews_ltm, license, load_id, row_num )
        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
    """
    rows_listings = [
        tuple(row.get(c) if pd.notna(row.get(c)) else None for c in use_listings)
        for _, row in listings_df[use_listings].iterrows()
    ]
    if rows_listings:
        run_many(conn, insert_listings_sql, rows_listings)

    reviews_df = reviews_df.copy()
    reviews_df["load_id"] = load_id
    reviews_df["row_num"] = range(1, len(reviews_df) + 1)
    rev_cols = ["listing_id", "date", "load_id", "row_num"]
    for c in rev_cols:
        if c not in reviews_df.columns and c in ("load_id", "row_num"):
            continue
        if c not in reviews_df.columns:
            reviews_df[c] = None
    use_reviews = [c for c in rev_cols if c in reviews_df.columns]
    insert_reviews_sql = """
        INSERT INTO staging_reviews ( listing_id, [date], load_id, row_num ) VALUES ( ?, ?, ?, ? )
    """
    rows_reviews = [
        tuple(row.get(c) if pd.notna(row.get(c)) else None for c in use_reviews)
        for _, row in reviews_df[use_reviews].iterrows()
    ]
    if rows_reviews:
        run_many(conn, insert_reviews_sql, rows_reviews)

    conn.commit()
