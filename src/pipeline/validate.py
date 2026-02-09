"""
Validate: Data quality checks on staging data.
Schema, uniqueness, completeness, range, referential integrity.
Writes output/data_quality_report.json with pass/fail and counts.
"""
import json
import os
from datetime import datetime
from pathlib import Path

from src.utils.db_helpers import query_one, query_all

_PROJECT_ROOT = os.getenv("PROJECT_ROOT") or Path(__file__).resolve().parents[2]


def load_config() -> dict:
    import yaml
    config_path = Path(_PROJECT_ROOT) / "src" / "config" / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_validation(conn, config: dict | None = None) -> dict:
    """
    Run all DQ checks against staging_listings and staging_reviews.
    Returns a report dict suitable for data_quality_report.json.
    """
    cfg = config or load_config()
    dq = cfg.get("data_quality", {})
    report = {
        "run_ts": datetime.utcnow().isoformat() + "Z",
        "overall_pass": True,
        "checks": [],
        "summary": {"total_listings": 0, "valid_listings": 0, "total_reviews": 0, "valid_reviews": 0},
    }

    total_listings = query_one(conn, "SELECT COUNT(*) FROM staging_listings")
    report["summary"]["total_listings"] = total_listings[0] if total_listings else 0

    # Listings: schema
    staging_cols = {row[0].lower() for row in query_all(conn, """
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'staging_listings'
    """)}
    required_listing = [c.lower() for c in dq.get("required_listing_columns", ["id", "host_id", "neighbourhood", "room_type"])]
    missing = [c for c in required_listing if c not in staging_cols]
    schema_listings_ok = len(missing) == 0
    report["checks"].append({
        "name": "listings_schema",
        "pass": schema_listings_ok,
        "message": "Required columns present" if schema_listings_ok else f"Missing columns: {missing}",
        "details": {"required": required_listing, "missing": missing},
    })
    if not schema_listings_ok:
        report["overall_pass"] = False

    # Listings: uniqueness
    n_list = (query_one(conn, "SELECT COUNT(*) FROM staging_listings") or (0,))[0]
    n_distinct = (query_one(conn, "SELECT COUNT(DISTINCT id) FROM staging_listings WHERE id IS NOT NULL") or (0,))[0]
    uniq_listings_ok = n_list == 0 or n_list == n_distinct
    report["checks"].append({
        "name": "listings_unique_id",
        "pass": uniq_listings_ok,
        "message": "No duplicate listing ids" if uniq_listings_ok else f"Duplicate ids: {n_list} rows, {n_distinct} distinct",
        "details": {"total_rows": n_list, "distinct_id": n_distinct},
    })
    if not uniq_listings_ok:
        report["overall_pass"] = False

    # Listings: completeness
    incomplete_listings = (query_one(conn, """
        SELECT COUNT(*) FROM staging_listings
        WHERE id IS NULL OR host_id IS NULL OR room_type IS NULL
    """) or (0,))[0]
    complete_listings_ok = incomplete_listings == 0
    report["checks"].append({
        "name": "listings_completeness",
        "pass": complete_listings_ok,
        "message": "Required fields non-null" if complete_listings_ok else f"Rows with missing id/host_id/room_type: {incomplete_listings}",
        "details": {"incomplete_count": incomplete_listings},
    })
    if not complete_listings_ok:
        report["overall_pass"] = False

    # Listings: range
    price_min = dq.get("price_min", 0)
    price_max = dq.get("price_max", 100000)
    lat_min, lat_max = dq.get("latitude_min", -90), dq.get("latitude_max", 90)
    lon_min, lon_max = dq.get("longitude_min", -180), dq.get("longitude_max", 180)
    av_max = dq.get("availability_365_max", 365)

    range_violations = (query_one(conn, """
        SELECT COUNT(*) FROM staging_listings
        WHERE (price IS NOT NULL AND (TRY_CAST(REPLACE(REPLACE(price,'$',''),',','') AS NUMERIC(12,2)) NOT BETWEEN ? AND ? OR TRY_CAST(REPLACE(REPLACE(price,'$',''),',','') AS NUMERIC(12,2)) < 0))
           OR (latitude IS NOT NULL AND (latitude NOT BETWEEN ? AND ?))
           OR (longitude IS NOT NULL AND (longitude NOT BETWEEN ? AND ?))
           OR (availability_365 IS NOT NULL AND (availability_365 < 0 OR availability_365 > ?))
    """, (price_min, price_max, lat_min, lat_max, lon_min, lon_max, av_max)) or (0,))[0]
    range_ok = range_violations == 0
    report["checks"].append({
        "name": "listings_range",
        "pass": range_ok,
        "message": "Price/lat/lon/availability in valid range" if range_ok else f"Rows with range violations: {range_violations}",
        "details": {"violations": range_violations},
    })
    if not range_ok:
        report["overall_pass"] = False

    total_reviews = query_one(conn, "SELECT COUNT(*) FROM staging_reviews")
    report["summary"]["total_reviews"] = total_reviews[0] if total_reviews else 0

    # Reviews: schema
    rev_cols = {row[0].lower() for row in query_all(conn, """
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'staging_reviews'
    """)}
    required_review = [c.lower() for c in dq.get("required_review_columns", ["listing_id", "date"])]
    missing_rev = [c for c in required_review if c not in rev_cols]
    schema_reviews_ok = len(missing_rev) == 0
    report["checks"].append({
        "name": "reviews_schema",
        "pass": schema_reviews_ok,
        "message": "Required columns present" if schema_reviews_ok else f"Missing: {missing_rev}",
        "details": {"required": required_review, "missing": missing_rev},
    })
    if not schema_reviews_ok:
        report["overall_pass"] = False

    # Reviews: uniqueness
    nr = (query_one(conn, "SELECT COUNT(*) FROM staging_reviews") or (0,))[0]
    nd = (query_one(conn, "SELECT COUNT(*) FROM (SELECT listing_id, [date] FROM staging_reviews GROUP BY listing_id, [date]) x") or (0,))[0]
    uniq_reviews_ok = nr == 0 or nr == nd
    report["checks"].append({
        "name": "reviews_unique_listing_date",
        "pass": uniq_reviews_ok,
        "message": "No duplicate (listing_id, date)" if uniq_reviews_ok else f"Duplicates: {nr} rows, {nd} distinct (listing_id, date)",
        "details": {"total_rows": nr, "distinct_pairs": nd},
    })
    if not uniq_reviews_ok:
        report["overall_pass"] = False

    # Reviews: completeness
    incomplete_reviews = (query_one(conn, "SELECT COUNT(*) FROM staging_reviews WHERE listing_id IS NULL OR [date] IS NULL") or (0,))[0]
    complete_reviews_ok = incomplete_reviews == 0
    report["checks"].append({
        "name": "reviews_completeness",
        "pass": complete_reviews_ok,
        "message": "listing_id and date non-null" if complete_reviews_ok else f"Rows with null: {incomplete_reviews}",
        "details": {"incomplete_count": incomplete_reviews},
    })
    if not complete_reviews_ok:
        report["overall_pass"] = False

    # Referential integrity
    orphan_reviews = (query_one(conn, """
        SELECT COUNT(DISTINCT r.listing_id) FROM staging_reviews r
        LEFT JOIN staging_listings l ON r.listing_id = l.id
        WHERE l.id IS NULL AND r.listing_id IS NOT NULL
    """) or (0,))[0]
    ref_ok = orphan_reviews == 0
    report["checks"].append({
        "name": "referential_integrity",
        "pass": ref_ok,
        "message": "All review listing_ids exist in listings" if ref_ok else f"Orphan listing_ids in reviews: {orphan_reviews}",
        "details": {"orphan_listing_id_count": orphan_reviews},
    })
    if not ref_ok:
        report["overall_pass"] = False

    valid_listings = query_one(conn, """
        SELECT COUNT(*) FROM staging_listings
        WHERE id IS NOT NULL AND host_id IS NOT NULL AND room_type IS NOT NULL
        AND (latitude IS NULL OR (latitude BETWEEN ? AND ?))
        AND (longitude IS NULL OR (longitude BETWEEN ? AND ?))
        AND (availability_365 IS NULL OR (availability_365 >= 0 AND availability_365 <= ?))
    """, (lat_min, lat_max, lon_min, lon_max, av_max))
    report["summary"]["valid_listings"] = valid_listings[0] if valid_listings else 0

    valid_reviews = query_one(conn, """
        SELECT COUNT(*) FROM staging_reviews r
        INNER JOIN staging_listings l ON r.listing_id = l.id
        WHERE r.listing_id IS NOT NULL AND r.[date] IS NOT NULL
    """)
    report["summary"]["valid_reviews"] = valid_reviews[0] if valid_reviews else 0

    return report


def write_report(report: dict, output_dir: str = "output") -> str:
    path = Path(_PROJECT_ROOT) / output_dir / "data_quality_report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    return str(path)
