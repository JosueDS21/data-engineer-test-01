"""
Load: Upsert dimensions (SCD Type 2), insert fact tables (append-only).
Ensures dim_date is populated; resolves surrogate keys from cleaned DataFrames.
All database access uses db_helpers (no low-level driver objects in this module).
"""
from datetime import date, timedelta

import pandas as pd

from src.utils.db_helpers import run_sql, query_one, run_many, run_insert_return_id


def ensure_dim_date(conn, min_date: date, max_date: date) -> None:
    """Insert into dim_date any missing dates in [min_date, max_date]."""
    d = min_date
    while d <= max_date:
        row = query_one(conn, "SELECT date_sk FROM dim_date WHERE full_date = ?", (d,))
        if row is None:
            q = (d.month - 1) // 3 + 1
            dow = d.weekday() + 1
            is_weekend = 1 if dow >= 6 else 0
            run_sql(conn, """
                INSERT INTO dim_date (full_date, year, month, day, quarter, day_of_week, is_weekend)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (d, d.year, d.month, d.day, q, dow, is_weekend))
        d += timedelta(days=1)
    conn.commit()


def get_or_insert_room_type(conn, room_type: str) -> int:
    """Return room_type_sk for room_type; insert if not exists."""
    row = query_one(conn, "SELECT room_type_sk FROM dim_room_type WHERE room_type = ?", (room_type or "Unknown",))
    if row:
        return row[0]
    sk = run_insert_return_id(conn, "INSERT INTO dim_room_type (room_type) VALUES (?)", (room_type or "Unknown",))
    conn.commit()
    return sk


def get_or_insert_host_scd2(conn, host_id: int, host_name: str | None) -> int:
    """Return host_sk for (host_id, host_name). SCD2: insert new version if name changed."""
    row = query_one(conn, "SELECT host_sk, host_name FROM dim_host WHERE host_id = ? AND is_current = 1", (host_id,))
    host_name = host_name if host_name is not None and str(host_name).strip() else None
    if row:
        existing_sk, existing_name = row[0], (row[1] or "").strip()
        if (existing_name or "") == (host_name or ""):
            return existing_sk
        run_sql(conn, "UPDATE dim_host SET effective_to = SYSDATETIME(), is_current = 0 WHERE host_sk = ?", (existing_sk,))
    sk = run_insert_return_id(conn, """
        INSERT INTO dim_host (host_id, host_name, effective_from, effective_to, is_current)
        VALUES (?, ?, SYSDATETIME(), NULL, 1)
    """, (host_id, host_name))
    conn.commit()
    return sk


def get_or_insert_neighbourhood_scd2(conn, neighbourhood: str | None, neighbourhood_group: str | None) -> int:
    """Return neighbourhood_sk. SCD2: insert new version if attributes changed."""
    n = (neighbourhood or "").strip() or None
    g = (neighbourhood_group or "").strip() or None
    row = query_one(conn, """
        SELECT TOP 1 neighbourhood_sk, neighbourhood, neighbourhood_group FROM dim_neighbourhood
        WHERE is_current = 1 AND ISNULL(neighbourhood,'') = ISNULL(?,'') AND ISNULL(neighbourhood_group,'') = ISNULL(?,'')
    """, (n, g))
    if row:
        sk, en, eg = row[0], (row[1] or "").strip(), (row[2] or "").strip()
        if (en or "") == (n or "") and (eg or "") == (g or ""):
            return sk
        run_sql(conn, "UPDATE dim_neighbourhood SET effective_to = SYSDATETIME(), is_current = 0 WHERE neighbourhood_sk = ?", (sk,))
    sk = run_insert_return_id(conn, """
        INSERT INTO dim_neighbourhood (neighbourhood, neighbourhood_group, effective_from, effective_to, is_current)
        VALUES (?, ?, SYSDATETIME(), NULL, 1)
    """, (n, g))
    conn.commit()
    return sk


def get_or_insert_listing_scd2(conn, listing_id: int, name: str | None, latitude: float | None, longitude: float | None, license: str | None) -> int:
    """Return listing_sk. SCD2: insert new version if attributes changed."""
    name = (name or "").strip() or None if name is not None else None
    row = query_one(conn, """
        SELECT listing_sk, name, latitude, longitude, license FROM dim_listing WHERE listing_id = ? AND is_current = 1
    """, (listing_id,))
    if row:
        sk, en, ela, elo, eli = row[0], row[1], row[2], row[3], row[4]
        if (en or "") == (name or "") and _eq(ela, latitude) and _eq(elo, longitude) and (eli or "") == (license or "").strip():
            return sk
        run_sql(conn, "UPDATE dim_listing SET effective_to = SYSDATETIME(), is_current = 0 WHERE listing_sk = ?", (sk,))
    sk = run_insert_return_id(conn, """
        INSERT INTO dim_listing (listing_id, name, latitude, longitude, license, effective_from, effective_to, is_current)
        VALUES (?, ?, ?, ?, ?, SYSDATETIME(), NULL, 1)
    """, (listing_id, name, latitude, longitude, (license or "").strip() or None))
    conn.commit()
    return sk


def _eq(a, b) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) < 1e-9


def get_date_sk(conn, d: date) -> int | None:
    row = query_one(conn, "SELECT date_sk FROM dim_date WHERE full_date = ?", (d,))
    return row[0] if row else None


def get_listing_sk_current(conn, listing_id: int) -> int | None:
    row = query_one(conn, "SELECT listing_sk FROM dim_listing WHERE listing_id = ? AND is_current = 1", (listing_id,))
    return row[0] if row else None


def run_load(conn, listings_clean: pd.DataFrame, reviews_clean: pd.DataFrame, load_date: date) -> None:
    """
    Upsert dimensions from listings_clean (SCD2 where applicable), then insert fact_listing_snapshots and fact_reviews.
    Idempotent for same load_date: one snapshot per listing per load_date (unique on listing_sk, load_date).
    """
    dates = set()
    if "last_review" in listings_clean.columns:
        for v in listings_clean["last_review"].dropna():
            try:
                dates.add(pd.to_datetime(v).date())
            except Exception:
                pass
    if "review_date" in reviews_clean.columns:
        for v in reviews_clean["review_date"].dropna():
            try:
                dates.add(pd.to_datetime(v).date())
            except Exception:
                pass
    if not dates:
        dates.add(load_date)
    min_d, max_d = min(dates), max(dates)
    ensure_dim_date(conn, min_d, max_d)

    room_type_sk_cache = {}
    host_sk_cache = {}
    neighbourhood_sk_cache = {}
    listing_sk_cache = {}

    insert_snapshot_sql = """
        INSERT INTO fact_listing_snapshots (
            listing_sk, host_sk, neighbourhood_sk, room_type_sk,
            price, minimum_nights, number_of_reviews, last_review_date_sk,
            reviews_per_month, calculated_host_listings_count, availability_365, number_of_reviews_ltm,
            estimated_revenue_365, occupancy_rate, price_tier, load_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for _, row in listings_clean.iterrows():
        lid = row.get("id")
        if pd.isna(lid):
            continue
        lid = int(lid)
        host_id = row.get("host_id")
        if pd.isna(host_id):
            continue
        host_id = int(host_id)

        rt = (row.get("room_type") or "Unknown").strip()
        if rt not in room_type_sk_cache:
            room_type_sk_cache[rt] = get_or_insert_room_type(conn, rt)
        rtsk = room_type_sk_cache[rt]

        key_h = (host_id, (row.get("host_name") or "").strip())
        if key_h not in host_sk_cache:
            host_sk_cache[key_h] = get_or_insert_host_scd2(conn, host_id, row.get("host_name"))
        hsk = host_sk_cache[key_h]

        n = (row.get("neighbourhood") or "").strip()
        g = (row.get("neighbourhood_group") or "").strip()
        key_n = (n, g)
        if key_n not in neighbourhood_sk_cache:
            neighbourhood_sk_cache[key_n] = get_or_insert_neighbourhood_scd2(conn, n or None, g or None)
        nsk = neighbourhood_sk_cache[key_n]

        lsk = get_or_insert_listing_scd2(
            conn,
            lid,
            row.get("name"),
            float(row["latitude"]) if not pd.isna(row.get("latitude")) else None,
            float(row["longitude"]) if not pd.isna(row.get("longitude")) else None,
            row.get("license"),
        )
        listing_sk_cache[lid] = lsk

        last_review_sk = None
        last_review_d = row.get("last_review")
        if last_review_d is not None and not pd.isna(last_review_d):
            try:
                last_review_sk = get_date_sk(conn, pd.to_datetime(last_review_d).date())
            except Exception:
                pass

        def _num(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            return int(v) if isinstance(v, (int, float)) and v == int(v) else int(v)

        def _float(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            return float(v)

        price = _float(row.get("price_clean"))
        min_nights = _num(row.get("minimum_nights"))
        num_rev = _num(row.get("number_of_reviews"))
        rev_per_month = _float(row.get("reviews_per_month"))
        host_listings = _num(row.get("calculated_host_listings_count"))
        avail = _num(row.get("availability_365"))
        rev_ltm = _num(row.get("number_of_reviews_ltm"))
        est_rev = _float(row.get("estimated_revenue_365"))
        occ = _float(row.get("occupancy_rate"))
        tier = ((row.get("price_tier") or "unknown") or "").strip()[:20]

        run_sql(conn, "DELETE FROM fact_listing_snapshots WHERE listing_sk = ? AND load_date = ?", (lsk, load_date))
        run_sql(conn, insert_snapshot_sql, (
            lsk, hsk, nsk, rtsk,
            price, min_nights, num_rev, last_review_sk,
            rev_per_month, host_listings, avail, rev_ltm,
            est_rev, occ, tier, load_date,
        ))

    conn.commit()

    review_rows = []
    for _, row in reviews_clean.iterrows():
        lid = row.get("listing_id")
        if pd.isna(lid):
            continue
        lid = int(lid)
        lsk = listing_sk_cache.get(lid) or get_listing_sk_current(conn, lid)
        if lsk is None:
            continue
        rd = row.get("review_date") if not pd.isna(row.get("review_date")) else row.get("date")
        if pd.isna(rd):
            continue
        try:
            d = pd.to_datetime(rd).date()
        except Exception:
            continue
        date_sk = get_date_sk(conn, d)
        if date_sk is None:
            ensure_dim_date(conn, d, d)
            date_sk = get_date_sk(conn, d)
        if date_sk is None:
            continue
        review_rows.append((lsk, date_sk, load_date))

    if review_rows:
        run_many(conn, "INSERT INTO fact_reviews (listing_sk, date_sk, load_date) VALUES (?, ?, ?)", review_rows)

    conn.commit()
