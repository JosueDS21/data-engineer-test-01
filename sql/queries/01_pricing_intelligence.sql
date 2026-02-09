-- =============================================================================
-- Query 1: Pricing Intelligence (SQL Server)
-- Identifies properties over/under-priced vs similar listings (same neighbourhood + room_type).
-- =============================================================================
--
-- BUSINESS LOGIC & ASSUMPTIONS
--   - "Similar" = same neighbourhood + room_type (comparable market segment).
--   - Market average = AVG(price) per (neighbourhood, room_type). NULL/zero prices excluded.
--   - Recommendation: underpriced if price < market*(1 - threshold), overpriced if > market*(1 + threshold).
--   - Threshold: 15% (fair band). Configurable via CTE params.
--
-- EDGE CASES & NULL HANDLING
--   - Listings with NULL or price <= 0 excluded from market_stats and from output.
--   - NULLIF(market_avg_price, 0) avoids division by zero when computing price_difference_pct.
--   - Minimum 3 comparables per segment (HAVING COUNT(*) >= 3) so average is meaningful.
--
-- OPTIMIZATION
--   - Single pass for latest load_date via (SELECT MAX(load_date) FROM fact_listing_snapshots).
--   - Indexes on fact_listing_snapshots(load_date, neighbourhood_sk, room_type_sk) and
--     dim_* is_current support the filters. Consider persisted MAX(load_date) if run very often.
-- =============================================================================

WITH params AS (
    SELECT CAST(0.15 AS NUMERIC(5,2)) AS price_threshold_pct  -- 15% band for "fair"
),
market_stats AS (
    SELECT
        n.neighbourhood,
        r.room_type,
        AVG(f.price) AS market_avg_price,
        COUNT(*)    AS comparable_count
    FROM fact_listing_snapshots f
    INNER JOIN dim_listing      l ON f.listing_sk = l.listing_sk AND l.is_current = 1
    INNER JOIN dim_neighbourhood n ON f.neighbourhood_sk = n.neighbourhood_sk AND n.is_current = 1
    INNER JOIN dim_room_type     r ON f.room_type_sk = r.room_type_sk
    WHERE f.load_date = (SELECT MAX(load_date) FROM fact_listing_snapshots)
      AND f.price IS NOT NULL
      AND f.price > 0
    GROUP BY n.neighbourhood, r.room_type
    HAVING COUNT(*) >= 3
),
listing_current AS (
    SELECT
        l.listing_id,
        f.price          AS current_price,
        n.neighbourhood,
        r.room_type,
        m.market_avg_price,
        m.comparable_count
    FROM fact_listing_snapshots f
    INNER JOIN dim_listing      l ON f.listing_sk = l.listing_sk AND l.is_current = 1
    INNER JOIN dim_neighbourhood n ON f.neighbourhood_sk = n.neighbourhood_sk AND n.is_current = 1
    INNER JOIN dim_room_type     r ON f.room_type_sk = r.room_type_sk
    INNER JOIN market_stats     m ON n.neighbourhood = m.neighbourhood AND r.room_type = m.room_type
    WHERE f.load_date = (SELECT MAX(load_date) FROM fact_listing_snapshots)
),
with_diff AS (
    SELECT
        listing_id,
        current_price,
        market_avg_price AS market_average,
        ROUND(
            (current_price - market_avg_price) * 100.0 / NULLIF(market_avg_price, 0),
            2
        ) AS price_difference_pct,
        (SELECT price_threshold_pct FROM params) AS threshold
    FROM listing_current
    WHERE current_price IS NOT NULL AND current_price > 0
)
SELECT
    listing_id,
    current_price,
    market_average,
    price_difference_pct,
    CASE
        WHEN price_difference_pct <= -threshold * 100 THEN 'underpriced'
        WHEN price_difference_pct >= threshold * 100  THEN 'overpriced'
        ELSE 'fair'
    END AS recommendation
FROM with_diff
ORDER BY price_difference_pct DESC;
