-- =============================================================================
-- Query 2: Host Performance Ranking (SQL Server)
-- Ranks hosts by a composite performance score (revenue potential, engagement, portfolio).
-- =============================================================================
--
-- BUSINESS LOGIC & ASSUMPTIONS
--   - Revenue potential: price * (availability_365/365) as proxy for bookable revenue per listing; summed per host.
--   - Engagement: number_of_reviews + number_of_reviews_ltm as volume signal (no rating column in schema).
--   - Portfolio size: calculated_host_listings_count (scale of operation).
--   - Composite score: 40% revenue_score + 40% engagement_score + 20% portfolio_score, each min-max normalized to 0–100.
--
-- EDGE CASES & NULL HANDLING
--   - COALESCE(availability_365, 0), COALESCE(number_of_reviews, 0), COALESCE(number_of_reviews_ltm, 0) so NULLs don’t break aggregates.
--   - NULLIF in normalization avoids division by zero when all hosts have the same value (score becomes 0).
--   - Listings with NULL or price <= 0 excluded from latest_snapshot so revenue is meaningful.
--
-- OPTIMIZATION
--   - Window functions (MIN/MAX OVER ()) compute normalization in one pass; consider materialized view if run frequently.
--   - Index on fact_listing_snapshots(load_date, host_sk) and dim_host(host_id, is_current) supports the joins.
-- =============================================================================

WITH latest_snapshot AS (
    SELECT
        f.host_sk,
        f.listing_sk,
        f.price,
        f.availability_365,
        f.number_of_reviews,
        f.number_of_reviews_ltm,
        f.calculated_host_listings_count,
        f.estimated_revenue_365
    FROM fact_listing_snapshots f
    WHERE f.load_date = (SELECT MAX(load_date) FROM fact_listing_snapshots)
      AND f.price IS NOT NULL
      AND f.price > 0
),
host_agg AS (
    SELECT
        h.host_id,
        MAX(h.host_name) AS host_name,
        COUNT(DISTINCT ls.listing_sk) AS listing_count,
        SUM(ls.price * COALESCE(ls.availability_365, 0) / 365.0) AS revenue_potential_raw,
        SUM(COALESCE(ls.number_of_reviews, 0) + COALESCE(ls.number_of_reviews_ltm, 0)) AS total_review_activity,
        MAX(ls.calculated_host_listings_count) AS portfolio_size
    FROM dim_host h
    INNER JOIN latest_snapshot ls ON h.host_sk = ls.host_sk AND h.is_current = 1
    GROUP BY h.host_id
),
norm AS (
    SELECT
        host_id,
        host_name,
        listing_count,
        revenue_potential_raw,
        total_review_activity,
        portfolio_size,
        (revenue_potential_raw - MIN(revenue_potential_raw) OVER ()) * 100.0
            / NULLIF(MAX(revenue_potential_raw) OVER () - MIN(revenue_potential_raw) OVER (), 0) AS revenue_score,
        (total_review_activity - MIN(total_review_activity) OVER ()) * 100.0
            / NULLIF(MAX(total_review_activity) OVER () - MIN(total_review_activity) OVER (), 0) AS engagement_score,
        (portfolio_size - MIN(portfolio_size) OVER ()) * 100.0
            / NULLIF(MAX(portfolio_size) OVER () - MIN(portfolio_size) OVER (), 0) AS portfolio_score
    FROM host_agg
),
scored AS (
    SELECT
        host_id,
        host_name,
        COALESCE(revenue_score, 0) * 0.4 + COALESCE(engagement_score, 0) * 0.4 + COALESCE(portfolio_score, 0) * 0.2 AS performance_score,
        ROUND(revenue_potential_raw, 2) AS revenue_potential,
        total_review_activity,
        portfolio_size,
        CONCAT(
            'revenue_potential=', ROUND(revenue_potential_raw, 0),
            '; reviews=', total_review_activity,
            '; portfolio_size=', portfolio_size
        ) AS key_metrics_breakdown
    FROM norm
)
SELECT
    host_id,
    host_name,
    ROUND(performance_score, 2) AS performance_score,
    DENSE_RANK() OVER (ORDER BY performance_score DESC) AS ranking,
    key_metrics_breakdown
FROM scored
ORDER BY ranking;
