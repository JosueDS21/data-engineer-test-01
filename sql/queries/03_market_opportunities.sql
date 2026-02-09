-- =============================================================================
-- Query 3: Market Opportunity Analysis (SQL Server)
-- Neighbourhoods with strong demand but limited supply (opportunity score).
-- =============================================================================
--
-- BUSINESS LOGIC & ASSUMPTIONS
--   - Demand: total review activity (number_of_reviews + number_of_reviews_ltm) across listings in the neighbourhood.
--   - Supply: count of distinct listings in the neighbourhood.
--   - Opportunity score = demand_score − supply_score (min-max normalized each to 0–100). High = demand outstrips supply.
--   - Recommended actions: expand supply when high demand + low supply; saturated when low demand + high supply.
--
-- EDGE CASES & NULL HANDLING
--   - COALESCE(number_of_reviews, 0), COALESCE(number_of_reviews_ltm, 0) so NULLs don’t break SUM/AVG.
--   - NULLIF in normalization avoids division by zero when all neighbourhoods have the same total_reviews or supply_listings.
--   - Neighbourhoods with no listings (or not in latest load) are not in the result set.
--
-- OPTIMIZATION
--   - Single scan of fact for latest load_date; index on (load_date, neighbourhood_sk) helps.
--   - Window functions for min-max normalization in one pass.
-- =============================================================================

WITH latest AS (
    SELECT
        f.neighbourhood_sk,
        f.listing_sk,
        f.number_of_reviews,
        f.number_of_reviews_ltm,
        f.availability_365
    FROM fact_listing_snapshots f
    WHERE f.load_date = (SELECT MAX(load_date) FROM fact_listing_snapshots)
),
neighbourhood_metrics AS (
    SELECT
        n.neighbourhood,
        COUNT(DISTINCT l.listing_sk) AS supply_listings,
        SUM(COALESCE(l.number_of_reviews, 0) + COALESCE(l.number_of_reviews_ltm, 0)) AS total_reviews,
        AVG(COALESCE(l.number_of_reviews, 0) + COALESCE(l.number_of_reviews_ltm, 0)) AS avg_reviews_per_listing,
        SUM(CASE WHEN COALESCE(l.availability_365, 0) > 0 THEN 1 ELSE 0 END) AS active_listings
    FROM dim_neighbourhood n
    INNER JOIN latest l ON n.neighbourhood_sk = l.neighbourhood_sk AND n.is_current = 1
    GROUP BY n.neighbourhood
),
scaled AS (
    SELECT
        neighbourhood,
        supply_listings,
        total_reviews,
        avg_reviews_per_listing,
        (total_reviews - MIN(total_reviews) OVER ()) * 100.0
            / NULLIF(MAX(total_reviews) OVER () - MIN(total_reviews) OVER (), 0) AS demand_score,
        (supply_listings - MIN(supply_listings) OVER ()) * 100.0
            / NULLIF(MAX(supply_listings) OVER () - MIN(supply_listings) OVER (), 0) AS supply_score
    FROM neighbourhood_metrics
),
opportunity AS (
    SELECT
        neighbourhood,
        ROUND(demand_score, 2) AS demand_score,
        ROUND(supply_score, 2) AS supply_score,
        ROUND(demand_score - supply_score, 2) AS opportunity_score,
        CASE
            WHEN demand_score > 60 AND supply_score < 40 THEN 'Expand supply - high demand, room to grow'
            WHEN demand_score > 50 AND supply_score < 50 THEN 'Consider adding listings'
            WHEN demand_score < 30 AND supply_score > 70 THEN 'Saturated - differentiate or avoid'
            ELSE 'Monitor - balanced market'
        END AS recommended_action
    FROM scaled
)
SELECT
    neighbourhood,
    demand_score,
    supply_score,
    opportunity_score,
    recommended_action
FROM opportunity
ORDER BY opportunity_score DESC;
