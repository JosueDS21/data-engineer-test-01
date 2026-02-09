# Part 1: Data Warehouse Design (SQL Server)

This document explains the **dimensional model** for the Airbnb analytics solution: how we chose the fact and dimensions, how we handle history (SCD), and how we index for performance.

---

## Source data (what we have)

- **listings.csv**: one row per property — id, host_id, host_name, neighbourhood, neighbourhood_group, room_type, price, minimum_nights, number_of_reviews, last_review, reviews_per_month, availability_365, etc.
- **reviews.csv**: one row per review — listing_id, date (no rating or text in this sample).

The business questions we need to support:

1. **Pricing** — How do prices vary by location and property type?
2. **Host performance** — What drives successful hosts (revenue, reviews, portfolio)?
3. **Market opportunities** — Where is demand high but supply low?

A **star schema** fits this: a few dimension tables (who, where, what type) and fact tables (measures and events) that we can slice by those dimensions.

---

## 1. Fact table grain and rationale

**Grain** = what one row in the fact table represents.

### Fact 1: Listing snapshot (property metrics)

- **Grain**: One row **per listing, per load date** (each time we run the ETL we add one row per listing).
- **Why**: We need "current" price, availability, review counts, etc., and we may want to compare over time. A **periodic snapshot** — one snapshot per run — gives us both. We never update old rows; we only **append** new snapshot rows.
- **Measures**: price, minimum_nights, number_of_reviews, reviews_per_month, availability_365, number_of_reviews_ltm, calculated_host_listings_count, estimated_revenue_365, occupancy_rate.

### Fact 2: Reviews (guest review events)

- **Grain**: One row **per review** (one row per listing_id + date in reviews.csv).
- **Why**: Reviews are events. One row per review is the natural grain and stays append-only.

| Fact table              | Grain              |
|-------------------------|--------------------|
| fact_listing_snapshots  | listing + load_date|
| fact_reviews            | one row per review |

---

## 2. Dimension design decisions

- **dim_host**: SCD Type 2 (host_name can change).
- **dim_neighbourhood**: SCD Type 2 (names can change).
- **dim_room_type**: Static lookup.
- **dim_listing**: SCD Type 2 (name, location, license can change).
- **dim_date**: Calendar for review dates and last_review.

---

## 3. Slowly changing dimensions (SCD)

- **Type 2** (host, neighbourhood, listing): On change, set effective_to and is_current=0 on old row; insert new row with effective_from, is_current=1.
- **Type 1 / static** (room_type, date): Insert once or overwrite.

---

## 4. Indexing and partitioning (SQL Server)

- Dimensions: index on business key; filtered index on is_current = 1.
- Facts: indexes on foreign keys and load_date.

---

## 5. Tradeoffs and alternatives

| Decision | Why |
|----------|-----|
| Snapshot grain per load | Compare metrics over time. |
| Surrogate keys | Isolate facts from attribute changes; SCD is cleaner. |
| Two fact tables | Keeps grain clear (listing state vs review events). |
| SCD Type 2 on host, neighbourhood, listing | "As of" history for reporting. |

---

## Deliverables for Part 1

- **sql/schema.sql** — Full DDL for SQL Server (dimensions, facts, staging, indexes).
- **SOLUTION.md** (this file) — Grain, dimensions, SCD, indexing, tradeoffs.
