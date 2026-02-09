# Data Engineer Test – Airbnb Analytics (SQL Server)

Solution for the [JelouLatam data-engineer-test-01](https://github.com/JelouLatam/data-engineer-test-01) challenge: **Part 1** (Data Warehouse Design), **Part 2** (SQL Analytics), and **Part 3** (ETL pipeline) on **SQL Server**.

---

## What's included

| Item | Description |
|------|-------------|
| **sql/schema.sql** | Part 1: DDL (dimensions SCD Type 2, facts, staging, indexes) for SQL Server. |
| **sql/queries/*.sql** | Part 2: Pricing, host performance, market opportunity queries. |
| **src/pipeline/** | Part 3: extract, validate, transform, load, orchestrator. |
| **src/config/config.yaml** | Paths, DQ rules, pricing tiers (no hardcoded credentials). |
| **src/utils/** | db_connector, db_helpers, logger. |
| **tests/test_*.py** | Unit tests for pipeline. |
| **SOLUTION.md** | Part 1 design doc. |
| **data/data_dictionary.md** | Column definitions for listings.csv and reviews.csv. |

---

## Part 1: Run the schema

1. Open **SQL Server Management Studio** (or Azure Data Studio) and connect to your instance.
2. Create a database (e.g. `CREATE DATABASE airbnb;`) and set your connection to use it.
3. Open **sql/schema.sql** and execute the whole script (F5). The script is idempotent (`IF NOT EXISTS`).

---

## Part 2: Run the analytics queries

After the schema is created and **data is loaded** (via Part 3 ETL), run each query in **sql/queries/**:

- **01_pricing_intelligence.sql** — listing_id, current_price, market_average, price_difference_pct, recommendation.
- **02_host_performance.sql** — host_id, host_name, performance_score, ranking, key_metrics_breakdown.
- **03_market_opportunities.sql** — neighbourhood, demand_score, supply_score, opportunity_score, recommended_action.

---

## Part 3: Run the ETL pipeline

**Before running:** (1) Schema applied (Part 1), (2) `data/listings.csv` and `data/reviews.csv` exist, (3) `.env` configured.

1. **Data**: Place `listings.csv` and `reviews.csv` in the **data/** folder (from the [repo](https://github.com/JelouLatam/data-engineer-test-01) or Inside Airbnb).

2. **Environment**: Copy `.env.example` to `.env` and set your SQL Server connection (DB_TYPE, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_DRIVER).

3. **Run** from the project root:
   ```bash
   pip install -r requirements.txt
   python -m src.pipeline.orchestrator
   ```

4. **Output**: `output/data_quality_report.json`, `logs/` execution log, and warehouse tables populated.

Pipeline flow: **Extract** → **Validate** → **Transform** → **Load**.
