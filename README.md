# Data Engineering

## Overview

Build an analytics solution for [Airbnb listing data](https://insideairbnb.com/get-the-data/). This challenge evaluates your skills in data modeling, pipeline engineering, and analytical SQL.

---

## Business Context

A vacation rental startup needs to understand their competitive landscape through data. You'll build a data warehouse solution to answer:

1. **Pricing Strategy** - How do prices vary by location and property characteristics?
2. **Host Performance** - What factors drive successful hosts?
3. **Market Intelligence** - Where are the untapped opportunities?

---

## Dataset

This repository contains sample Airbnb data:

```
data/
├── listings.csv          # 1,000+ property listings
├── reviews.csv           # 5,000+ guest reviews
└── data_dictionary.md    # Column descriptions
```

---

## Requirements

### 1. Data Warehouse Design

Design a dimensional model following star schema principles.

**Deliverables:**

```
sql/
└── schema.sql            # Complete DDL for your dimensional model
```

**Documentation required in `SOLUTION.md`:**

- Fact table grain selection and rationale
- Dimension design decisions
- How you handle slowly changing dimensions (SCD)
- Indexing and partitioning strategy
- Tradeoffs and alternatives considered

**What we evaluate:**

- Understanding of dimensional modeling (Kimball methodology)
- Appropriate grain selection
- Consideration of query patterns and performance
- Clear justification of design decisions

---

### 2. SQL Analytics

Write production-quality SQL queries answering these business questions.

**Deliverables:**

```
sql/
└── queries/
    ├── 01_pricing_intelligence.sql
    ├── 02_host_performance.sql
    └── 03_market_opportunities.sql
```

**Query 1: Pricing Intelligence**

```
Identify properties that are significantly over/under-priced
compared to similar listings (same neighborhood + property type).

Output: listing_id, current_price, market_average, price_difference_pct,
        recommendation (underpriced/fair/overpriced)
```

**Query 2: Host Performance Ranking**

```
Rank hosts by performance using a composite score you define.
Consider: revenue potential, ratings, response metrics, portfolio size.

Output: host_id, host_name, performance_score, ranking,
        key_metrics_breakdown
```

**Query 3: Market Opportunity Analysis**

```
Find neighborhoods with strong demand signals but limited supply.
Define clear metrics for "demand" and "supply".

Output: neighborhood, demand_score, supply_score, opportunity_score,
        recommended_action
```

**Requirements:**

- Use advanced SQL: window functions, CTEs, complex aggregations
- Include query optimization considerations
- Handle edge cases and null values
- Document your business logic and assumptions

---

### 3. Data Pipeline Implementation

Build a production-ready ETL/ELT pipeline that loads the raw data into your warehouse.

**Pipeline Architecture:**

```
Extract → Validate → Transform → Load
```

**Required Components:**

**a) Data Extraction**

- Read CSV files from source
- Handle file encoding and formats
- Stage raw data with minimal transformation

**b) Data Validation**

- Implement comprehensive data quality checks:
  - Schema validation (expected columns, data types)
  - Uniqueness constraints (no duplicate IDs)
  - Completeness checks (critical fields non-null)
  - Range validation (prices > 0, coordinates valid)
  - Referential integrity (foreign key relationships)
- Generate data quality report with pass/fail metrics
- Handle invalid records gracefully (log, quarantine, continue)

**c) Data Transformation**

- Clean and standardize data:
  - Parse amenities from strings to structured format
  - Normalize price formats
  - Handle boolean conversions
  - Standardize text fields
- Feature engineering:
  - Calculate estimated revenue per listing
  - Derive occupancy rates
  - Extract amenity flags (wifi, kitchen, parking, etc.)
  - Create price tier classifications
- Implement SCD Type 2 for changing dimensions
- Generate surrogate keys

**d) Data Loading**

- Upsert to dimension tables with SCD logic
- Insert to fact tables (append-only)
- Maintain referential integrity
- Include error handling and rollback capability
- Ensure idempotent operations

**Code Quality Requirements:**

- Modular, reusable code structure
- Proper error handling and logging
- Configuration management (no hardcoded values)
- Clear documentation and comments

**Deliverables:**

```
src/
├── pipeline/
│   ├── extract.py
│   ├── validate.py
│   ├── transform.py
│   ├── load.py
│   └── orchestrator.py
├── config/
│   └── config.yaml
└── utils/
    ├── db_connector.py
    └── logger.py

tests/
└── test_*.py                 # Unit tests for pipeline components

output/
└── data_quality_report.json

logs/
└── pipeline_execution.log

requirements.txt              # Python dependencies
.env.example                  # Environment variables template
```

---

## Bonus Challenge (Optional)

**Advanced: End-to-End Orchestration & Observability**

Take your pipeline to production-grade:

1. **Orchestration**: Implement workflow management using Airflow, Prefect, or Dagster

   - Task dependencies and scheduling
   - Retry logic and failure handling
   - Backfill capabilities

2. **Observability**: Add comprehensive monitoring

   - Pipeline execution metrics (duration, records processed)
   - Data quality metrics over time
   - Alerting on failures or anomalies
   - Dashboard for pipeline health

3. **Containerization**: Docker setup for reproducibility
   - Multi-container setup (database, pipeline, orchestrator)
   - Docker Compose configuration
   - Environment variable management

**Deliverables:**

- `dags/` or `flows/` - Orchestration workflows
- `docker-compose.yml` - Container configuration
- `monitoring/` - Observability dashboards/configs
- Documentation on running the full stack

---

## Technology Stack

Use tools that best showcase your expertise. Here are current industry standards:

**Data Warehouses:**

- Snowflake, BigQuery, Redshift, Databricks SQL (cloud)
- PostgreSQL, DuckDB, SingleStore, ClickHouse (local/hybrid)

**Processing & Transformation:**

- Python (Pandas, Polars, PySpark)
- dbt (highly valued for transformation workflows)
- SQL (native warehouse capabilities)

**Data Quality:**

- Great Expectations, Soda Core, Pandera
- Custom validation frameworks

**Orchestration:**

- Apache Airflow, Prefect, Dagster, Mage

**Note:** Choose the stack where you can demonstrate depth, not breadth. We want to see mastery of your chosen tools.

---

## Deliverables

Your submission must include:

```
data-engineer-test-01/
├── README.md                 # Setup and execution instructions
├── SOLUTION.md               # Your design decisions and approach
├── requirements.txt          # Python dependencies
├── .env.example              # Environment variables template
│
├── data/                     # Provided dataset (do not modify)
│   ├── listings.csv
│   ├── reviews.csv
│   └── data_dictionary.md
│
├── sql/
│   ├── schema.sql
│   └── queries/
│       ├── 01_pricing_intelligence.sql
│       ├── 02_host_performance.sql
│       └── 03_market_opportunities.sql
│
├── src/
│   ├── pipeline/
│   │   ├── extract.py
│   │   ├── validate.py
│   │   ├── transform.py
│   │   ├── load.py
│   │   └── orchestrator.py
│   ├── config/
│   │   └── config.yaml
│   └── utils/
│       ├── db_connector.py
│       └── logger.py
│
├── tests/                    # Unit tests for pipeline
│   └── test_*.py
│
├── output/
│   └── data_quality_report.json
│
├── logs/
│    └── pipeline_execution.log
│
├── docker-compose.yml        # Container orchestration
├── Dockerfile                # Container definition
├── dags/                     # Airflow DAGs
│   └── etl_pipeline.py
└── monitoring/               # Observability configs
    └── dashboards.json
```

---

## Evaluation Criteria

We will assess your submission based on:

- **Data modeling quality** - Schema design, grain selection, SCD implementation
- **Pipeline engineering** - Code quality, modularity, error handling, data validation
- **SQL proficiency** - Query correctness, advanced techniques, optimization awareness
- **Code readability and structure** - Clean architecture, proper organization, maintainability
- **Clarity of documentation** - Clear explanations, justified decisions, comprehensive SOLUTION.md

---

## Submission Process

1. **Fork this repository**

2. **Implement your solution** in your forked repo

3. **Test thoroughly** - ensure someone can run your solution following your README

4. **Create a Pull Request**

---

**We're excited to see your solution! Show us how you approach real-world data engineering challenges. 🚀**
