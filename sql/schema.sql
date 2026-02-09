-- =============================================================================
-- Part 1: Data Warehouse Design - Star Schema (SQL Server)
-- =============================================================================
-- Source: listings.csv + reviews.csv (Airbnb)
-- Fact grain: (1) one row per listing per load_date, (2) one row per review.
-- Dimensions use surrogate keys; host, neighbourhood, listing use SCD Type 2.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DIMENSION: Host (SCD Type 2 — track name changes over time)
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_host')
BEGIN
    CREATE TABLE dim_host (
        host_sk           BIGINT IDENTITY(1,1) PRIMARY KEY,
        host_id           BIGINT NOT NULL,
        host_name         NVARCHAR(500),
        effective_from    DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
        effective_to      DATETIME2 NULL,
        is_current        BIT NOT NULL DEFAULT 1,
        CONSTRAINT uq_dim_host UNIQUE (host_id, effective_from)
    );
END;
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_dim_host_host_id' AND object_id = OBJECT_ID('dim_host'))
    CREATE INDEX idx_dim_host_host_id ON dim_host(host_id);
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_dim_host_current' AND object_id = OBJECT_ID('dim_host'))
    CREATE INDEX idx_dim_host_current ON dim_host(is_current) WHERE is_current = 1;

-- -----------------------------------------------------------------------------
-- DIMENSION: Neighbourhood (SCD Type 2)
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_neighbourhood')
BEGIN
    CREATE TABLE dim_neighbourhood (
        neighbourhood_sk   BIGINT IDENTITY(1,1) PRIMARY KEY,
        neighbourhood      NVARCHAR(500),
        neighbourhood_group NVARCHAR(500),
        effective_from     DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
        effective_to       DATETIME2 NULL,
        is_current         BIT NOT NULL DEFAULT 1,
        CONSTRAINT uq_dim_neighbourhood UNIQUE (neighbourhood, neighbourhood_group, effective_from)
    );
END;
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_dim_neighbourhood_current' AND object_id = OBJECT_ID('dim_neighbourhood'))
    CREATE INDEX idx_dim_neighbourhood_current ON dim_neighbourhood(is_current) WHERE is_current = 1;

-- -----------------------------------------------------------------------------
-- DIMENSION: Room type (static lookup — no history)
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_room_type')
    CREATE TABLE dim_room_type (
        room_type_sk  INT IDENTITY(1,1) PRIMARY KEY,
        room_type     NVARCHAR(100) NOT NULL UNIQUE
    );

-- -----------------------------------------------------------------------------
-- DIMENSION: Listing (SCD Type 2 — name, location, license can change)
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_listing')
BEGIN
    CREATE TABLE dim_listing (
        listing_sk     BIGINT IDENTITY(1,1) PRIMARY KEY,
        listing_id     BIGINT NOT NULL,
        name           NVARCHAR(MAX),
        latitude       NUMERIC(10, 7),
        longitude      NUMERIC(10, 7),
        license        NVARCHAR(200),
        effective_from DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
        effective_to   DATETIME2 NULL,
        is_current     BIT NOT NULL DEFAULT 1,
        CONSTRAINT uq_dim_listing UNIQUE (listing_id, effective_from)
    );
END;
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_dim_listing_listing_id' AND object_id = OBJECT_ID('dim_listing'))
    CREATE INDEX idx_dim_listing_listing_id ON dim_listing(listing_id);
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_dim_listing_current' AND object_id = OBJECT_ID('dim_listing'))
    CREATE INDEX idx_dim_listing_current ON dim_listing(is_current) WHERE is_current = 1;

-- -----------------------------------------------------------------------------
-- DIMENSION: Date (calendar — for review dates and last_review)
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_date')
    CREATE TABLE dim_date (
        date_sk     INT IDENTITY(1,1) PRIMARY KEY,
        full_date   DATE NOT NULL UNIQUE,
        year        SMALLINT NOT NULL,
        month       SMALLINT NOT NULL,
        day         SMALLINT NOT NULL,
        quarter     SMALLINT NOT NULL,
        day_of_week SMALLINT NOT NULL,
        is_weekend  BIT NOT NULL
    );
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_dim_date_full' AND object_id = OBJECT_ID('dim_date'))
    CREATE INDEX idx_dim_date_full ON dim_date(full_date);

-- -----------------------------------------------------------------------------
-- FACT: Listing snapshots
-- Grain: one row per listing per load_date (append-only each ETL run)
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_listing_snapshots')
BEGIN
    CREATE TABLE fact_listing_snapshots (
        snapshot_sk                    BIGINT IDENTITY(1,1) PRIMARY KEY,
        listing_sk                    BIGINT NOT NULL,
        host_sk                       BIGINT NOT NULL,
        neighbourhood_sk              BIGINT NOT NULL,
        room_type_sk                  INT NOT NULL,
        price                         NUMERIC(12, 2),
        minimum_nights                 INT,
        number_of_reviews             INT,
        last_review_date_sk           INT NULL,
        reviews_per_month             NUMERIC(6, 4),
        calculated_host_listings_count INT,
        availability_365              INT,
        number_of_reviews_ltm         INT,
        estimated_revenue_365         NUMERIC(14, 2),
        occupancy_rate                NUMERIC(5, 4),
        price_tier                    NVARCHAR(20),
        load_date                     DATE NOT NULL DEFAULT CAST(GETDATE() AS DATE),
        CONSTRAINT fk_fls_listing   FOREIGN KEY (listing_sk)   REFERENCES dim_listing(listing_sk),
        CONSTRAINT fk_fls_host      FOREIGN KEY (host_sk)     REFERENCES dim_host(host_sk),
        CONSTRAINT fk_fls_neighbourhood FOREIGN KEY (neighbourhood_sk) REFERENCES dim_neighbourhood(neighbourhood_sk),
        CONSTRAINT fk_fls_room_type FOREIGN KEY (room_type_sk) REFERENCES dim_room_type(room_type_sk),
        CONSTRAINT fk_fls_last_review FOREIGN KEY (last_review_date_sk) REFERENCES dim_date(date_sk),
        CONSTRAINT uq_fact_listing_snapshots UNIQUE (listing_sk, load_date)
    );
END;
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_fact_listing_snapshots_listing' AND object_id = OBJECT_ID('fact_listing_snapshots'))
    CREATE INDEX idx_fact_listing_snapshots_listing ON fact_listing_snapshots(listing_sk);
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_fact_listing_snapshots_host' AND object_id = OBJECT_ID('fact_listing_snapshots'))
    CREATE INDEX idx_fact_listing_snapshots_host ON fact_listing_snapshots(host_sk);
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_fact_listing_snapshots_neighbourhood' AND object_id = OBJECT_ID('fact_listing_snapshots'))
    CREATE INDEX idx_fact_listing_snapshots_neighbourhood ON fact_listing_snapshots(neighbourhood_sk);
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_fact_listing_snapshots_load' AND object_id = OBJECT_ID('fact_listing_snapshots'))
    CREATE INDEX idx_fact_listing_snapshots_load ON fact_listing_snapshots(load_date);

-- -----------------------------------------------------------------------------
-- FACT: Reviews
-- Grain: one row per review (listing + date)
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_reviews')
BEGIN
    CREATE TABLE fact_reviews (
        review_sk   BIGINT IDENTITY(1,1) PRIMARY KEY,
        listing_sk  BIGINT NOT NULL,
        date_sk     INT NOT NULL,
        load_date   DATE NOT NULL DEFAULT CAST(GETDATE() AS DATE),
        CONSTRAINT fk_fr_listing FOREIGN KEY (listing_sk) REFERENCES dim_listing(listing_sk),
        CONSTRAINT fk_fr_date   FOREIGN KEY (date_sk)    REFERENCES dim_date(date_sk)
    );
END;
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_fact_reviews_listing' AND object_id = OBJECT_ID('fact_reviews'))
    CREATE INDEX idx_fact_reviews_listing ON fact_reviews(listing_sk);
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_fact_reviews_date' AND object_id = OBJECT_ID('fact_reviews'))
    CREATE INDEX idx_fact_reviews_date ON fact_reviews(date_sk);
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_fact_reviews_load' AND object_id = OBJECT_ID('fact_reviews'))
    CREATE INDEX idx_fact_reviews_load ON fact_reviews(load_date);

-- -----------------------------------------------------------------------------
-- Staging (for ETL: raw CSV data before transform)
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'staging_listings')
    CREATE TABLE staging_listings (
        id                            BIGINT,
        name                          NVARCHAR(MAX),
        host_id                       BIGINT,
        host_name                     NVARCHAR(500),
        neighbourhood_group           NVARCHAR(500),
        neighbourhood                 NVARCHAR(500),
        latitude                      NUMERIC(10, 7),
        longitude                     NUMERIC(10, 7),
        room_type                     NVARCHAR(100),
        price                         NVARCHAR(50),
        minimum_nights                INT,
        number_of_reviews             INT,
        last_review                   NVARCHAR(20),
        reviews_per_month             NUMERIC(10, 6),
        calculated_host_listings_count INT,
        availability_365              INT,
        number_of_reviews_ltm         INT,
        license                       NVARCHAR(200),
        load_id                       NVARCHAR(100),
        row_num                       BIGINT
    );

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'staging_reviews')
    CREATE TABLE staging_reviews (
        listing_id  BIGINT,
        date        NVARCHAR(20),
        load_id     NVARCHAR(100),
        row_num     BIGINT
    );
