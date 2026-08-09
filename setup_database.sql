-- ============================================================
-- TravelNusantara Data Warehouse — DDL Script
-- Applies to: db_oltp AND db_dwh
-- ============================================================

-- ============================================================
-- DATABASE: db_oltp
-- Operational Transactional Database
-- ============================================================

CREATE TABLE IF NOT EXISTS Bookings (
    booking_id          SERIAL PRIMARY KEY,
    booking_date        TIMESTAMP,
    user_id             INT,
    flight_carrier_code VARCHAR(10),
    flight_origin_id    INT,
    flight_dest_id      INT,
    passengers          INT,
    revenue             DECIMAL(10, 2)
);

-- ============================================================
-- DATABASE: db_dwh
-- Star Schema Data Warehouse (Kimball Dimensional Model)
-- ============================================================

-- 1. Airport Dimension
CREATE TABLE IF NOT EXISTS Dim_Airport (
    airport_id_key  SERIAL PRIMARY KEY,
    airport_id      INT UNIQUE,
    city            VARCHAR(100),
    state           VARCHAR(50),
    name            VARCHAR(255)
);

-- 2. Airline Dimension
CREATE TABLE IF NOT EXISTS Dim_Airline (
    airline_key     SERIAL PRIMARY KEY,
    carrier_code    VARCHAR(10) UNIQUE,
    airline_name    VARCHAR(100)
);

-- 3. Date Dimension
CREATE TABLE IF NOT EXISTS Dim_Date (
    date_key        SERIAL PRIMARY KEY,
    full_date       DATE UNIQUE,
    day_of_week     INT,   -- 1 (Monday) to 7 (Sunday)
    day_of_month    INT,
    month           INT,   -- 1 to 12
    quarter         INT,   -- 1 to 4
    year            INT
);

-- 4. Flight Fact Table (Operational Delays + Revenue Aggregations)
CREATE TABLE IF NOT EXISTS Fact_Flights (
    flight_key          SERIAL PRIMARY KEY,
    date_key            INT REFERENCES Dim_Date(date_key),
    airline_key         INT REFERENCES Dim_Airline(airline_key),
    origin_airport_key  INT REFERENCES Dim_Airport(airport_id_key),
    dest_airport_key    INT REFERENCES Dim_Airport(airport_id_key),
    departure_delay     INT,    -- minutes (positive = delayed, negative = early)
    arrival_delay       INT,    -- minutes (positive = delayed, negative = early)
    total_passengers    INT,
    total_revenue       DECIMAL(10, 2)
);

-- 5. Customer Feedback Fact Table (AI-Enriched Sentiment & Complaint Categories)
CREATE TABLE IF NOT EXISTS Fact_Customer_Feedback (
    feedback_key        SERIAL PRIMARY KEY,
    date_key            INT REFERENCES Dim_Date(date_key),
    airline_key         INT REFERENCES Dim_Airline(airline_key),
    sentiment           VARCHAR(20),   -- 'Positive' | 'Neutral' | 'Negative'
    complaint_category  VARCHAR(50),   -- 'Delay' | 'Baggage' | 'Service' | 'Pricing' | 'None'
    satisfaction_score  INT,           -- 1 (worst) to 5 (best)
    review_text         TEXT
);

-- ============================================================
-- 6. Populate Dim_Date with a daily series (2020-01-01 to 2026-12-31)
-- Uses PostgreSQL generate_series() for automatic date spine generation.
-- ON CONFLICT DO NOTHING ensures safe re-runs (idempotent).
-- ============================================================
INSERT INTO Dim_Date (full_date, day_of_week, day_of_month, month, quarter, year)
SELECT
    CAST(t.d AS DATE)              AS full_date,
    EXTRACT(ISODOW  FROM t.d)      AS day_of_week,
    EXTRACT(DAY     FROM t.d)      AS day_of_month,
    EXTRACT(MONTH   FROM t.d)      AS month,
    EXTRACT(QUARTER FROM t.d)      AS quarter,
    EXTRACT(YEAR    FROM t.d)      AS year
FROM generate_series(
    '2020-01-01'::timestamp,
    '2026-12-31'::timestamp,
    '1 day'::interval
) AS t(d)
ON CONFLICT (full_date) DO NOTHING;

-- ============================================================
-- 7. Composite Airline Performance View
--
-- PURPOSE: Provides a single, queryable performance ranking for
-- all airlines that combines three independent data sources:
--   (a) On-Time Performance (OTP) — from Fact_Flights
--   (b) Revenue Efficiency       — from Fact_Flights
--   (c) Customer Satisfaction    — from Fact_Customer_Feedback
--
-- COMPOSITE SCORE WEIGHTS:
--   40% — OTP Score       (1 - avg_departure_delay_norm)
--   30% — Revenue Score   (normalized SUM revenue)
--   30% — Satisfaction    (avg_satisfaction_score / 5.0)
--
-- The final score is in [0.0, 1.0] where 1.0 = perfect performance.
-- Use this view when users ask about overall/composite airline performance,
-- ranking, or comparisons across multiple dimensions.
-- ============================================================
CREATE OR REPLACE VIEW v_airline_performance AS
WITH flight_stats AS (
    SELECT
        da.airline_key,
        da.airline_name,
        da.carrier_code,
        ROUND(AVG(ff.departure_delay)::numeric, 2)          AS avg_departure_delay,
        ROUND(AVG(ff.arrival_delay)::numeric, 2)            AS avg_arrival_delay,
        ROUND(AVG(CASE WHEN ff.departure_delay <= 15 THEN 1.0 ELSE 0.0 END)::numeric, 4) AS otp_rate,
        SUM(ff.total_revenue)                               AS total_revenue,
        SUM(ff.total_passengers)                            AS total_passengers,
        COUNT(ff.flight_key)                                AS total_flights
    FROM fact_flights ff
    JOIN dim_airline da ON ff.airline_key = da.airline_key
    GROUP BY da.airline_key, da.airline_name, da.carrier_code
),
feedback_stats AS (
    SELECT
        fcf.airline_key,
        ROUND(AVG(fcf.satisfaction_score)::numeric, 3)      AS avg_satisfaction,
        COUNT(fcf.feedback_key)                              AS total_reviews,
        COUNT(CASE WHEN fcf.sentiment = 'Positive' THEN 1 END) AS positive_reviews,
        COUNT(CASE WHEN fcf.sentiment = 'Negative' THEN 1 END) AS negative_reviews
    FROM fact_customer_feedback fcf
    GROUP BY fcf.airline_key
),
normalized AS (
    SELECT
        fs.airline_key,
        fs.airline_name,
        fs.carrier_code,
        fs.avg_departure_delay,
        fs.avg_arrival_delay,
        fs.otp_rate,
        fs.total_revenue,
        fs.total_passengers,
        fs.total_flights,
        COALESCE(fb.avg_satisfaction, 3.0)          AS avg_satisfaction,
        COALESCE(fb.total_reviews, 0)               AS total_reviews,
        COALESCE(fb.positive_reviews, 0)            AS positive_reviews,
        COALESCE(fb.negative_reviews, 0)            AS negative_reviews,

        -- Normalize OTP: 0.0 to 1.0 (higher = more on-time)
        fs.otp_rate                                 AS otp_score,

        -- Normalize revenue: each airline vs. max in the dataset
        ROUND(
            (fs.total_revenue / NULLIF(MAX(fs.total_revenue) OVER (), 0))::numeric, 4
        )                                           AS revenue_score,

        -- Normalize satisfaction: 1-5 scale → 0.0-1.0
        ROUND(
            (COALESCE(fb.avg_satisfaction, 3.0) / 5.0)::numeric, 4
        )                                           AS satisfaction_score
    FROM flight_stats fs
    LEFT JOIN feedback_stats fb ON fs.airline_key = fb.airline_key
)
SELECT
    airline_name,
    carrier_code,
    avg_departure_delay,
    avg_arrival_delay,
    ROUND((otp_rate * 100)::numeric, 1)             AS otp_percentage,
    total_revenue,
    total_passengers,
    total_flights,
    avg_satisfaction,
    total_reviews,
    positive_reviews,
    negative_reviews,
    -- Composite Performance Score: 40% OTP + 30% Revenue + 30% Satisfaction
    ROUND(
        (0.40 * otp_score + 0.30 * revenue_score + 0.30 * satisfaction_score)::numeric,
        4
    )                                               AS performance_score,
    -- Human-readable rank (1 = best)
    RANK() OVER (
        ORDER BY (0.40 * otp_score + 0.30 * revenue_score + 0.30 * satisfaction_score) DESC
    )                                               AS performance_rank
FROM normalized
ORDER BY performance_rank;
