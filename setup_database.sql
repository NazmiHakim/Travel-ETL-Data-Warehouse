CREATE TABLE Bookings (
    booking_id SERIAL PRIMARY KEY,
    booking_date TIMESTAMP,
    user_id INT,
    flight_carrier_code VARCHAR(10),
    flight_origin_id INT,
    flight_dest_id INT,
    passengers INT,
    revenue DECIMAL(10, 2)
);

-- ===================================
-- SCRIPT UNTUK DATABASE: db_dwh
-- ===================================

-- 1. Dimensi Bandara
CREATE TABLE Dim_Airport (
    airport_id_key SERIAL PRIMARY KEY,
    airport_id INT UNIQUE,
    city VARCHAR(100),
    state VARCHAR(50),
    name VARCHAR(255)
);

-- 2. Dimensi Maskapai
CREATE TABLE Dim_Airline (
    airline_key SERIAL PRIMARY KEY,
    carrier_code VARCHAR(10) UNIQUE,
    airline_name VARCHAR(100)
);

-- 3. Dimensi Waktu
CREATE TABLE Dim_Date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE UNIQUE,
    day_of_week INT,
    day_of_month INT,
    month INT,
    quarter INT,
    year INT
);

-- 4. Tabel Fakta Penerbangan
CREATE TABLE Fact_Flights (
    flight_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES Dim_Date(date_key),
    airline_key INT REFERENCES Dim_Airline(airline_key),
    origin_airport_key INT REFERENCES Dim_Airport(airport_id_key),
    dest_airport_key INT REFERENCES Dim_Airport(airport_id_key),
    departure_delay INT,
    arrival_delay INT,
    total_passengers INT,
    total_revenue DECIMAL(10, 2)
);

-- 5. (PENTING) Isi Dim_Date dengan data
INSERT INTO Dim_Date (full_date, day_of_week, day_of_month, month, quarter, year)
SELECT
    CAST(t.tanggal AS DATE) AS full_date,
    EXTRACT(ISODOW FROM t.tanggal) AS day_of_week,
    EXTRACT(DAY FROM t.tanggal) AS day_of_month,
    EXTRACT(MONTH FROM t.tanggal) AS month,
    EXTRACT(QUARTER FROM t.tanggal) AS quarter,
    EXTRACT(YEAR FROM t.tanggal) AS year
FROM
    generate_series('2020-01-01'::timestamp, '2025-12-31'::timestamp, '1 day'::interval) AS t(tanggal)
ON CONFLICT (full_date) DO NOTHING;
