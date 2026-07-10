CREATE TABLE IF NOT EXISTS daily_temperatures (
    id SERIAL PRIMARY KEY,
    location_name VARCHAR(50),
    record_date DATE,
    max_temp_celsius NUMERIC(5, 2),
    min_temp_celsius NUMERIC(5, 2),
    extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);