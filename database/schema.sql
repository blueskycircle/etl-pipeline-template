-- Create the Staging Table
-- This table holds the raw data exactly as extracted from the OpenWeatherMap API response.
-- Added snapshot_id to version data from each pipeline run.
CREATE TABLE weather_staging (
    staging_id BIGINT AUTO_INCREMENT PRIMARY KEY, -- Use BIGINT AUTO_INCREMENT for MySQL primary key
    snapshot_id BIGINT NOT NULL,                -- Identifier for the specific ETL run/batch/snapshot
    city_name VARCHAR(100),                     -- Name of the city for the weather observation
    country_code VARCHAR(10),                   -- Country code (e.g., 'GB', 'US')
    observation_timestamp TIMESTAMP NULL,       -- Use TIMESTAMP for MySQL (handles UTC conversion)
    temperature_celsius REAL,                   -- Temperature in Celsius
    feels_like_celsius REAL,                    -- "Feels like" temperature in Celsius
    humidity_percent INTEGER,                   -- Humidity percentage (0-100)
    pressure_hpa INTEGER,                       -- Atmospheric pressure in hectopascals
    wind_speed_mps REAL,                        -- Wind speed in meters per second
    weather_main VARCHAR(50),                   -- Short description (e.g., 'Clouds', 'Rain')
    weather_description VARCHAR(100),           -- More detailed description (e.g., 'broken clouds')
    api_response_json JSON,                     -- Use JSON type for MySQL (requires MySQL 5.7.8+)
    fetched_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp when the record was fetched
);

-- Create the Clean Data Table
-- This table holds the final, validated, and cleansed weather data ready for reporting or analysis.
CREATE TABLE weather_clean (
    clean_id BIGINT AUTO_INCREMENT PRIMARY KEY, -- Use BIGINT AUTO_INCREMENT
    city_name VARCHAR(100) NOT NULL,            -- City name (ensured not null)
    country_code VARCHAR(10),                   -- Country code
    observation_timestamp TIMESTAMP NOT NULL,   -- Use TIMESTAMP
    temperature_celsius REAL,                   -- Temperature
    feels_like_celsius REAL,                    -- Feels like temperature
    humidity_percent INTEGER,                   -- Humidity
    pressure_hpa INTEGER,                       -- Pressure
    wind_speed_mps REAL,                        -- Wind speed
    weather_main VARCHAR(50),                   -- Main weather condition
    weather_description VARCHAR(100),           -- Detailed weather condition
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when the record was processed
    -- Add a unique constraint to prevent duplicates in the final table based on city and observation time
    UNIQUE (city_name, observation_timestamp)
);

-- Create the Data Quality Log Table
-- This table records details about any data quality issues found during the checks.
CREATE TABLE data_quality_log (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,   -- Use BIGINT AUTO_INCREMENT
    snapshot_id BIGINT NOT NULL,                -- Identifier for the ETL run where the issue was found
    check_name VARCHAR(100) NOT NULL,           -- Name of the quality check that failed
    staging_table_name VARCHAR(100) DEFAULT 'weather_staging', -- Table where the issue was found
    staging_id BIGINT,                          -- Match staging_id type (BIGINT)
    problematic_column VARCHAR(100),            -- Name of the column with the issue (if applicable)
    problematic_value TEXT,                     -- The actual value that caused the issue (if applicable)
    issue_description TEXT,                     -- A brief description of the quality issue
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp when the quality issue was logged
);

-- Optional: Add indexes to staging table columns frequently used in WHERE clauses or JOINs
CREATE INDEX idx_staging_snapshot_id ON weather_staging (snapshot_id);
CREATE INDEX idx_staging_city_obs_time ON weather_staging (city_name, observation_timestamp);
CREATE INDEX idx_staging_fetched_time ON weather_staging (fetched_timestamp);
CREATE INDEX idx_log_snapshot_id ON data_quality_log (snapshot_id);