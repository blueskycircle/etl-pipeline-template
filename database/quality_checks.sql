INSERT INTO data_quality_log (
    snapshot_id,
    check_name,
    staging_id,
    problematic_column,
    problematic_value,
    issue_description
)
SELECT
    snapshot_id,
    'Temperature Range Check',
    staging_id,
    'temperature_celsius',
    CAST(temperature_celsius AS CHAR),
    'Temperature is outside the plausible range (-70°C to +60°C)'
FROM
    weather_staging
WHERE
    snapshot_id = :current_snapshot_id
    AND (temperature_celsius < -70 OR temperature_celsius > 60);

INSERT INTO weather_clean (
    city_name,
    country_code,
    observation_timestamp,
    temperature_celsius,
    feels_like_celsius,
    humidity_percent,
    pressure_hpa,
    wind_speed_mps,
    weather_main,
    weather_description
)
SELECT
    city_name,
    country_code,
    observation_timestamp,
    temperature_celsius,
    feels_like_celsius,
    humidity_percent,
    pressure_hpa,
    wind_speed_mps,
    weather_main,
    weather_description
FROM
    weather_staging
WHERE
    snapshot_id = :current_snapshot_id
    AND temperature_celsius BETWEEN -70 AND 60
    AND city_name IS NOT NULL
    AND TRIM(city_name) != ''
ON DUPLICATE KEY UPDATE
    country_code = VALUES(country_code),
    temperature_celsius = VALUES(temperature_celsius),
    feels_like_celsius = VALUES(feels_like_celsius),
    humidity_percent = VALUES(humidity_percent),
    pressure_hpa = VALUES(pressure_hpa),
    wind_speed_mps = VALUES(wind_speed_mps),
    weather_main = VALUES(weather_main),
    weather_description = VALUES(weather_description),
    processed_timestamp = CURRENT_TIMESTAMP;