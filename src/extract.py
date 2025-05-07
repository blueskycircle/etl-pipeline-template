import requests
import os
import time
import json
import logging
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
import db_connector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Load environment variables (including API key)
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

# List of cities to fetch weather data for
TARGET_CITIES = ["London,UK", "Paris,FR", "New York,US", "Tokyo,JP", "Sydney,AU"]


def fetch_weather_data():
    """
    Fetches weather data for target cities from OpenWeatherMap API,
    parses the response, and loads it into the weather_staging table.

    Returns:
        int: The snapshot_id for this extraction run, or None if fetching fails.
    """
    if not API_KEY:
        logging.error("OpenWeatherMap API key not found in environment variables.")
        return None

    # Generate a unique snapshot ID for this run (using Unix timestamp + 4 random digits)
    base_snapshot_id = int(time.time())
    random_digits = random.randint(1000, 9999)  # Generate 4 random digits
    snapshot_id = int(f"{base_snapshot_id}{random_digits}")
    logging.info(f"Starting extraction for snapshot_id: {snapshot_id}")

    conn = None
    extracted_data_count = 0
    try:
        conn = db_connector.connect()
        if not conn:
            logging.error("Database connection failed.")
            return None

        cursor = conn.cursor()

        # SQL statement to insert data into the staging table
        insert_sql = """
            INSERT INTO weather_staging (
                snapshot_id, city_name, country_code, observation_timestamp,
                temperature_celsius, feels_like_celsius, humidity_percent,
                pressure_hpa, wind_speed_mps, weather_main, weather_description,
                api_response_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for city in TARGET_CITIES:
            params = {"q": city, "appid": API_KEY, "units": "metric"}
            try:
                response = requests.get(BASE_URL, params=params, timeout=10)
                response.raise_for_status()

                data = response.json()
                raw_json = json.dumps(data)

                # Parse and transform data
                city_name = data.get("name")
                country = data.get("sys", {}).get("country")
                obs_timestamp_unix = data.get("dt")
                obs_timestamp = (
                    datetime.fromtimestamp(obs_timestamp_unix, timezone.utc)
                    if obs_timestamp_unix
                    else None
                )

                main_data = data.get("main", {})
                temp_c = main_data.get("temp")
                feels_like_c = main_data.get("feels_like")
                humidity = main_data.get("humidity")
                pressure = main_data.get("pressure")

                wind_data = data.get("wind", {})
                wind_speed = wind_data.get("speed")

                weather_info = data.get("weather", [{}])[0]
                weather_main = weather_info.get("main")
                weather_desc = weather_info.get("description")

                # Prepare data tuple
                data_tuple = (
                    snapshot_id,
                    city_name,
                    country,
                    obs_timestamp,
                    temp_c,
                    feels_like_c,
                    humidity,
                    pressure,
                    wind_speed,
                    weather_main,
                    weather_desc,
                    raw_json,
                )

                # Execute the insert statement
                cursor.execute(insert_sql, data_tuple)
                extracted_data_count += 1
                logging.info(f"Successfully fetched and staged data for {city_name}")

            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching data for {city}: {e}")
            except json.JSONDecodeError:
                logging.error(f"Error decoding JSON response for {city}")
            except KeyError as e:
                logging.error(f"Missing key in API response for {city}: {e}")
            except Exception as e:
                logging.error(f"An unexpected error occurred for {city}: {e}")

            time.sleep(1)  # Add a small delay to avoid hitting API rate limits

        # Commit all inserts for this snapshot
        conn.commit()
        logging.info(
            f"Extraction complete for snapshot_id: {snapshot_id}. Staged {extracted_data_count} records."
        )
        return snapshot_id

    except mysql.connector.Error as err:
        logging.error(f"Database error during extraction: {err}")
        if conn:
            conn.rollback()
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during extraction: {e}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            cursor.close()
            db_connector.close(conn)
