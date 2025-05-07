import os
import logging
import db_connector
import mysql.connector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def run_quality_checks_and_load(snapshot_id):
    """
    Executes the SQL commands in quality_checks.sql to perform data quality checks
    and load valid data into the weather_clean table.
    Tracks how many rows were added to data_quality_log and weather_clean.
    """
    logging.info(f"Starting transformation and load for snapshot_id: {snapshot_id}")
    try:
        # Connect to the database
        conn = db_connector.connect()
        if not conn:
            logging.error("Database connection failed.")
            return

        cursor = conn.cursor()

        # Get initial row counts
        cursor.execute("SELECT COUNT(*) FROM data_quality_log")
        initial_data_quality_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM weather_clean")
        initial_weather_clean_count = cursor.fetchone()[0]

        # Load the SQL script from quality_checks.sql
        sql_file_path = os.path.join(
            os.path.dirname(__file__), "..", "database", "quality_checks.sql"
        )
        with open(sql_file_path, "r", encoding="utf-8") as f:
            sql_script = f.read()

        # Replace the placeholder with the actual snapshot_id
        sql_script = sql_script.replace(":current_snapshot_id", str(snapshot_id))

        # Split the script into individual SQL commands
        sql_commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]

        logging.info(
            f"Executing {len(sql_commands)} SQL commands from quality_checks.sql..."
        )
        for i, command in enumerate(sql_commands, start=1):
            try:
                logging.info(f"Executing command {i}/{len(sql_commands)}")
                cursor.execute(command)
            except mysql.connector.Error as err:
                logging.error(f"Error executing command {i}: {err}")
                conn.rollback()
                break

        # Commit the transaction
        conn.commit()

        # Get final row counts
        cursor.execute("SELECT COUNT(*) FROM data_quality_log")
        final_data_quality_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM weather_clean")
        final_weather_clean_count = cursor.fetchone()[0]

        # Calculate growth
        data_quality_growth = final_data_quality_count - initial_data_quality_count
        weather_clean_growth = final_weather_clean_count - initial_weather_clean_count

        logging.info("Transformation and load completed successfully.")
        logging.info(f"Rows added to data_quality_log: {data_quality_growth}")
        logging.info(f"Rows added to weather_clean: {weather_clean_growth}")

    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        if conn:
            conn.rollback()
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            db_connector.close(conn)


# Example usage
if __name__ == "__main__":
    test_snapshot_id = 1746644257  # Replace with a valid snapshot_id
    run_quality_checks_and_load(test_snapshot_id)
