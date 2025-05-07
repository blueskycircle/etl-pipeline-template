import logging
from dotenv import load_dotenv
import extract
import transform_load

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def run_pipeline():
    """
    Runs the full ETL pipeline: Extract -> Transform & Load.
    """
    logging.info("Starting ETL Pipeline Run")

    # --- Step 1: Extract Data ---
    logging.info("Running Extraction Step")
    snapshot_id = extract.fetch_weather_data()

    if snapshot_id is None:
        logging.error("Extraction step failed. Aborting pipeline run.")
        logging.info("Pipeline Run Finished (with errors)")
        return

    logging.info(f"Extraction successful. Snapshot ID: {snapshot_id}")

    # --- Step 2: Transform and Load Data ---
    logging.info("Running Transformation & Load Step")
    transform_load.run_quality_checks_and_load(snapshot_id)

    logging.info("Pipeline Run Finished")


if __name__ == "__main__":
    load_dotenv(dotenv_path="../.env")

    run_pipeline()
