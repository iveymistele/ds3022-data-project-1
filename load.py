import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

urls = []
yellow_urls = [f"{base_url}/yellow_tripdata_2024-{m:02d}.parquet" for m in range(1, 13)]

def load_parquet_files():
    con = None
    try:
        # Connect to DuckDB
        con = duckdb.connect(database="emissions.duckdb", read_only=False)
        logger.info("Connected to DuckDB instance")

        # Drop if exists to avoid conflicts
        con.execute("DROP TABLE IF EXISTS yellow")

        # Create from first file
        con.execute("""
            CREATE TABLE yellow AS
            SELECT passenger_count, trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime
            FROM read_parquet($urls)
        """, {"urls": yellow_urls})
        logger.info("Finished loading all parquet files")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()