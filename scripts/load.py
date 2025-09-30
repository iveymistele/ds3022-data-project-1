import duckdb
import logging
import time # sleep intervals to avoid blocks from gov server


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
    filename="load.log"
)
logger = logging.getLogger(__name__)

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"


# Loop through all years, months and append urls to lists for green and yellow data 
yellow_urls = [
    f"{base_url}/yellow_tripdata_{year}-{month:02d}.parquet"
    for year in range(2015, 2025)  # 2015–2024
    for month in range(1, 13)
]

green_urls = [
    f"{base_url}/green_tripdata_{year}-{month:02d}.parquet"
    for year in range(2015, 2025)
    for month in range(1, 13)
]

time.sleep(120)

# Main function, 
# Drops old tables if exist and extracts 4 columns from remote server
# Places into two separate tables (yellow and green) and writes a third table from emissions csv
def load_parquet_files():
    con = duckdb.connect(database="emissions.duckdb", read_only=False)
    logger.info("Connected to DuckDB instance")

    # Drop old tables
    con.execute("DROP TABLE IF EXISTS yellow")
    con.execute("DROP TABLE IF EXISTS green")
    con.execute("DROP TABLE IF EXISTS emissions")

    # Create yellow table schema from first file
    con.execute(f"""
        CREATE TABLE yellow AS
        SELECT passenger_count, trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime
        FROM read_parquet('{yellow_urls[0]}')
    """)
    logger.info(f"Created yellow table from {yellow_urls[0]}")

    time.sleep(20)
    # Insert rest
    for url in yellow_urls[1:]:
        try:
            con.execute(f"""
                INSERT INTO yellow
                SELECT passenger_count, trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime
                FROM read_parquet('{url}')
            """)
            # Row count after each insert
            count = con.execute("SELECT COUNT(*) FROM yellow").fetchone()[0]
            print(f"Yellow row count after {url}: {count}")
            time.sleep(40)
        except Exception as e:
            logger.warning(f"Skipping {url} due to error: {e}")

    # Emissions table
    con.execute("""
        CREATE TABLE emissions AS
        SELECT * FROM read_csv('data/vehicle_emissions.csv')
    """)
    print("Emissions row count:", con.execute("SELECT COUNT(*) FROM emissions").fetchone()[0])


    time.sleep(180)
    # Same pattern for green
    con.execute(f"""
        CREATE TABLE green AS
        SELECT passenger_count, trip_distance, lpep_pickup_datetime, lpep_dropoff_datetime
        FROM read_parquet('{green_urls[0]}')
    """)
    logger.info(f"Created green table from {green_urls[0]}")
    time.sleep(20)
    for url in green_urls[1:]:
        try:
            con.execute(f"""
                INSERT INTO green
                SELECT passenger_count, trip_distance, lpep_pickup_datetime, lpep_dropoff_datetime
                FROM read_parquet('{url}')
            """)
            count = con.execute("SELECT COUNT(*) FROM green").fetchone()[0]
            print(f"Green row count after {url}: {count}")
            time.sleep(40)
        except Exception as e:
            logger.warning(f"Skipping {url} due to error: {e}")



if __name__ == "__main__":
    load_parquet_files()
