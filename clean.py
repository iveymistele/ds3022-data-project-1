import duckdb
import logging

# uses 2 functions, one to clean and one to call the cleaning on each table (necessary because of different column names for pickup and dropoff)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
    filename="clean.log"
)
logger = logging.getLogger(__name__)


def clean_table(con, table_name, pickup_col, dropoff_col):
    # print initial rows
    initial_rows = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
    logger.info(f"[{table_name}] Initial row count: {initial_rows}")

    # drop duplicates
    con.execute(f"""
        CREATE TABLE {table_name}_2 AS 
        SELECT DISTINCT * FROM {table_name};

        DROP TABLE {table_name};
        ALTER TABLE {table_name}_2 RENAME TO {table_name};
    """)
    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
    logger.info(f"[{table_name}] Row count after dropping duplicates: {row_count}")

    # add duration column
    con.execute(f"""
        ALTER TABLE {table_name} DROP COLUMN IF EXISTS trip_duration_s;
        ALTER TABLE {table_name} ADD COLUMN trip_duration_s BIGINT;

        UPDATE {table_name}
        SET trip_duration_s = EXTRACT(EPOCH FROM ({dropoff_col} - {pickup_col}));
    """)
    logger.info(f"[{table_name}] Added trip_duration_s")

    # clean invalid trips
    con.execute(f"""
        DELETE FROM {table_name}
        WHERE passenger_count = 0
           OR trip_distance = 0
           OR trip_distance > 100
           OR trip_duration_s > 86400;
    """)
    logger.info(f"[{table_name}] Removed invalid trips")

    # verify
    empty_trips = con.execute(f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count = 0;").fetchone()[0]
    zero_distance = con.execute(f"SELECT COUNT(*) FROM {table_name} WHERE trip_distance = 0;").fetchone()[0]
    long_trips = con.execute(f"SELECT COUNT(*) FROM {table_name} WHERE trip_duration_s > 86400;").fetchone()[0]
    far_trips = con.execute(f"SELECT COUNT(*) FROM {table_name} WHERE trip_distance > 100;").fetchone()[0]
    row_count_f = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]

    # print to log
    logger.info(f"[{table_name}] Trips with 0 passengers: {empty_trips}")
    logger.info(f"[{table_name}] Trips with 0 distance: {zero_distance}")
    logger.info(f"[{table_name}] Trips longer than 1 day: {long_trips}")
    logger.info(f"[{table_name}] Trips over 100 miles: {far_trips}")
    logger.info(f"[{table_name}] Final row count: {row_count_f}")

    # print to screen
    print(f"[{table_name}] Trips with 0 passengers: {empty_trips}")
    print(f"[{table_name}] Trips with 0 distance: {zero_distance}")
    print(f"[{table_name}] Trips longer than 1 day: {long_trips}")
    print(f"[{table_name}] Trips over 100 miles: {far_trips}")
    print(f"[{table_name}] Final row count: {row_count_f}")


def clean_db(): 
    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DB")

        # clean yellow
        clean_table(con, "yellow", "tpep_pickup_datetime", "tpep_dropoff_datetime")

        # clean green
        clean_table(con, "green", "lpep_pickup_datetime", "lpep_dropoff_datetime")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__": 
    clean_db()
