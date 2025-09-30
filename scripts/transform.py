import duckdb
import logging

# note: this was my OG transform script. switched to DBT 

'''
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
    filename="transform.log"
)
logger = logging.getLogger(__name__)

def transform_table(con, table_name, vehicle_type_col):
    try:
        logger.info(f"Starting transforms for {table_name}...")

        # Drop old columns if they exist (so reruns don’t fail)
        con.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS trip_co2_kgs;")
        con.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS avg_mph;")
        con.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS hour_of_day;")
        con.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS day_of_week;")
        con.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS week_of_year;")
        con.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS month_of_year;")

        # Add columns back
        con.execute(f"""
            ALTER TABLE {table_name} ADD COLUMN trip_co2_kgs DOUBLE;
            ALTER TABLE {table_name} ADD COLUMN avg_mph DOUBLE;
            ALTER TABLE {table_name} ADD COLUMN hour_of_day INTEGER;
            ALTER TABLE {table_name} ADD COLUMN day_of_week INTEGER;
            ALTER TABLE {table_name} ADD COLUMN week_of_year INTEGER;
            ALTER TABLE {table_name} ADD COLUMN month_of_year INTEGER;
        """)

        # Calculate trip_co2_kgs via join with emissions table
        con.execute(f"""
            UPDATE {table_name}
            SET trip_co2_kgs = (trip_distance * e.co2_grams_per_mile / 1000.0)
            FROM emissions e
            WHERE e.vehicle_type = '{vehicle_type_col}';
        """)

        # Average MPH = miles / (seconds / 3600)
        con.execute(f"""
            UPDATE {table_name}
            SET avg_mph = CASE
                WHEN trip_duration_s > 0 THEN trip_distance / (trip_duration_s / 3600.0)
                ELSE NULL
            END;
        """)

        # Extract hour, day, week, month
        if table_name == "yellow":
            pickup_col = "tpep_pickup_datetime"
        else:
            pickup_col = "lpep_pickup_datetime"

        con.execute(f"""
            UPDATE {table_name}
            SET hour_of_day = EXTRACT(HOUR FROM {pickup_col}),
                day_of_week = EXTRACT(DOW FROM {pickup_col}),
                week_of_year = EXTRACT(WEEK FROM {pickup_col}),
                month_of_year = EXTRACT(MONTH FROM {pickup_col});
        """)

        logger.info(f"Finished transforms for {table_name}")

    except Exception as e:
        logger.error(f"An error occurred while transforming {table_name}: {e}")
        print(f"An error occurred while transforming {table_name}: {e}")


def transform_db():
    con = duckdb.connect(database="emissions.duckdb", read_only=False)
    logger.info("Connected to DuckDB")

    transform_table(con, "yellow", "yellow_taxi")
    transform_table(con, "green", "green_taxi")


if __name__ == "__main__":
    transform_db()


'''