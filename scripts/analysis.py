import duckdb
import logging
import pandas as pd
import matplotlib.pyplot as plt

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
    filename="analysis.log"
)
logger = logging.getLogger(__name__)

'''
1. What was the single largest carbon producing trip of the year for YELLOW and GREEN trips? (One result for each type)
2. Across the entire year, what on average are the most carbon heavy and carbon light hours of the day for YELLOW and for GREEN trips? (1-24)
3. Across the entire year, what on average are the most carbon heavy and carbon light days of the week for YELLOW and for GREEN trips? (Sun-Sat)
4. Across the entire year, what on average are the most carbon heavy and carbon light weeks of the year for YELLOW and for GREEN trips? (1-52)
5. Across the entire year, what on average are the most carbon heavy and carbon light months of the year for YELLOW and for GREEN trips? (Jan-Dec)
6. Use a plotting library of your choice (`matplotlib`, `seaborn`, etc.) to generate a time-series plot or histogram with MONTH
along the X-axis and CO2 totals along the Y-axis. Render two lines/bars/plots of data, one each for YELLOW and GREEN taxi trip CO2 totals.
'''

# Q1 Function - takes in both yellow and green tables as input and selects largest carbon producing trip from each
# Value is converted to a data frame for the table with one element
def largest_trip(con, table):
    try:
        result = con.execute(f"""
            SELECT trip_distance, trip_co2_kgs
            FROM {table}
            ORDER BY trip_co2_kgs DESC
            LIMIT 1
            ;
        """).fetchdf()
        logger.info(f"[{table}] Largest trip query ran successfully")
        return result
    except Exception as e:
        logger.error(f"[{table}] Largest trip query failed: {e}")
        return pd.DataFrame()

# Q2 Function - takes in both yellow and green tables as input and finds average most heavy and light carbon usage hours of the day
# Averages are converted to a data frame which contains all averages in descending order to be selected from when function is implemented
def heavy_light_hours(con, table):
    try:
        result = con.execute(f"""
            SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_co2
            FROM {table}
            GROUP BY hour_of_day
            ORDER BY avg_co2 DESC
            ;
            """).fetchdf()
        logger.info(f"[{table}] Heavy/Light hours query ran")
        return result
    except Exception as e: 
            logger.error(f"[{table}] Heavy/light hours query failed: {e}")
            return pd.DataFrame()

# Q3 Function - takes in both yellow and green tables as input and finds average most heavy and light carbon usage days of the week
# Averages are converted to a data frame which contains all averages in descending order to be selected from when function is implemented
def heavy_light_days(con, table):
    try:
        result = con.execute(f"""
            SELECT day_of_week, AVG(trip_co2_kgs) AS avg_co2
            FROM {table}
            GROUP BY day_of_week
            ORDER BY avg_co2 DESC
            ;
            """).fetchdf()
        logger.info(f"[{table}] Heavy/Light days query ran")
        return result
    except Exception as e: 
            logger.error(f"[{table}] Heavy/light days query failed: {e}")
            return pd.DataFrame()
    
# Q4 Function - takes in both yellow and green tables as input and finds average most heavy and light carbon usage days of the week
# Averages are converted to a data frame which contains all averages in descending order to be selected from when function is implemented
def heavy_light_weeks(con, table):
    try:
        result = con.execute(f"""
            SELECT week_of_year, AVG(trip_co2_kgs) AS avg_co2
            FROM {table}
            GROUP BY week_of_year
            ORDER BY avg_co2 DESC
            ;
            """).fetchdf()
        logger.info(f"[{table}] Heavy/Light weeks query ran")
        return result
    except Exception as e: 
            logger.error(f"[{table}] Heavy/light weeks query failed: {e}")
            return pd.DataFrame()

# Q5 Function - takes in both yellow and green tables as input and finds average most heavy and light carbon usage months of the year
# Averages are converted to a data frame which contains all averages in descending order to be selected from when function is implemented
def heavy_light_months(con, table):
    try:
        result = con.execute(f"""
            SELECT month_of_year, AVG(trip_co2_kgs) AS avg_co2
            FROM {table}
            GROUP BY month_of_year
            ORDER BY avg_co2 DESC
            ;
            """).fetchdf()
        logger.info(f"[{table}] Heavy/Light months query ran")
        return result
    except Exception as e: 
            logger.error(f"[{table}] Heavy/light months query failed: {e}")
            return pd.DataFrame()
    
# Q6 Function - plot 
# Generates and saves as png two line plots of carbon usage by month for each table (yellow and green)
# Commented out is the version I used to generate the plots for 2024 only
def generate_plots(con):
    try:
       
        # Yellow Plot
        yellow_df = con.execute("""
            SELECT 
            EXTRACT(YEAR FROM tpep_pickup_datetime) AS year,
            month_of_year, 
            SUM(trip_co2_kgs) AS total_co2
            FROM transformed_yellow
            GROUP BY year, month_of_year
            ORDER BY year, month_of_year;
        """).fetchdf()
    
        # Adjust x axis to reflect month AND year 
        yellow_df["year_month"] = yellow_df["year"].astype(int).astype(str) + "-" + yellow_df["month_of_year"].astype(int).astype(str)
        yellow_df.plot(kind="line", x="year_month", y="total_co2", marker="o", figsize=(12, 5))
        plt.title("Total CO2 Emissions by Month (2015-2024) - Yellow Cabs")
        plt.xlabel("Year-Month")
        plt.ylabel("Total CO2 (kg)")
        plt.grid(True)
        plt.savefig("co2_by_month_yellow.png")
        plt.close()
        print("Saved plot as co2_by_month_yellow.png")
        logger.info("Saved plot co2_by_month_yellow.png")

        # Green plot 
        green_df = con.execute("""
            SELECT 
            EXTRACT(YEAR FROM lpep_pickup_datetime) AS year,
            month_of_year, 
            SUM(trip_co2_kgs) AS total_co2
            FROM transformed_green
            GROUP BY year, month_of_year
            ORDER BY year, month_of_year;
        """).fetchdf()

        # Again adjust x axis to reflect month AND year 
        green_df["year_month"] = green_df["year"].astype(int).astype(str) + "-" + green_df["month_of_year"].astype(int).astype(str)
        green_df.plot(kind="line", x="year_month", y="total_co2", marker="o", figsize=(12, 5))
        plt.title("Total CO2 Emissions by Month (2015-2024) - Green Cabs")
        plt.xlabel("Year-Month")
        plt.ylabel("Total CO2 (kg)")
        plt.grid(True)
        plt.savefig("co2_by_month_green.png")
        plt.close()
        print("Saved plot as co2_by_month_green.png")
        logger.info("Saved plot co2_by_month_green.png")

        # Below is my code for just 2024 plots 
        '''
                # Yellow Plot
        yellow_df = con.execute("""
            SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2
            FROM transformed_yellow
            GROUP BY month_of_year
            ORDER BY month_of_year
            ;
        """).fetchdf()
    
        yellow_df.plot(kind="line", x="month_of_year", y="total_co2", marker="o", figsize=(8, 5))
        plt.title("Total CO2 Emissions by Month - Yellow Cabs")
        plt.xlabel("Month")
        plt.ylabel("Total CO2 (kg)")
        plt.grid(True)
        plt.savefig("co2_by_month_yellow.png")
        plt.close()
        print("Saved plot as co2_by_month_yellow.png")
        logger.info("Saved plot co2_by_month_yellow.png")

        # Green plot 
        green_df = con.execute("""
            SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2
            FROM transformed_green
            GROUP BY month_of_year
            ORDER BY month_of_year
            ;
        """).fetchdf()
    
       
        green_df.plot(kind="line", x="month_of_year", y="total_co2", marker="o", figsize=(8, 5))
        plt.title("Total CO2 Emissions by Month - Green Cabs")
        plt.xlabel("Month (1-12 = January-December)")
        plt.ylabel("Total CO2 (kg)")
        plt.grid(True)
        plt.savefig("co2_by_month_green.png")
        plt.close()
        print("Saved plot as co2_by_month_green.png")
        logger.info("Saved plot co2_by_month_green.png")
        
        '''
    except Exception as e: 
        logger.error("Plots failed. :( ")

# Main analysis function - implements all of above 
# Returns printed results for table (transformed_green and transformed_yellow), renders plots and saves as png
def run_analysis(): 
    con = duckdb.connect(database="emissions.duckdb", read_only=True)
    logger.info("Connected to DuckDB")

    tables = ["transformed_green", "transformed_yellow"]

    for table in tables:
        
        print(f"\nAnswers for Table {table}: ")

        largest = largest_trip(con, table)
        print(f"(Q1) Largest CO₂ trip:\n{largest.to_string(index=False)}")

        hours = heavy_light_hours(con, table)
        print(f"(Q2) Heavy hour: {hours.iloc[0]['hour_of_day']} ({hours.iloc[0]['avg_co2']:.2f} kg)")
        print(f"(Q2) Light hour: {hours.iloc[-1]['hour_of_day']} ({hours.iloc[-1]['avg_co2']:.2f} kg)")

        days = heavy_light_days(con, table)
        print(f"(Q3) Heavy day: {days.iloc[0]['day_of_week']} ({days.iloc[0]['avg_co2']:.2f} kg)")
        print(f"(Q3) Light day: {days.iloc[-1]['day_of_week']} ({days.iloc[-1]['avg_co2']:.2f} kg)")

        weeks = heavy_light_weeks(con, table)
        print(f"(Q4) Heavy week: {weeks.iloc[0]['week_of_year']} ({weeks.iloc[0]['avg_co2']:.2f} kg)")
        print(f"(Q4) Light week: {weeks.iloc[-1]['week_of_year']} ({weeks.iloc[-1]['avg_co2']:.2f} kg)")

        months = heavy_light_months(con, table)
        print(f"(Q5) Heavy month: {months.iloc[0]['month_of_year']} ({months.iloc[0]['avg_co2']:.2f} kg)")
        print(f"(Q5) Light month: {months.iloc[-1]['month_of_year']} ({months.iloc[-1]['avg_co2']:.2f} kg)")

    # Run and save plot outputs (Q6) 
    generate_plots(con)


if __name__ == "__main__":
    run_analysis()