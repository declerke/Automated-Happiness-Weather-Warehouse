# run_etl.py
from etl_happiness import load_happiness_data
from etl_weather import fetch_and_load_weather

if __name__ == "__main__":
    print("Starting Automated Data Warehouse ETL...")
    load_happiness_data()
    fetch_and_load_weather()
    print("ETL Pipeline Completed Successfully!")