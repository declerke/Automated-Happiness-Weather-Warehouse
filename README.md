# 🌍 Automated Data Warehouse: Happiness & Weather Analysis

This project is an End-to-End Automated Data Warehouse designed to analyze the intersection between environmental conditions (Weather) and national well-being (World Happiness). It integrates static datasets with real-time API data to uncover correlations between climate and social metrics.



## 📂 Repository Structure

* **`etl_happiness.py`**: Handles the extraction and loading of the World Happiness Report data.
* **`etl_weather.py`**: Fetches real-time weather data for major global cities using geocoding and weather APIs.
* **`generate_happiness_report.py`**: A statistical engine that generates visualizations and correlation reports.
* **`run_etl.py`**: The orchestration script to trigger the full data ingestion pipeline.
* **`config.py`**: Centralized configuration for database credentials and target city tracking.
* **`world_happiness_2024.csv`**: Source data containing global happiness indices and economic factors.

## 🛠️ Technical Stack

* **Language**: Python 3.x
* **Data Ingestion**: Requests (Open-Meteo API, Nominatim API)
* **Data Processing**: Pandas, NumPy, SciPy
* **Database**: PostgreSQL
* **ORM/Driver**: Psycopg2, SQLAlchemy
* **Visualization**: Matplotlib, Seaborn

## 🚀 Getting Started

### 1. Database Setup
Ensure you have a PostgreSQL instance running. You can configure your connection details in `config.py`.

```python
PG_CONFIG = {
    "host": "localhost",
    "database": "data_warehouse",
    "user": "postgres",
    "password": "YOUR_PASSWORD",
    "port": 5432
}
```

### 2. Run the ETL Pipeline
To populate the data warehouse with the latest happiness scores and current weather snapshots, run:

```bash
python run_etl.py
```

### 3. Generate Analysis Reports
Once the data is loaded, generate the statistical reports and visualizations by running:

```bash
python generate_happiness_report.py
```

## 📊 Data Model & Logic

The project utilizes a star-schema inspired relational model to link environmental and social data.



1.  **Extraction**: 
    * Parses `world_happiness_2024.csv` for social indicators.
    * Uses OpenStreetMap geocoding to find coordinates for global cities.
    * Fetches current atmospheric data (temperature, humidity, weather codes) via Open-Meteo.
2.  **Transformation**:
    * Normalizes country and city names for consistent joins.
    * Handles API failures and missing data gracefully.
    * Calculates statistical significance (P-values) and Pearson correlations.
3.  **Loading**:
    * Implements `ON CONFLICT` logic in PostgreSQL to update existing records (Upsert).
    * Maintains snapshots of weather data to track conditions over time.

## 📈 Analysis Output
The `generate_happiness_report.py` script produces:
* **happiness_vs_temperature_global.png**: A scatter plot with a regression line.
* **happiness_distributions.png**: Statistical distribution of happiness scores across the dataset.
* **report_insights.txt**: A text-based summary of the correlation findings.
