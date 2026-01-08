# etl_happiness.py
import pandas as pd
import psycopg2
from config import PG_CONFIG

def load_happiness_data():
    df = pd.read_csv("world_happiness_2024.csv")
    
    print("Original columns:", df.columns.tolist())
    
    # Map the "Explained by:" columns to our metrics (these are the actual contributions)
    df = df.rename(columns={
        "Country name": "country_name",
        "Ladder score": "ladder_score",
        "Explained by: Log GDP per capita": "gdp_per_capita",
        "Explained by: Social support": "social_support",
        "Explained by: Healthy life expectancy": "healthy_life_expectancy",
        "Explained by: Freedom to make life choices": "freedom_to_make_choices",
        "Explained by: Generosity": "generosity",
        "Explained by: Perceptions of corruption": "perceptions_of_corruption"
    })
    
    # No 'region' in 2024 data — we'll set it to NULL or derive later
    df["region"] = None  # or use a mapping if needed
    
    cols = ["country_name", "region", "ladder_score", "gdp_per_capita", 
            "social_support", "healthy_life_expectancy", "freedom_to_make_choices",
            "generosity", "perceptions_of_corruption"]
    
    df = df[cols]
    
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    
    insert_query = """
    INSERT INTO dim_country 
    (country_name, region, ladder_score, gdp_per_capita, social_support, 
     healthy_life_expectancy, freedom_to_make_choices, generosity, perceptions_of_corruption)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (country_name) DO UPDATE SET
        ladder_score = EXCLUDED.ladder_score,
        gdp_per_capita = EXCLUDED.gdp_per_capita,
        social_support = EXCLUDED.social_support,
        healthy_life_expectancy = EXCLUDED.healthy_life_expectancy,
        freedom_to_make_choices = EXCLUDED.freedom_to_make_choices,
        generosity = EXCLUDED.generosity,
        perceptions_of_corruption = EXCLUDED.perceptions_of_corruption,
        region = EXCLUDED.region;
    """
    
    for _, row in df.iterrows():
        cur.execute(insert_query, tuple(row))
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Successfully loaded {len(df)} countries into dim_country!")
    print("Top 5 happiest:")
    print(df.head()[['country_name', 'ladder_score']])

if __name__ == "__main__":
    load_happiness_data()