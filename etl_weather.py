import requests
import psycopg2
from config import PG_CONFIG, CITIES
import logging

logging.basicConfig(level=logging.INFO)

COUNTRY_MAPPING = {
    "UK": "United Kingdom",
    "US": "United States",
    "UAE": "United Arab Emirates",
    "Turkey": "Turkiye",
    "South Korea": "South Korea",
    "Russia": "Russia",
    "China": "China",
    "Brazil": "Brazil",
}

def fetch_coordinates(city):
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": city, "format": "json", "limit": 1}
    headers = {"User-Agent": "automated-data-warehouse/1.0 (@de_clerke Kenya)"}
    try:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200 and response.json():
            data = response.json()[0]
            return float(data["lat"]), float(data["lon"])
    except Exception as e:
        logging.warning(f"Geocoding error for {city}: {e}")
    return None, None

def fetch_and_load_weather():
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    
    city_insert_query = """
    INSERT INTO dim_city (city_name, country_name) 
    VALUES (%s, %s) 
    ON CONFLICT (city_name) DO UPDATE SET country_name = EXCLUDED.country_name
    RETURNING city_id
    """
    
    weather_insert_query = """
    INSERT INTO fact_weather_snapshot 
    (city_id, temperature_celsius, feels_like_celsius, humidity_percent,
     weather_main, weather_description, wind_speed_mps)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
    """
    
    for city in CITIES:
        lat, lon = fetch_coordinates(city)
        if not lat or not lon:
            continue
        
        raw_country = city.split(",")[-1].strip()
        country = COUNTRY_MAPPING.get(raw_country, raw_country)
        
        try:
            cur.execute(city_insert_query, (city, country))
            city_id = cur.fetchone()[0]
            
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,apparent_temperature,relative_humidity_2m,weather_code,wind_speed_10m",
                "timezone": "auto"
            }
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()["current"]
                code = data["weather_code"]
                descriptions = {
                    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
                    45: "Fog", 48: "Depositing rime fog",
                    51: "Light drizzle", 61: "Rain", 71: "Snow", 80: "Rain showers",
                    95: "Thunderstorm"
                }
                weather_desc = descriptions.get(code, f"Weather code {code}")
                weather_main = "Clouds" if 1 <= code <= 3 else weather_desc.split()[0]
                
                cur.execute(weather_insert_query, (
                    city_id,
                    data["temperature_2m"],
                    data["apparent_temperature"],
                    data["relative_humidity_2m"],
                    weather_main,
                    weather_desc,
                    data["wind_speed_10m"]
                ))
                conn.commit()
                print(f"Loaded weather for {city} ({country}): {data['temperature_2m']}°C")
            
        except psycopg2.Error as e:
            conn.rollback()
            logging.error(f"Error skipping {city}: {e}")
            continue
    
    cur.close()
    conn.close()
    print("Open-Meteo Weather ETL complete!")

if __name__ == "__main__":
    fetch_and_load_weather()