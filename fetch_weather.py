import os
import requests
import psycopg2
import time
from datetime import datetime
from dotenv import load_dotenv
from germa_cities import GERMAN_CITIES
from logger import setup_logger
from retry_utils import retry_with_backoff

#add logger
logger = setup_logger(__name__)


# Load environment variables
load_dotenv()

# Database connection details
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# OpenWeatherMap API details
API_KEY = os.getenv('OPENWEATHER_API_KEY')
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

def fetch_weather(city, country_code='DE'):
    """Fetch weather data from OpenWeatherMap API"""
    params = {
        'q': f"{city},{country_code}",
        'appid': API_KEY,
        'units': 'metric'  # Get temperature in Celsius
    }
    

    response = requests.get(BASE_URL, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def validate_weather_data(data):
    """Returns (is_valid, error_message)"""
    try:
        if not data or 'name' not in data:
            return False, "Missing city name"
        
        temp = data['main']['temp']
        if not -50 <= temp <= 60:
            return False, f"Temperature {temp}°C out of range"
        
        #coordinates
        lat = data['coord']['lat']
        lon = data['coord']['lon']
        if not -90 <= lat <= 90:
            return False, f"Invalid latitude {lat}"
        if not -180 <= lon <= 180:
            return False, f"Invalid longitude {lon}"
        
        return True, None

    except (KeyError, TypeError) as e:
        return False, f"Data format error: {e}"


        
def insert_weather_data(cursor, weather_data):
    """Insert weather data into PostgreSQL"""
    if not weather_data:
        return False
    
    try:
        # # Connect to database
        # conn = psycopg2.connect(**DB_CONFIG)
        # cursor = conn.cursor()
    
        # Extract data from API response
        city = weather_data['name']
        temperature = weather_data['main']['temp']
        feels_like = weather_data['main']['feels_like']
        humidity = weather_data['main']['humidity']
        weather_description = weather_data['weather'][0]['description']
        pressure = weather_data['main']['pressure']
        wind_speed = weather_data['wind']['speed']
        wind_direction = weather_data['wind'].get('deg', 0)
        visibility = weather_data.get('visibility', 0)
        lat = weather_data['coord']['lat']
        lon = weather_data['coord']['lon']
        
        # Insert into database
        query = """
            INSERT INTO raw_weather 
            (city, temperature, feels_like, humidity, weather_description, 
             pressure_hpa, wind_speed_ms, wind_direction_deg, visibility_meters, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (city, temperature, feels_like, humidity, weather_description,
                                pressure, wind_speed, wind_direction, visibility, lat, lon))
        
        logger.info(f"Inserted weather data for {city}: {temperature}°C, {weather_description}")
        return True

    except (psycopg2.Error, KeyError) as e:
        logger.error(f"✗ Error inserting {city}: {e}")
        return False

def main():
    """Main function to fetch and store weather data"""
    print(f"Fetching weather data for {len(GERMAN_CITIES)} German cities...\n")
    
    success_count = 0
    failure_count = 0
    
   # Single DB connection for all cities
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        for i, city in enumerate(GERMAN_CITIES, 1):
            logger.info(f"[{i}/{len(GERMAN_CITIES)}] Fetching {city}...")
            
            try:
                weather_data = fetch_weather(city)

                # Validate data first !
                is_valid, error = validate_weather_data(weather_data)
                if not is_valid:
                    logger.warning(f"Invalid data for {city}: {error}")
                    failure_count += 1
                    continue
                
                if insert_weather_data(cursor, weather_data):
                    success_count += 1
                else:
                    failure_count += 1
            except Exception as e:
                logger.error(f"Failed to process {city}: {e}")
                failure_count += 1
            
            if i < len(GERMAN_CITIES):
                time.sleep(1)
        
        conn.commit()
        
    finally:
        cursor.close()
        conn.close()
    
    logger.info(f"✓ Successfully inserted: {success_count}")
    logger.info(f"✗ Failed: {failure_count}")

if __name__ == "__main__":
    start_time = time.time()
    main()
    elapsed = time.time() - start_time
    logger.info(f"Total time: {elapsed:.2f} seconds")
