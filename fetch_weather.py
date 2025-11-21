import os
import requests
import psycopg2
import time
from datetime import datetime
from dotenv import load_dotenv
from germa_cities import GERMAN_CITIES

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
    
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"  ✗ Error fetching weather for {city}: {e}")
        return None

        
def insert_weather_data(weather_data):
    """Insert weather data into PostgreSQL"""
    if not weather_data:
        return False
    
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
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
        
        # Commit and close
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"  ✓ {city}: {temperature}°C, {weather_description}")
        return True
        
    except psycopg2.Error as e:
        print(f"  ✗ Database error for {city}: {e}")
        return False
    except KeyError as e:
        print(f"  ✗ Missing data field: {e}")
        return False

def main():
    """Main function to fetch and store weather data"""
    print(f"Fetching weather data for {len(GERMAN_CITIES)} German cities...\n")
    
    success_count = 0
    failure_count = 0
    
    for i, city in enumerate(GERMAN_CITIES, 1):
        print(f"[{i}/{len(GERMAN_CITIES)}] Fetching {city}...")
        
        weather_data = fetch_weather(city)
        
        if insert_weather_data(weather_data):
            success_count += 1
        else:
            failure_count += 1
        
        # Rate limiting: OpenWeatherMap free tier allows 60 calls/minute
        # Sleep for 1 second between requests to be safe
        if i < len(GERMAN_CITIES):
            time.sleep(1)
    
    print(f"\n{'='*50}")
    print(f"✓ Successfully inserted: {success_count}")
    print(f"✗ Failed: {failure_count}")
    print(f"{'='*50}")

if __name__ == "__main__":
    start_time = time.time()
    main()
    elapsed = time.time() - start_time
    print(f"Total time: {elapsed:.2f} seconds")
