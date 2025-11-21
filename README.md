# German Weather Data Pipeline

Automated ETL pipeline collecting real-time weather data from the 100 largest German cities.

## Tech Stack
- **Database**: PostgreSQL
- **Language**: Python 3
- **API**: OpenWeatherMap
- **Automation**: Cron (hourly)

## Features
- ✅ Hourly data collection from 100 cities
- ✅ Stores temperature, humidity, wind, pressure, visibility
- ✅ Geographic coordinates for each city
- ✅ Automated pipeline with cron

## Setup
1. Install dependencies: `pip install psycopg2-binary requests python-dotenv`
2. Create `.env` file with your API key
3. Set up PostgreSQL database
4. Run: `python fetch_weather.py`

## Work in Progress
- [ ] Wikipedia historical events scraper
- [ ] Data visualization dashboard
- [ ] Advanced analytics queries

---
*Portfolio project - Part of data engineering learning path*
