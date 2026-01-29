# Replace scrape_wikipedia.py with this fixed version:
import requests
from bs4 import BeautifulSoup
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv
import re
from logger import setup_logger
from retry_utils import retry_with_backoff

# Setup logger
logger = setup_logger(__name__)

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

 
@retry_with_backoff(max_retries=3)
def scrape_wikipedia_events(month, day):
    """Scrape historical events from Wikipedia's 'On this day' pages"""
    
    month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']
    
    month_name = month_names[month - 1]
    url = f"https://en.wikipedia.org/wiki/{month_name}_{day}"
    
    logger.info(f"Scraping: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        events = []
        
        # Find h2 with id="Events"
        events_h2 = soup.find('h2', {'id': 'Events'})
        
        if not events_h2:
            logger.info(f"  ✗ Could not find Events section")
            return events
        
        logger.info(f"  ✓ Found Events section")
        
        # Find ALL ul elements between Events and the next h2 (Births)
        current = events_h2.find_next()
        
        while current:
            # Stop when we hit the next major section (h2)
            if current.name == 'h2':
                break
            
            # Process all <ul> elements
            if current.name == 'ul':
                for li in current.find_all('li', recursive=False):
                    text = li.get_text().strip()
                    
                    # Wikipedia uses various dash formats: –, -, —, etc.
                    # Match: "YEAR [space] [dash] [space] description"
                    year_match = re.match(r'^(\d{1,4})\s*[–\-—]\s*(.+)', text)
                    
                    if year_match:
                        try:
                            year_str = year_match.group(1)
                            description = year_match.group(2).strip()
                            
                            # Handle BCE dates (skip for simplicity)
                            if 'BCE' in text or 'BC' in text:
                                continue
                            
                            year = int(year_str)
                            
                            # Skip if year is unrealistic
                            if 1 <= year <= datetime.now().year:
                                events.append({
                                    'date': f"{year:04d}-{month:02d}-{day:02d}",
                                    'year': year,
                                    'description': description[:500],  # Limit length
                                    'category': 'Historical Event',
                                    'source_url': url
                                })
                        except (ValueError, IndexError):
                            pass
            
            current = current.find_next()
        
        logger.info(f"  ✓ Found {len(events)} events for {month_name} {day}")
        return events
        
    except requests.exceptions.RequestException as e:
        logger.error(f"  ✗ Error scraping {url}: {e}")
        return []

def insert_events(events):
    """Insert events into PostgreSQL"""
    if not events:
        return 0
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        inserted = 0
        for event in events:
            query = """
                INSERT INTO historical_events 
                (event_date, event_year, event_description, event_category, source_url)
                VALUES (%s, %s, %s, %s, %s)
            """
            
            cursor.execute(query, (
                event['date'],
                event['year'],
                event['description'],
                event['category'],
                event['source_url']
            ))
            inserted += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return inserted
        
    except psycopg2.Error as e:
        logger.error(f"  ✗ Database error: {e}")
        return 0

def main():
    """Scrape today's historical events"""
    today = datetime.now()
    month = today.month
    day = today.day
    
    logger.info(f"Scraping historical events for {today.strftime('%B %d')}...\n")
    
    events = scrape_wikipedia_events(month, day)
    
    if events:
        inserted = insert_events(events)
        logger.info(f"\n✓ Inserted {inserted} events into database")
    else:
        logger.info("\n✗ No events found")

if __name__ == "__main__":
    main()
