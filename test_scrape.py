# Update test_scrape.py:
import requests
from bs4 import BeautifulSoup

url = "https://en.wikipedia.org/wiki/November_21"
response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
soup = BeautifulSoup(response.content, 'html.parser')

events_h2 = soup.find('h2', {'id': 'Events'})

if events_h2:
    print("✓ Found Events H2\n")
    
    # Use find_next instead of next_sibling
    print("Using find_next to find <ul>:")
    next_ul = events_h2.find_next('ul')
    
    if next_ul:
        print(f"✓✓✓ Found UL!")
        items = next_ul.find_all('li', recursive=False)
        print(f"Has {len(items)} list items\n")
        
        if items:
            print("First 3 items:")
            for i, li in enumerate(items[:3]):
                text = li.get_text().strip()
                print(f"\n{i+1}. {text[:150]}")
    else:
        print("✗ No UL found")
