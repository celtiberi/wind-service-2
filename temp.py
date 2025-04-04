import requests
from bs4 import BeautifulSoup
import os
import json
from datetime import datetime, timedelta
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import re
import unicodedata

# Directory to store shapefiles, metadata, and forecast mappings
DOWNLOAD_DIR = "marine_shapefiles"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
METADATA_FILE = os.path.join(DOWNLOAD_DIR, "metadata.json")
FORECAST_URLS_FILE = os.path.join(DOWNLOAD_DIR, "forecast_urls.json")

# URLs
MARINE_ZONES_URL = "https://www.weather.gov/gis/MarineZones"

# Shapefile titles to download
SHAPEFILE_TITLES = [
    "Coastal Marine Zones Including the Great Lakes",
    "Offshore Marine Zones",
    "High Seas Marine Zones"
]

# Hardcoded regional forecast links
REGIONAL_LINKS = [
    "https://www.weather.gov/marine/atlantictext",
    "https://www.weather.gov/marine/pacifictext",
    "https://www.weather.gov/marine/gulftext",
    "https://www.weather.gov/marine/greatlakestext",
    "https://www.weather.gov/marine/alaskatext",
    "https://www.weather.gov/marine/hawaiitext"
]

def download_shapefiles():
    """Download the latest shapefile for each zone type based on the most recent valid date."""
    response = requests.get(MARINE_ZONES_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    shapefiles_by_title = {title: [] for title in SHAPEFILE_TITLES}
    current_title = None
    rowspan_remaining = 0

    for tr in soup.find_all('tr'):
        tds = tr.find_all('td')
        if not tds or len(tds) < 2:
            continue

        if tds[0].get('rowspan'):
            description = " ".join(tds[0].text.split())
            for title in SHAPEFILE_TITLES:
                if title in description:
                    current_title = title
                    rowspan_remaining = int(tds[0].get('rowspan', 1)) - 1
                    date_idx, link_idx = 1, 2
                    break
        elif current_title and rowspan_remaining > 0:
            date_idx, link_idx = 0, 1
            rowspan_remaining -= 1
        else:
            continue

        date_str = tds[date_idx].text.strip()
        try:
            valid_date = datetime.strptime(date_str, "%d %B %Y")
        except ValueError:
            continue

        link = tds[link_idx].find('a')['href']
        if not link.startswith('http'):
            link = "https://www.weather.gov" + link

        shapefiles_by_title[current_title].append({
            "valid_date_obj": valid_date,
            "valid_date": valid_date.strftime("%m/%d/%Y"),
            "link": link
        })
        print(f"Found shapefile: {current_title} - Valid Date: {date_str}")

    latest_shapefiles = {}
    for title in SHAPEFILE_TITLES:
        if shapefiles_by_title[title]:
            latest = max(shapefiles_by_title[title], key=lambda x: x["valid_date_obj"])
            latest_shapefiles[title] = latest

    metadata = {}
    for title, info in latest_shapefiles.items():
        shp_response = requests.get(info["link"])
        shp_response.raise_for_status()
        filename = os.path.join(DOWNLOAD_DIR, os.path.basename(info["link"]))
        with open(filename, 'wb') as f:
            f.write(shp_response.content)

        metadata[title] = {
            "filename": filename,
            "valid_date": info["valid_date"]
        }

    with open(METADATA_FILE, 'w') as f:
        json.dump(metadata, f, indent=4)
    return metadata

def build_forecast_mapping():
    """Build or load a mapping of zone IDs or numbers to forecast URLs."""
    if os.path.exists(FORECAST_URLS_FILE):
        with open(FORECAST_URLS_FILE, 'r') as f:
            print(f"Loading forecast URLs from {FORECAST_URLS_FILE}")
            return json.load(f)

    print(f"Building new forecast URL mapping...")
    zone_to_url = {}
    for region_url in REGIONAL_LINKS:
        response = requests.get(region_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        for li in soup.find_all('li'):
            a = li.find('a', href=True)
            if a and a['href'].endswith('.txt'):
                href = a['href']
                if not href.startswith('http'):
                    href = "https://www.weather.gov" + href
                
                text = unicodedata.normalize('NFKD', li.get_text()).encode('ASCII', 'ignore').decode()
                zone_match = re.search(r'\(([^()]+)\)', text)
                if zone_match:
                    zone_info = zone_match.group(1)
                    zone_parts = zone_info.split('/')
                    zone_id = zone_parts[0]
                    zone_to_url[zone_id] = href

    with open(FORECAST_URLS_FILE, 'w') as f:
        json.dump(zone_to_url, f, indent=4)
    print(f"Forecast URLs saved to {FORECAST_URLS_FILE}")
    return zone_to_url

def load_shapefiles(metadata):
    """Load shapefiles into a single GeoDataFrame."""
    all_zones = []
    for title, info in metadata.items():
        gdf = gpd.read_file(info["filename"])
        all_zones.append(gdf)
    return pd.concat(all_zones, ignore_index=True)

def check_for_updates():
    """Check if shapefiles need updating based on valid dates."""
    if not os.path.exists(METADATA_FILE):
        return True

    with open(METADATA_FILE, 'r') as f:
        metadata = json.load(f)

    current_date = datetime.now()
    for title, info in metadata.items():
        valid_date = datetime.strptime(info["valid_date"], "%m/%d/%Y")
        if current_date > valid_date + timedelta(weeks=1):
            return True
    return False

def get_zone_for_coordinate(lat, lon, zones):
    """Find the marine zone containing the given coordinate."""
    point = Point(lon, lat)
    for _, zone in zones.iterrows():
        if zone.geometry.contains(point):
            return zone['ID']  # Adjust field name if different
    return None

def get_zone_for_bbox(min_lon, min_lat, max_lon, max_lat, zones):
    """Find a marine zone within the bounding box, trying multiple points if needed."""
    # List of points to try: center, then corners
    points_to_try = [
        ((min_lat + max_lat) / 2, (min_lon + max_lon) / 2),  # Center
        (min_lat, min_lon),  # Bottom-left
        (max_lat, min_lon),  # Top-left
        (min_lat, max_lon),  # Bottom-right
        (max_lat, max_lon)   # Top-right
    ]

    for lat, lon in points_to_try:
        zone_id = get_zone_for_coordinate(lat, lon, zones)
        if zone_id:
            return (zone_id, lat, lon)  # Return zone ID and the coordinate that worked
    return (None, None, None)  # No marine zone found

def get_forecast_for_zone(zone_id, forecast_mapping):
    """Fetch the full forecast text for a given marine zone."""
    url = forecast_mapping.get(zone_id)
    if not url:
        return f"No forecast URL found for zone {zone_id}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text.strip()
    except requests.RequestException as e:
        print(f"Error fetching forecast: {e}")
        return None

def main():
    # Check for updates and download shapefiles if needed
    if check_for_updates():
        print("Downloading new shapefiles...")
        metadata = download_shapefiles()
    else:
        print("Using existing shapefiles...")
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)

    # Load or build forecast mapping
    forecast_mapping = build_forecast_mapping()

    # Load shapefiles
    zones = load_shapefiles(metadata)

    # Example: Single coordinate (Caribbean)
    lat, lon = 20.090, -83.502
    zone_id = get_zone_for_coordinate(lat, lon, zones)
    if zone_id:
        forecast = get_forecast_for_zone(zone_id, forecast_mapping)
        if forecast:
            print(f"Forecast for coordinate ({lat}, {lon}) in zone {zone_id}:\n{forecast}")
        else:
            print(f"No forecast found for zone {zone_id}")
    else:
        print(f"No marine zone found for coordinate ({lat}, {lon})")

    # Example: Bounding box (part of Atlantic)
    min_lon, min_lat, max_lon, max_lat = -80, 25, -70, 35
    zone_id, used_lat, used_lon = get_zone_for_bbox(min_lon, min_lat, max_lon, max_lat, zones)
    if zone_id:
        forecast = get_forecast_for_zone(zone_id, forecast_mapping)
        if forecast:
            print(f"\nForecast for bounding box ({min_lon}, {min_lat}, {max_lon}, {max_lat}) "
                  f"using point ({used_lat:.3f}, {used_lon:.3f}) in zone {zone_id}:\n{forecast}")
        else:
            print(f"\nNo forecast found for bounding box ({min_lon}, {min_lat}, {max_lon}, {max_lat}) "
                  f"using point ({used_lat:.3f}, {used_lon:.3f}) in zone {zone_id}")
    else:
        print(f"\nNo marine zone found within bounding box ({min_lon}, {min_lat}, {max_lon}, {max_lat})")

if __name__ == "__main__":
    main()