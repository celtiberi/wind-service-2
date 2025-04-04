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
from pathlib import Path

class NOAAMarineForecast:
    def __init__(self):
        # Directory to store shapefiles, metadata, and forecast mappings
        self.download_dir = Path("marine_shapefiles")
        self.download_dir.mkdir(exist_ok=True)
        self.metadata_file = self.download_dir / "metadata.json"
        self.forecast_urls_file = self.download_dir / "forecast_urls.json"

        # URLs
        self.marine_zones_url = "https://www.weather.gov/gis/MarineZones"

        # Shapefile titles to download
        self.shapefile_titles = [
            "Coastal Marine Zones Including the Great Lakes",
            "Offshore Marine Zones",
            "High Seas Marine Zones"
        ]

        # Hardcoded regional forecast links
        self.regional_links = [
            "https://www.weather.gov/marine/atlantictext",
            "https://www.weather.gov/marine/pacifictext",
            "https://www.weather.gov/marine/gulftext",
            "https://www.weather.gov/marine/greatlakestext",
            "https://www.weather.gov/marine/alaskatext",
            "https://www.weather.gov/marine/hawaiitext"
        ]

        # Initialize instance variables
        self.zones = None
        self.forecast_mapping = None
        self.metadata = None

    def download_shapefiles(self):
        """Download the latest shapefile for each zone type based on the most recent valid date."""
        response = requests.get(self.marine_zones_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        shapefiles_by_title = {title: [] for title in self.shapefile_titles}
        current_title = None
        rowspan_remaining = 0

        for tr in soup.find_all('tr'):
            tds = tr.find_all('td')
            if not tds or len(tds) < 2:
                continue

            if tds[0].get('rowspan'):
                description = " ".join(tds[0].text.split())
                for title in self.shapefile_titles:
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

        latest_shapefiles = {}
        for title in self.shapefile_titles:
            if shapefiles_by_title[title]:
                latest = max(shapefiles_by_title[title], key=lambda x: x["valid_date_obj"])
                latest_shapefiles[title] = latest

        metadata = {}
        for title, info in latest_shapefiles.items():
            shp_response = requests.get(info["link"])
            shp_response.raise_for_status()
            filename = self.download_dir / os.path.basename(info["link"])
            with open(filename, 'wb') as f:
                f.write(shp_response.content)

            metadata[title] = {
                "filename": str(filename),
                "valid_date": info["valid_date"]
            }

        with open(self.metadata_file, 'w') as f:
            json.dump(metadata, f, indent=4)
        return metadata

    def build_forecast_mapping(self):
        """Build or load a mapping of zone IDs or numbers to forecast URLs."""
        if self.forecast_urls_file.exists():
            with open(self.forecast_urls_file, 'r') as f:
                return json.load(f)

        zone_to_url = {}
        for region_url in self.regional_links:
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

        with open(self.forecast_urls_file, 'w') as f:
            json.dump(zone_to_url, f, indent=4)
        return zone_to_url

    def load_shapefiles(self, metadata):
        """Load shapefiles into a single GeoDataFrame."""
        all_zones = []
        for title, info in metadata.items():
            gdf = gpd.read_file(info["filename"])
            all_zones.append(gdf)
        return pd.concat(all_zones, ignore_index=True)

    def check_for_updates(self):
        """Check if shapefiles need updating based on valid dates."""
        if not self.metadata_file.exists():
            return True

        with open(self.metadata_file, 'r') as f:
            metadata = json.load(f)

        current_date = datetime.now()
        for title, info in metadata.items():
            valid_date = datetime.strptime(info["valid_date"], "%m/%d/%Y")
            if current_date > valid_date + timedelta(weeks=1):
                return True
        return False

    def get_zone_for_coordinate(self, lat, lon):
        """Find the marine zone containing the given coordinate."""
        if self.zones is None:
            raise ValueError("Zones not loaded. Call initialize() first.")
            
        point = Point(lon, lat)
        for _, zone in self.zones.iterrows():
            if zone.geometry.contains(point):
                return zone['ID']
        return None

    def get_zone_for_bbox(self, min_lon, min_lat, max_lon, max_lat):
        """Find a marine zone within the bounding box, trying multiple points if needed."""
        if self.zones is None:
            raise ValueError("Zones not loaded. Call initialize() first.")
            
        # List of points to try: center, then corners
        points_to_try = [
            ((min_lat + max_lat) / 2, (min_lon + max_lon) / 2),  # Center
            (min_lat, min_lon),  # Bottom-left
            (max_lat, min_lon),  # Top-left
            (min_lat, max_lon),  # Bottom-right
            (max_lat, max_lon)   # Top-right
        ]

        for lat, lon in points_to_try:
            zone_id = self.get_zone_for_coordinate(lat, lon)
            if zone_id:
                return (zone_id, lat, lon)  # Return zone ID and the coordinate that worked
        return (None, None, None)  # No marine zone found

    def get_forecast_for_zone(self, zone_id):
        """Fetch the full forecast text for a given marine zone."""
        if self.forecast_mapping is None:
            raise ValueError("Forecast mapping not loaded. Call initialize() first.")
            
        url = self.forecast_mapping.get(zone_id)
        if not url:
            return None

        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.text.strip()
        except requests.RequestException as e:
            print(f"Error fetching forecast: {e}")
            return None

    def initialize(self):
        """Initialize the service by loading or downloading necessary data."""
        # Check for updates and download shapefiles if needed
        if self.check_for_updates():
            self.metadata = self.download_shapefiles()
        else:
            with open(self.metadata_file, 'r') as f:
                self.metadata = json.load(f)

        # Load or build forecast mapping
        self.forecast_mapping = self.build_forecast_mapping()

        # Load shapefiles
        self.zones = self.load_shapefiles(self.metadata)

    def get_forecast(self, lat=None, lon=None, bbox=None):
        """
        Get the marine forecast for a specific location or bounding box.
        
        Args:
            lat (float, optional): Latitude for point lookup
            lon (float, optional): Longitude for point lookup
            bbox (tuple, optional): (min_lon, min_lat, max_lon, max_lat) for bounding box lookup
            
        Returns:
            dict: {
                'forecast': str,  # The forecast text
                'zone_id': str,   # The zone ID
                'lat': float,     # The latitude used
                'lon': float,     # The longitude used
                'error': str      # Error message if any
            }
        """
        if self.zones is None or self.forecast_mapping is None:
            self.initialize()

        if lat is not None and lon is not None:
            zone_id = self.get_zone_for_coordinate(lat, lon)
            used_lat, used_lon = lat, lon
        elif bbox is not None:
            min_lon, min_lat, max_lon, max_lat = bbox
            zone_id, used_lat, used_lon = self.get_zone_for_bbox(min_lon, min_lat, max_lon, max_lat)
        else:
            return {
                'error': 'Either lat/lon or bbox must be provided',
                'forecast': None,
                'zone_id': None,
                'lat': None,
                'lon': None
            }

        if not zone_id:
            return {
                'error': 'No marine zone found for the given location',
                'forecast': None,
                'zone_id': None,
                'lat': used_lat,
                'lon': used_lon
            }

        forecast = self.get_forecast_for_zone(zone_id)
        if not forecast:
            return {
                'error': f'No forecast found for zone {zone_id}',
                'forecast': None,
                'zone_id': zone_id,
                'lat': used_lat,
                'lon': used_lon
            }

        return {
            'error': None,
            'forecast': forecast,
            'zone_id': zone_id,
            'lat': used_lat,
            'lon': used_lon
        }