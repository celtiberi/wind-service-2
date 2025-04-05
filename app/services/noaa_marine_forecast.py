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
from app.models.schemas import MarineForecastResponse # Import the response model

class NOAAMarineForecast:
    HIGH_SEAS_NAME_TO_URL = {
        'North Atlantic Ocean between 31N and 67N latitude and between the East Coast North America and 35W longitude':
          'https://tgftp.nws.noaa.gov/data/raw/fz/fznt01.kwbc.hsf.at1.txt',
        'Atlantic Ocean West of 35W longitude between 31N latitude and 7N latitude .  This includes the Caribbean and the Gulf of Mexico':
          'https://tgftp.nws.noaa.gov/data/raw/fz/fznt02.knhc.hsf.at2.txt',
        'North Pacific Ocean between 30N and the Bering Strait and between the West Coast of North America and 160E Longitude':
          'https://tgftp.nws.noaa.gov/data/raw/fz/fzpn02.kwbc.hsf.epi.txt',
        'Central North Pacific Ocean between the Equator and 30N latitude and between 140W longitude and 160E longitude':
          'https://tgftp.nws.noaa.gov/data/raw/fz/fzpn40.phfo.hsf.np.txt',
        'Eastern North Pacific Ocean between the Equator and 30N latitude and east of 140W longitude and 3.4S to Equator east of 120W longitude':
          'https://tgftp.nws.noaa.gov/data/raw/fz/fzpn03.knhc.hsf.ep2.txt',
        'Central South Pacific Ocean between the Equator and 25S latitude and between 120W longitude and 160E longitude':
          'https://tgftp.nws.noaa.gov/data/raw/fz/fzps40.phfo.hsf.sp.txt',
      }
      
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
        """Build or load a mapping of zone IDs to forecast URLs by finding relevant links."""
        # if self.forecast_urls_file.exists():
        #     try:
        #         with open(self.forecast_urls_file, 'r') as f:
        #             mapping = json.load(f)
        #             print(f"Loaded existing forecast mapping with {len(mapping)} entries.")
        #             return mapping
        #     except json.JSONDecodeError:
        #         print("Error reading forecast mapping file, rebuilding...")
        #     except Exception as e:
        #         print(f"Error loading forecast mapping file ({e}), rebuilding...")

        zone_to_url = {}
        # Regex to capture zone ID from filename (e.g., ANZ050, PKZ311)
        # Anchored (^$) and with a capture group ()
        zone_pattern = re.compile(r'^([a-zA-Z]{3}[0-9]{3})\.txt$', re.IGNORECASE)
        
        print("Building forecast mapping from regional pages...")
        for region_url in self.regional_links:
            print(f"Processing: {region_url}")
            try:
                response = requests.get(region_url, timeout=10) # Add timeout
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                found_count = 0
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    # Check if the link points to a marine forecast txt file
                    if '/data/forecasts/marine/' in href and href.endswith('.txt'):
                        # Extract filename
                        filename = href.split('/')[-1] # Get last part of path
                        
                        match = zone_pattern.search(filename) # Search the filename only
                        if match:
                            zone_id = match.group(1).upper() # Extract captured group (the ID)
                            
                            # Construct full URL if relative
                            full_url = href
                            if not href.startswith('http'):
                                # Find the base URL (handle potential tgftp subdomain)
                                base_url = "https://tgftp.nws.noaa.gov" if "tgftp" in full_url else "https://www.weather.gov"
                                # Ensure we don't add double slashes if href starts with one
                                if href.startswith('/'):
                                    full_url = base_url + href
                                else:
                                    full_url = base_url + '/' + href # Should not happen often with this structure

                            if zone_id not in zone_to_url: # Avoid duplicates/overwrites
                                zone_to_url[zone_id] = full_url
                                found_count += 1
                                # print(f"  Found: {zone_id} -> {full_url}") # Optional: Debug print
                print(f"  Found {found_count} forecast links on this page.")

            except requests.RequestException as e:
                print(f"  Error fetching or parsing {region_url}: {e}")
            except Exception as e:
                 print(f"  Unexpected error processing {region_url}: {e}")

        print(f"Finished building forecast mapping. Total zones found: {len(zone_to_url)}")
        try:
            with open(self.forecast_urls_file, 'w') as f:
                json.dump(zone_to_url, f, indent=4)
            print(f"Saved forecast mapping to {self.forecast_urls_file}")
        except IOError as e:
            print(f"Error saving forecast mapping file: {e}")
           
        return zone_to_url

    def load_shapefiles(self, metadata):
        """Load shapefiles into a single GeoDataFrame."""
        all_zones = []
        required_columns = ['ID', 'NAME', 'geometry'] # Ensure NAME column is considered
        for title, info in metadata.items():
            try:
                gdf = gpd.read_file(info["filename"])
                # Standardize column names if possible (example, may need adjustment)
                if 'id' in gdf.columns and 'ID' not in gdf.columns:
                    gdf = gdf.rename(columns={'id': 'ID'})
                if 'name' in gdf.columns and 'NAME' not in gdf.columns:
                     gdf = gdf.rename(columns={'name': 'NAME'})
                
                # Select only necessary columns, handling potential missing ones
                cols_to_use = [col for col in required_columns if col in gdf.columns]
                all_zones.append(gdf[cols_to_use])
            except Exception as e:
                print(f"Error loading or processing shapefile {info['filename']}: {e}")
                
        if not all_zones:
            raise ValueError("No valid shapefiles could be loaded.")
        # Ensure the final concatenated GDF has at least the geometry column
        combined_gdf = pd.concat(all_zones, ignore_index=True)
        if 'geometry' not in combined_gdf.columns:
            raise ValueError("Concatenated GeoDataFrame must contain a 'geometry' column.")
            
        # Fill missing ID/NAME columns if they exist after concat, before use
        if 'ID' not in combined_gdf.columns:
             combined_gdf['ID'] = pd.NA
        if 'NAME' not in combined_gdf.columns:
            combined_gdf['NAME'] = pd.NA
            
        return combined_gdf

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
        """Find the marine zone (ID or High Seas Name) containing the given coordinate."""
        if self.zones is None:
            raise ValueError("Zones not loaded. Call initialize() first.")
            
        point = Point(lon, lat)
        for _, zone in self.zones.iterrows():
            # Check geometry first
            if 'geometry' in zone and zone.geometry.contains(point):
                zone_id = zone.get('ID')
                zone_name = zone.get('NAME')
                
                # Prefer standard ID if valid
                if pd.notna(zone_id) and isinstance(zone_id, str) and zone_id.strip():
                    return zone_id.strip()
                    
                # Fallback to NAME if it matches a High Seas forecast key
                if pd.notna(zone_name) and isinstance(zone_name, str) and zone_name.strip() in self.HIGH_SEAS_NAME_TO_URL:
                    return zone_name.strip()
                
                # If geometry matched but no valid ID/Name found for forecast, continue search
                # (or log a warning if needed)
                    
        return None # No matching zone with a usable ID or NAME found

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

    def get_forecast_for_zone(self, zone_identifier):
        """Fetch the full forecast text for a given marine zone ID or High Seas Name."""
        if self.forecast_mapping is None:
            # Attempt to load mapping if not already loaded during initialize
            try:
                self.forecast_mapping = self.build_forecast_mapping()
                if self.forecast_mapping is None:
                     raise ValueError("Forecast mapping could not be built or loaded.")
            except Exception as e:
                 print(f"Error building/loading forecast mapping: {e}")
                 raise ValueError("Forecast mapping unavailable.")

        url = None
        # Try standard zone ID mapping first using .get()
        if isinstance(zone_identifier, str):
             url = self.forecast_mapping.get(zone_identifier)

        # If not found in standard map, try High Seas Name mapping using .get()
        if url is None and isinstance(zone_identifier, str):
            url = self.HIGH_SEAS_NAME_TO_URL.get(zone_identifier)
            
        if not url:
            print(f"No forecast URL found for identifier: {zone_identifier}")
            return None

        try:
            response = requests.get(url, timeout=10) # Add timeout
            response.raise_for_status()
            return response.text.strip()
        except requests.RequestException as e:
            print(f"Error fetching forecast for {zone_identifier} from {url}: {e}")
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

    def get_forecast(self, lat=None, lon=None, bbox=None) -> MarineForecastResponse:
        """
        Get the marine forecast for a specific location or bounding box.
        
        Args:
            lat (float, optional): Latitude for point lookup
            lon (float, optional): Longitude for point lookup
            bbox (tuple, optional): (min_lon, min_lat, max_lon, max_lat) for bounding box lookup
            
        Returns:
            MarineForecastResponse: Containing the forecast text or an error message
                                   in the 'forecast' field.
        """
        if self.zones is None or self.forecast_mapping is None:
            try:
                self.initialize()
            except Exception as e:
                # Handle initialization errors
                error_msg = f"Initialization error: {e}"
                print(error_msg)
                return MarineForecastResponse(forecast=error_msg)

        used_lat, used_lon = None, None # Initialize
        if lat is not None and lon is not None:
            zone_id = self.get_zone_for_coordinate(lat, lon)
            used_lat, used_lon = lat, lon
        elif bbox is not None:
            min_lon, min_lat, max_lon, max_lat = bbox
            zone_id, used_lat, used_lon = self.get_zone_for_bbox(min_lon, min_lat, max_lon, max_lat)
        else:
            return MarineForecastResponse(
                forecast='Either lat/lon or bbox must be provided'
            )

        if not zone_id:
            return MarineForecastResponse(
                forecast='No marine zone found for the given location',
                lat=used_lat,
                lon=used_lon
            )

        forecast_text = self.get_forecast_for_zone(zone_id)
        if not forecast_text:
            return MarineForecastResponse(
                forecast=f'No forecast found for zone {zone_id}',
                zone_id=zone_id,
                lat=used_lat,
                lon=used_lon
            )

        return MarineForecastResponse(
            forecast=forecast_text,
            zone_id=zone_id,
            lat=used_lat,
            lon=used_lon
        )