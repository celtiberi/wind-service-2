from typing import Dict, Tuple, Optional
from app.models.schemas import BoundingBox, LocationRequest
from functools import lru_cache
import geopandas as gpd
from pathlib import Path
import requests

# Get the project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Load GeoDataFrames once at startup
try:
    MARINE_GDF = gpd.read_file(PROJECT_ROOT / "app" / "natural_earth" / "ne_10m_geography_marine_polys.shp")
    COUNTRIES_GDF = gpd.read_file(PROJECT_ROOT / "app" / "natural_earth" / "ne_10m_admin_0_countries.shp")
    LAKES_GDF = gpd.read_file(PROJECT_ROOT / "app" / "natural_earth" / "ne_10m_lakes.shp")
    print("Successfully loaded geography data files")
except Exception as e:
    print(f"Error loading geography data files: {e}")
    MARINE_GDF = None
    COUNTRIES_GDF = None
    LAKES_GDF = None

def get_bounding_box(request: LocationRequest) -> BoundingBox:
    """
    Get bounding box coordinates from a request.
    
    Args:
        request: BoundingBox request containing either:
            - name: Name of a region
            - lat/lon: Point coordinates
            - min_lat/max_lat/min_lon/max_lon: Explicit bounding box coordinates
    
    Returns:
        BoundingBox
        
    Raises:
        ValueError: If the location name is not found
    """
    if request.name:
        return get_bbox_by_name(request.name)        
    elif request.lat is not None and request.lon is not None:
        # For a single point, create a small box around it
        buffer = 1  # ~11km at the equator
        return BoundingBox(
            min_lat=request.lat - buffer,
            max_lat=request.lat + buffer,
            min_lon=request.lon - buffer,
            max_lon=request.lon + buffer
        )
    else:
        return BoundingBox(
            min_lat=request.min_lat,
            max_lat=request.max_lat,
            min_lon=request.min_lon,
            max_lon=request.max_lon
        )

@lru_cache(maxsize=128)
def get_bbox_by_name(name: str) -> BoundingBox:
    """
    Get bounding box by name from shapefiles or Nominatim API with buffer for small areas.
    
    Args:
        name: Name of the location to look up
        
    Returns:
        BoundingBox object
        
    Raises:
        ValueError: If the location name is not found
    """
    name_lower = name.lower()
    buffer = 3.0  # Buffer for small areas (adjust as needed)

    shapefiles = [
        (MARINE_GDF, "name"),
        (COUNTRIES_GDF, "NAME"),
        (LAKES_GDF, "name"),
    ]

    for gdf, name_column in shapefiles:
        try:
            if gdf is None:
                continue
            location = gdf[gdf[name_column].str.lower() == name_lower]
            if not location.empty:
                bbox_coords = location.total_bounds
                min_lat, max_lat = bbox_coords[1], bbox_coords[3]
                min_lon, max_lon = bbox_coords[0], bbox_coords[2]
                
                lat_range = max_lat - min_lat
                lon_range = max_lon - min_lon
                
                if lat_range < 5 or lon_range < 5:  # Apply buffer for small areas
                    return BoundingBox(
                        min_lat=min_lat - buffer,
                        max_lat=max_lat + buffer,
                        min_lon=min_lon - buffer,
                        max_lon=max_lon + buffer
                    )
                else: # Return exact bounding box for larger areas
                    return BoundingBox(
                        min_lat=min_lat,
                        max_lat=max_lat,
                        min_lon=min_lon,
                        max_lon=max_lon
                    )
        except Exception as e:
            print(f"Error checking {name_column} in shapefile: {e}")
            continue

    # If not found in shapefiles, try Nominatim API
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": name, "format": "json", "limit": 1}
    headers = {"User-Agent": "WeatherDataAPI/1.0 (your-email@example.com)"}
    response = requests.get(url, params=params, headers=headers)
    data = response.json()
    if data and "boundingbox" in data[0]:
        bbox_coords = [float(x) for x in data[0]["boundingbox"]]
        min_lat, max_lat = bbox_coords[0], bbox_coords[1]
        min_lon, max_lon = bbox_coords[2], bbox_coords[3]
        
        lat_range = max_lat - min_lat
        lon_range = max_lon - min_lon
        
        if lat_range < 5 or lon_range < 5: # Apply buffer for small areas
            return BoundingBox(
                min_lat=min_lat - buffer,
                max_lat=max_lat + buffer,
                min_lon=min_lon - buffer,
                max_lon=max_lon + buffer
            )
        else: # Return exact bounding box for larger areas
            return BoundingBox(
                min_lat=min_lat,
                max_lat=max_lat,
                min_lon=min_lon,
                max_lon=max_lon
            )

    raise ValueError(f"Location '{name}' not found") 