from functools import lru_cache
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app.models.schemas import BoundingBox, WindDataResponse, WindDataPoint, GribFileInfo, LocationRequest
from app.services.wind_service import WindService
from app.tools.polling import poll_gfs_data, stop_polling, set_download_all_files, start_polling
from typing import List
from datetime import datetime
import json
import geopandas as gpd
from pathlib import Path
import time
import requests

# Get the project root directory (where main.py is)
PROJECT_ROOT = Path(__file__).parent.parent

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

@lru_cache(maxsize=128)
def get_bbox_by_name(name: str) -> dict:
    """Get bounding box by name from shapefiles or Nominatim API with buffer for small areas."""
    name_lower = name.lower()
    buffer = 1.0  # 1-degree buffer for small areas (adjust as needed)

    # Define the shapefiles to check with their name columns
    shapefiles = [
        (MARINE_GDF, "name"),
        (COUNTRIES_GDF, "NAME"),
        (LAKES_GDF, "name"),
    ]

    # Loop through each shapefile
    for gdf, name_column in shapefiles:
        try:
            if gdf is None:
                continue
            location = gdf[gdf[name_column].str.lower() == name_lower]
            if not location.empty:
                bbox = location.total_bounds
                # If the area is small (e.g., an island), add buffer
                lat_range = bbox[3] - bbox[1]
                lon_range = bbox[2] - bbox[0]
                if lat_range < 5 or lon_range < 5:  # Arbitrary threshold for "small"
                    return {
                        "min_lat": bbox[1] - buffer,
                        "max_lat": bbox[3] + buffer,
                        "min_lon": bbox[0] - buffer,
                        "max_lon": bbox[2] + buffer
                    }
                return {"min_lat": bbox[1], "max_lat": bbox[3], "min_lon": bbox[0], "max_lon": bbox[2]}
        except Exception as e:
            print(f"Error checking {name_column} in shapefile: {e}")
            continue

    # Fallback to Nominatim API if no match found in shapefiles
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": name, "format": "json", "limit": 1}
    headers = {"User-Agent": "WindDataAPI/1.0 (your-email@example.com)"}  # Replace with your email
    response = requests.get(url, params=params, headers=headers)
    data = response.json()
    if data and "boundingbox" in data[0]:
        bbox = [float(x) for x in data[0]["boundingbox"]]
        # Add buffer for small areas (e.g., islands)
        lat_range = bbox[1] - bbox[0]
        lon_range = bbox[3] - bbox[2]
        if lat_range < 5 or lon_range < 5:
            return {
                "min_lat": bbox[0] - buffer,
                "max_lat": bbox[1] + buffer,
                "min_lon": bbox[2] - buffer,
                "max_lon": bbox[3] + buffer
            }
        return {"min_lat": bbox[0], "max_lat": bbox[1], "min_lon": bbox[2], "max_lon": bbox[3]}

    raise ValueError(f"Location '{name}' not found")


app = FastAPI(
    title="Wind Data API",
    description="""
    This API provides wind data and visualizations for specified geographical regions using GFS (Global Forecast System) data.
    
    ## Features
    * Get wind speed data for any geographical region
    * Generate wind map visualizations with wind barbs
    * Return data in both tabular and visual formats
    
    ## Data Source
    Uses GFS (Global Forecast System) data from NOAA, providing:
    * 10-meter wind components (U and V)
    * 0.25-degree resolution
    * Updated every 6 hours
    
    ## Usage Example
    ```python
    import requests
    
    # Define the bounding box (5°x5° box around New York area)
    data = {
        "min_lat": 37.5,
        "max_lat": 42.5,
        "min_lon": -72.5,
        "max_lon": -67.5
    }
    
    # Make the API request
    response = requests.post("http://localhost:8000/wind-data", json=data)
    result = response.json()
    
    # Access the data
    print(f"Valid Time: {result['valid_time']}")
    print(f"Number of data points: {len(result['data_points'])}")
    
    # Save the wind map image
    import base64
    with open("wind_map.png", "wb") as f:
        f.write(base64.b64decode(result['image_base64']))
    ```
    """,
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize the wind service
wind_service = WindService()

# Start GFS polling in a background thread
polling_thread = None

@app.on_event("startup")
async def startup_event():
    """Start the GFS polling thread when the application starts"""
    global polling_thread
    polling_thread = start_polling()
    print("GFS polling started in background thread")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up when the application shuts down"""
    global polling_thread
    if polling_thread and polling_thread.is_alive():
        print("Stopping GFS polling thread...")
        stop_polling()
        print("GFS polling thread stop signal sent")

@app.post("/wind-data", 
    response_model=WindDataResponse,
    summary="Get wind data and visualization",
    description="""
    Retrieve wind data and generate a visualization for a specified geographical region.
    You can specify the region either by coordinates or by name (e.g., 'Caribbean Sea').
    
    The endpoint returns:
    * Wind speed data points for each grid location
    * A base64-encoded PNG image of the wind map with wind barbs
    * The valid time of the data
    * Information about the GRIB file used
    
    The wind map includes:
    * Color-coded wind speed visualization
    * Wind barbs showing direction and speed
    * Coastlines for geographical context
    * A colorbar indicating wind speed values
    """,
    responses={
        200: {
            "description": "Successfully retrieved wind data and generated visualization",
            "content": {
                "application/json": {
                    "example": {
                        "valid_time": "2024-03-22T12:00:00",
                        "data_points": [
                            {
                                "latitude": 37.5,
                                "longitude": -72.5,
                                "wind_speed_knots": 15.2
                            }
                        ],
                        "image_base64": "base64_encoded_png_image",
                        "grib_file": {
                            "filename": "gfs.t12z.pgrb2.0p25.f000",
                            "cycle_time": "t12z",
                            "download_time": "2024-03-22T12:30:00",
                            "forecast_hour": 0
                        }
                    }
                }
            }
        },
        404: {
            "description": "Location not found",
            "content": {
                "application/json": {
                    "example": {"detail": "Location 'Unknown Sea' not found in geography database"}
                }
            }
        },
        503: {
            "description": "Service temporarily unavailable - GRIB files not yet available",
            "content": {
                "application/json": {
                    "example": {"detail": "GRIB files not yet available. Please try again in a few minutes."}
                }
            }
        },
        500: {
            "description": "Error processing the request",
            "content": {
                "application/json": {
                    "example": {"detail": "Error opening GRIB file: File not found"}
                }
            }
        }
    }
)
async def get_wind_data(request: BoundingBox):
    """
    Get wind data and visualization for a specified region.
    
    Args:
        request: Either coordinates (min_lat, max_lat, min_lon, max_lon) or a location name
        
    Returns:
        WindDataResponse containing:
        - valid_time: The valid time of the data
        - data_points: List of wind data points with latitude, longitude, and wind speed
        - image_base64: Base64 encoded PNG image of the wind map
        - grib_file: Information about the GRIB file used
        
    Raises:
        HTTPException: If the location is not found or there's an error processing the request
    """
    try:
        if not wind_service.is_ready():
            raise HTTPException(
                status_code=503,
                detail="GRIB files not yet available. Please try again in a few minutes."
            )
            
        # Get coordinates either from name or direct input
        if request.name:
            try:
                bbox = get_bbox_by_name(request.name)
                min_lat, max_lat, min_lon, max_lon = (
                    bbox["min_lat"],
                    bbox["max_lat"],
                    bbox["min_lon"],
                    bbox["max_lon"]
                )
            except ValueError as e:
                raise HTTPException(status_code=404, detail=str(e))
        else:
            min_lat, max_lat, min_lon, max_lon = (
                request.min_lat,
                request.max_lat,
                request.min_lon,
                request.max_lon
            )
            
        data_points, image_base64, valid_time, grib_info = wind_service.process_wind_data(
            min_lat, max_lat, min_lon, max_lon
        )
        
        return WindDataResponse(
            valid_time=valid_time,
            data_points=data_points,
            image_base64=image_base64,
            grib_file=grib_info
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health",
    summary="Health check",
    description="Check if the API is running and healthy",
    responses={
        200: {
            "description": "API is healthy",
            "content": {
                "application/json": {
                    "example": {"status": "healthy"}
                }
            }
        }
    }
)
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "gfs_ready": wind_service.is_ready()}
