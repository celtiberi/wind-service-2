from functools import lru_cache
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app.models.schemas import BoundingBox, WindDataResponse, WindDataPoint, GribFile, LocationRequest, MarineHazardsResponse, PrecipitationDataPoint
from app.services.weather_service import WeatherService
from app.services.process_weather_data import logger
from typing import List, Dict
from datetime import datetime
import json
import geopandas as gpd
from pathlib import Path
import time
import requests
from pydantic import BaseModel


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
    buffer = 3.0  # 1-degree buffer for small areas (adjust as needed)

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
    headers = {"User-Agent": "WeatherDataAPI/1.0 (your-email@example.com)"}  # Replace with your email
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
    title="Weather Data API",
    description="""
    This API provides weather data and visualizations for specified geographical regions using GFS (Global Forecast System) data.
    
    ## Features
    * Get wind speed data for any geographical region
    * Get precipitation rate data for any geographical region
    * Generate wind map visualizations with wind barbs
    * Generate precipitation map visualizations with storm indicators
    * Return data in both tabular and visual formats
    * Provide storm indicators (heavy rain, lightning potential, storm clouds, etc.)
    
    ## Data Source
    Uses GFS (Global Forecast System) data from NOAA, providing:
    * 10-meter wind components (U and V)
    * Total Precipitation Rate
    * Convective parameters (CAPE, CIN, storm relative helicity)
    * Cloud cover and radar reflectivity
    * 0.25-degree resolution
    * Updated every 6 hours
    
    ## Usage Example (Wind Data)
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

    ## Usage Example (Precipitation Data with Storm Indicators)
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
    response = requests.post("http://localhost:8000/precipitation-data", json=data)
    result = response.json()
    
    # Access the data
    print(f"Valid Time: {result['valid_time']}")
    print(f"Number of data points: {len(result['data_points'])}")
    print(f"Storm Indicators: {result['storm_indicators']}")
    
    # Save the precipitation map image
    import base64
    with open("precipitation_map.png", "wb") as f:
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

# Initialize the weather service
wind_service = WeatherService()


@app.on_event("startup")
async def startup_event():
    """Start the GFS polling thread when the application starts"""
    print("GFS polling started in background thread")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up when the application shuts down"""

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
                            "path": "gfs.t12z.pgrb2.0p25.f000",
                            "download_time": "2024-03-22T12:30:00",
                            "metadata": {
                                "cycle": "t12z",
                                "resolution": "0p25",
                                "forecast_hour": "f000"
                            }
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
        - description: Text description of current conditions
        
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
            
        data_points, image_base64, valid_time, grib_file, description = wind_service.process_wind_data(
            min_lat, max_lat, min_lon, max_lon
        )
        
        return WindDataResponse(
            valid_time=valid_time,
            data_points=data_points,
            image_base64=image_base64,
            grib_file=grib_file,
            description=description
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/marine-hazards", 
    response_model=MarineHazardsResponse,
    summary="Get marine hazards data, visualization, and storm indicators",
    description="""
    Retrieve marine hazards data, generate a visualization, and provide storm indicators for a specified geographical region.
    You can specify the region either by coordinates or by name (e.g., 'Caribbean Sea').
    
    The endpoint returns:
    * data_points: List of PrecipitationDataPoint objects containing latitude, longitude and precipitation_rate_mmh
    * image_base64: Base64-encoded PNG image of the precipitation visualization
    * valid_time: Datetime when the data is valid for
    * grib_file: GribFile object with path, download time and metadata
    * storm_indicators: Dictionary of storm risk indicators and their detailed values
    * description: Text description of current storm conditions and hazards
    
    The precipitation map includes:
    * Color-coded marine hazards visualization
    * Coastlines for geographical context
    * A colorbar indicating precipitation rate values
    * A text overlay in the upper right corner listing active storm indicators (e.g., "Storm Indicators:\n- Heavy Rain Risk\n- Storm Clouds")
    
    Storm indicators include:
    * Heavy rain risk (precipitation rate > 5 mm/hour)
    * Lightning potential (CAPE > 1000 J/kg, CIN < 50 J/kg, or radar reflectivity > 40 dB)
    * Storm clouds (total cloud cover > 80% or low cloud cover > 70%)
    * Severe storm risk (storm relative helicity > 150 m²/s²)
    * Frozen precipitation risk (percent frozen precipitation > 50%)
    """,
    responses={
        200: {
            "description": "Successfully retrieved marine hazards data, generated visualization, and provided storm indicators",
            "content": {
                "application/json": {
                    "example": {
                        "valid_time": "2024-03-22T12:00:00",
                        "data_points": [
                            {
                                "latitude": 37.5,
                                "longitude": -72.5,
                                "precipitation_rate_mmh": 2.5
                            }
                        ],
                        "image_base64": "base64_encoded_png_image",
                        "grib_file": {
                            "path": "gfs.t12z.pgrb2.0p25.f000",
                            "download_time": "2024-03-22T12:30:00",
                            "metadata": {
                                "cycle": "t12z",
                                "resolution": "0p25",
                                "forecast_hour": "f000"
                            }
                        },
                        "storm_indicators": {
                            "heavy_rain_risk": True,
                            "lightning_potential": False,
                            "storm_clouds": True,
                            "severe_storm_risk": False,
                            "frozen_precipitation_risk": False,
                            "details": {
                                "max_precipitation_rate_mmh": 6.2,
                                "rain_present": True,
                                "max_cape_jkg": 800,
                                "min_cin_jkg": 20,
                                "max_radar_reflectivity_db": 45,
                                "max_total_cloud_cover_percent": 90,
                                "max_low_cloud_cover_percent": 75,
                                "max_storm_relative_helicity_m2s2": 120,
                                "max_percent_frozen_precipitation": 10
                            }
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
async def get_marine_hazards(request: BoundingBox):
    """
    Get precipitation data, visualization, and storm indicators for a specified region.
    
    Args:
        request: Either coordinates (min_lat, max_lat, min_lon, max_lon) or a location name
        
    Returns:
        MarineHazardsResponse containing:
        - valid_time: The valid time of the data
        - data_points: List of precipitation data points with latitude, longitude, and precipitation rate
        - image_base64: Base64 encoded PNG image of the precipitation map with storm indicators
        - grib_file: Information about the GRIB file used
        - storm_indicators: Dictionary of storm-related indicators
        - description: Text description of current conditions
        
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
            
        data_points, image_base64, valid_time, grib_file, storm_indicators, description = wind_service.process_marine_hazards(
            min_lat, max_lat, min_lon, max_lon
        )
        
        return MarineHazardsResponse(
            valid_time=valid_time,
            data_points=data_points,
            image_base64=image_base64,
            grib_file=grib_file,
            storm_indicators=storm_indicators,
            description=description
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