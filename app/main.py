from functools import lru_cache
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app.models.schemas import BoundingBox, WindDataResponse, WaveDataResponse, WaveDataPoint, GribFile, LocationRequest, MarineHazardsResponse, PrecipitationDataPoint
from app.services.weather_service import WeatherService
from app.services.process_weather_data import logger
from typing import List, Dict, Optional
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
    LAKES_GDF = None

@lru_cache(maxsize=128)
def get_bbox_by_name(name: str) -> dict:
    """Get bounding box by name from shapefiles or Nominatim API with buffer for small areas."""
    name_lower = name.lower()
    buffer = 3.0  # 1-degree buffer for small areas (adjust as needed)

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
                bbox = location.total_bounds
                lat_range = bbox[3] - bbox[1]
                lon_range = bbox[2] - bbox[0]
                if lat_range < 5 or lon_range < 5:
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

    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": name, "format": "json", "limit": 1}
    headers = {"User-Agent": "WeatherDataAPI/1.0 (your-email@example.com)"}
    response = requests.get(url, params=params, headers=headers)
    data = response.json()
    if data and "boundingbox" in data[0]:
        bbox = [float(x) for x in data[0]["boundingbox"]]
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
    * Get precipitation rate data with storm indicators
    * Get wave data (height, period, direction) for marine areas
    * Generate visualizations for wind, precipitation, and waves
    * Return data in both tabular and visual formats
    
    ## Data Source
    Uses GFS (Global Forecast System) data from NOAA, providing:
    * 10-meter wind components (U and V)
    * Total Precipitation Rate and convective parameters
    * Wave parameters (significant height, period, direction)
    * 0.25-degree resolution for atmospheric data, 0.16-degree for waves
    * Updated every 6 hours
    
    ## Usage Example (Wave Data)
    ```python
    import requests
    
    # Define the bounding box (5°x5° box around Caribbean Sea)
    data = {
        "min_lat": 10,
        "max_lat": 15,
        "min_lon": -65,
        "max_lon": -60
    }
    
    # Make the API request
    response = requests.post("http://localhost:8000/wave-data", json=data)
    result = response.json()
    
    # Access the data
    print(f"Valid Time: {result['valid_time']}")
    print(f"Number of data points: {len(result['data_points'])}")
    
    # Save the wave map image
    import base64
    with open("wave_map.png", "wb") as f:
        f.write(base64.b64decode(result['image_base64']))
    ```
    """,
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize separate weather services for wind and waves
weather_service = WeatherService()


@app.on_event("startup")
async def startup_event():
    print("GFS polling started in background thread")

@app.on_event("shutdown")
async def shutdown_event():
    pass

@app.post("/wind-data", response_model=WindDataResponse)
async def get_wind_data(request: BoundingBox):
    try:
        if not weather_service.is_ready():
            raise HTTPException(status_code=503, detail="GRIB files not yet available. Please try again in a few minutes.")
            
        if request.name:
            try:
                bbox = get_bbox_by_name(request.name)
                min_lat, max_lat, min_lon, max_lon = (
                    bbox["min_lat"], bbox["max_lat"], bbox["min_lon"], bbox["max_lon"]
                )
            except ValueError as e:
                raise HTTPException(status_code=404, detail=str(e))
        else:
            min_lat, max_lat, min_lon, max_lon = (
                request.min_lat, request.max_lat, request.min_lon, request.max_lon
            )
            
        data_points, image_base64, valid_time, grib_file, description = weather_service.process_wind_data(
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

@app.post("/marine-hazards", response_model=MarineHazardsResponse)
async def get_marine_hazards(request: BoundingBox):
    try:
        if not weather_service.is_ready():
            raise HTTPException(status_code=503, detail="GRIB files not yet available. Please try again in a few minutes.")
            
        if request.name:
            try:
                bbox = get_bbox_by_name(request.name)
                min_lat, max_lat, min_lon, max_lon = (
                    bbox["min_lat"], bbox["max_lat"], bbox["min_lon"], bbox["max_lon"]
                )
            except ValueError as e:
                raise HTTPException(status_code=404, detail=str(e))
        else:
            min_lat, max_lat, min_lon, max_lon = (
                request.min_lat, request.max_lat, request.min_lon, request.max_lon
            )
            
        data_points, image_base64, valid_time, grib_file, storm_indicators, description = weather_service.process_marine_hazards(
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

@app.post("/wave-data", 
    response_model=WaveDataResponse,
    summary="Get wave data and visualization",
    description="""
    Retrieve wave data and generate a visualization for a specified geographical region.
    You can specify the region either by coordinates or by name (e.g., 'Caribbean Sea').
    
    The endpoint returns:
    * Wave data points for each grid location (height, period, direction)
    * A base64-encoded PNG image of the wave map with direction arrows
    * The valid time of the data
    * Information about the GRIB file used
    
    The wave map includes:
    * Color-coded significant wave height visualization
    * Arrows showing primary wave direction
    * Coastlines for geographical context
    * A colorbar indicating wave height values
    
    Parameters:
    * unit: Specify the unit for wave height ('meters' or 'feet', default: 'meters')
    """,
    responses={
        200: {
            "description": "Successfully retrieved wave data and generated visualization",
            "content": {
                "application/json": {
                    "example": {
                        "valid_time": "2024-03-22T12:00:00",
                        "data_points": [
                            {
                                "latitude": 10.0,
                                "longitude": -65.0,
                                "wave_height": 1.5,
                                "wave_period_s": 8.2,
                                "wave_direction_deg": 45.0
                            }
                        ],
                        "image_base64": "base64_encoded_png_image",
                        "grib_file": {
                            "path": "gfswave.t12z.global.0p16.f000.grib2",
                            "download_time": "2024-03-22T12:30:00",
                            "metadata": {
                                "cycle": "t12z",
                                "resolution": "0p16",
                                "forecast_hour": "f000"
                            }
                        }
                    }
                }
            }
        },
        404: {
            "description": "Location not found",
            "content": {"application/json": {"example": {"detail": "Location 'Unknown Sea' not found"}}}
        },
        503: {
            "description": "Service temporarily unavailable - GRIB files not yet available",
            "content": {"application/json": {"example": {"detail": "GRIB files not yet available"}}}
        },
        500: {
            "description": "Error processing the request",
            "content": {"application/json": {"example": {"detail": "Error opening GRIB file"}}}
        }
    }
)
async def get_wave_data(request: BoundingBox):
    """
    Get wave data and visualization for a specified region.
    
    Args:
        request: Either coordinates (min_lat, max_lat, min_lon, max_lon) or a location name,
                plus optional unit parameter ('meters' or 'feet')
        
    Returns:
        WaveDataResponse containing:
        - valid_time: The valid time of the data
        - data_points: List of wave data points with latitude, longitude, height, period, and direction
        - image_base64: Base64 encoded PNG image of the wave map
        - grib_file: Information about the GRIB file used
        - description: Text description of current conditions (optional)
        
    Raises:
        HTTPException: If the location is not found or there's an error processing the request
    """
    try:
        if not weather_service.is_ready():
            raise HTTPException(status_code=503, detail="Wave GRIB files not yet available. Please try again in a few minutes.")
            
        if request.name:
            try:
                bbox = get_bbox_by_name(request.name)
                min_lat, max_lat, min_lon, max_lon = (
                    bbox["min_lat"], bbox["max_lat"], bbox["min_lon"], bbox["max_lon"]
                )
            except ValueError as e:
                raise HTTPException(status_code=404, detail=str(e))
        else:
            min_lat, max_lat, min_lon, max_lon = (
                request.min_lat, request.max_lat, request.min_lon, request.max_lon
            )
            
        data_points, image_base64, valid_time, grib_file, description = weather_service.process_wave_data(
            min_lat, max_lat, min_lon, max_lon, unit=request.unit
        )
        
        return WaveDataResponse(
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

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "weather_service_ready": weather_service.is_ready(),
    }