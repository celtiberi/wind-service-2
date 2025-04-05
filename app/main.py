from functools import lru_cache
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app.models.schemas import (
    BoundingBox, WindDataResponse, WaveDataResponse, MarineHazardsResponse, LocationRequest, MarineForecastResponse    
)
from app.services.weather_service import WeatherService
from app.services.process_weather_data import logger
from app.services.noaa_marine_forecast import NOAAMarineForecast
from typing import List, Dict, Optional
from datetime import datetime
import json
import geopandas as gpd
from pathlib import Path
import time
import requests
from pydantic import BaseModel
from app.utils.bbox import get_bounding_box
import logging


app = FastAPI(
    title="Weather Data API",
    description="""
    This API provides weather data and visualizations for specified geographical regions using GFS (Global Forecast System) data.
    
    ## Features
    * Get wind speed data for any geographical region
    * Get precipitation rate data with storm indicators
    * Get wave data (height, period, direction) for marine areas
    * Get marine forecasts for specific locations or areas
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
marine_forecast_service = NOAAMarineForecast()


@app.on_event("startup")
async def startup_event():
    print("GFS polling started in background thread")

@app.on_event("shutdown")
async def shutdown_event():
    pass

@app.get("/")
async def root():
    """Root endpoint that returns API information"""
    return {
        "name": "Weather Data API",
        "version": "1.0.0",
        "description": "API for retrieving weather data including wind, waves, and marine hazards",
        "endpoints": [
            {
                "path": "/wind-data",
                "method": "POST",
                "description": "Get wind data and visualization for a specified region"
            },
            {
                "path": "/wave-data",
                "method": "POST",
                "description": "Get wave data and visualization for a specified region"
            },
            {
                "path": "/marine-hazards",
                "method": "POST",
                "description": "Get marine hazards data and visualization for a specified region"
            }
        ]
    }

@app.post("/wind-data", 
    response_model=WindDataResponse,
    summary="Get wind data and visualization",
    description="""
    Retrieve wind data and generate a visualization for a specified geographical region.
    You can specify the region either by coordinates or by name (e.g., 'Caribbean Sea').
    
    The endpoint returns:
    * Wind data points for each grid location
    * A base64-encoded PNG image of the wind map
    * The valid time of the data
    * Information about the GRIB file used
    * A text description of current wind conditions
    
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
                        },
                        "description": "Wind conditions description"
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
async def get_wind_data(request: LocationRequest):
    """
    Get wind data and visualization for a specified region.
    
    Args:
        request: Either coordinates (min_lat, max_lat, min_lon, max_lon) or a location name
        
    Returns:
        WindDataResponse containing:
        - valid_time: The valid time of the data
        - data_points: List of wind data points with latitude, longitude, and speed
        - image_base64: Base64 encoded PNG image of the wind map
        - grib_file: Information about the GRIB file used
        - description: Text description of current wind conditions
        
    Raises:
        HTTPException: If the location is not found or there's an error processing the request
    """
    try:
        if not weather_service.is_ready():
            raise HTTPException(status_code=503, detail="Wind GRIB files not yet available. Please try again in a few minutes.")
            
        bbox = get_bounding_box(request)
        
        return weather_service.process_wind_data(bbox)
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
    * A text description of current wave conditions
    
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
                        },
                        "description": "Wave conditions in the region show moderate waves with significant wave heights ranging from 1.2 to 2.5 feet (average 1.8 feet). Waves are moving NE with an average period of 8.2 seconds."
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
async def get_wave_data(request: LocationRequest):
    """
    Get wave data and visualization for a specified region.
    
    Args:
        request: LocationRequest object containing either:
            - name: Name of a region (e.g., 'Caribbean Sea')
            - lat/lon: Point coordinates
            - min_lat/max_lat/min_lon/max_lon: Explicit bounding box coordinates
            - unit: Optional unit for wave height ('meters' or 'feet', default: 'meters')
        
    Returns:
        WaveDataResponse containing:
        - valid_time: The valid time of the data
        - data_points: List of wave data points with latitude, longitude, height, period, and direction
        - image_base64: Base64 encoded PNG image of the wave map
        - grib_file: Information about the GRIB file used
        - description: Text description of current wave conditions
        
    Raises:
        HTTPException: If the location is not found or there's an error processing the request
    """
    try:
        if not weather_service.is_ready():
            raise HTTPException(status_code=503, detail="Wave GRIB files not yet available. Please try again in a few minutes.")
        
        bbox = get_bounding_box(request)
            
        return weather_service.process_wave_data(bbox, unit=request.unit)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/marine-hazards", 
    response_model=MarineHazardsResponse,
    summary="Get marine hazards data and visualization",
    description="""
    Retrieve marine hazards data and generate a visualization for a specified geographical region.
    You can specify the region either by coordinates or by name (e.g., 'Caribbean Sea').
    
    The endpoint returns:
    * Marine hazards data points for each grid location
    * A base64-encoded PNG image of the hazards map
    * The valid time of the data
    * Information about the GRIB file used
    * A text description of current hazards
    
    The hazards map includes:
    * Color-coded visualization of different hazards
    * Storm indicators and risk areas
    * Coastlines for geographical context
    * A legend indicating hazard types
    """,
    responses={
        200: {
            "description": "Successfully retrieved marine hazards data and generated visualization",
            "content": {
                "application/json": {
                    "example": {
                        "valid_time": "2024-03-22T12:00:00",
                        "data_points": [...],
                        "image_base64": "base64_encoded_png_image",
                        "grib_file": {...},
                        "storm_indicators": [...],
                        "description": "Marine hazards description"
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
async def get_marine_hazards(request: LocationRequest):
    """
    Get marine hazards data and visualization for a specified region.
    
    Args:
        request: LocationRequest object containing either:
            - name: Name of a region (e.g., 'Caribbean Sea')
            - lat/lon: Point coordinates
            - min_lat/max_lat/min_lon/max_lon: Explicit bounding box coordinates
        
    Returns:
        MarineHazardsResponse containing:
        - valid_time: The valid time of the data
        - data_points: List of marine hazards data points
        - image_base64: Base64 encoded PNG image of the hazards map
        - grib_file: Information about the GRIB file used
        - storm_indicators: List of storm indicators
        - description: Text description of current hazards
        
    Raises:
        HTTPException: If the location is not found or there's an error processing the request
    """
    try:
        if not weather_service.is_ready():
            raise HTTPException(status_code=503, detail="Marine hazards GRIB files not yet available. Please try again in a few minutes.")
            
        bbox = get_bounding_box(request)
        
        return weather_service.process_marine_hazards(bbox)
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



@app.post("/marine-forecast", response_model=MarineForecastResponse)
async def get_marine_forecast(request: LocationRequest):
    """
    Get marine forecast for a specific location or area.
    
    You can specify the location either by:
    - Name (e.g., 'Caribbean Sea')
    - Point coordinates (lat, lon)
    - Bounding box coordinates (min_lat, max_lat, min_lon, max_lon)
    
    Returns:
        MarineForecastResponse containing:
        - forecast: The marine forecast text (or an error message)
        - zone_id: The zone ID where the forecast is from (if found)
        - lat: The latitude used to find the forecast (if applicable)
        - lon: The longitude used to find the forecast (if applicable)
    """
    try:
        if request.name:
            # Use the utility function to get BoundingBox object first
            bbox_obj = get_bounding_box(request)
            result = marine_forecast_service.get_forecast(bbox=(
                bbox_obj.min_lon, bbox_obj.min_lat, bbox_obj.max_lon, bbox_obj.max_lat
            ))
        elif request.lat is not None and request.lon is not None:
            result = marine_forecast_service.get_forecast(lat=request.lat, lon=request.lon)
        elif all(v is not None for v in [request.min_lat, request.max_lat, request.min_lon, request.max_lon]):
            result = marine_forecast_service.get_forecast(bbox=(
                request.min_lon, request.min_lat, request.max_lon, request.max_lat
            ))
        else:
            # Return the error directly in the response model format
            return MarineForecastResponse(
                forecast="Must provide either name, lat/lon coordinates, or bounding box coordinates"
            )
        
        # The service now handles errors internally and returns them in the forecast field
        return result
    except ValueError as e:
        # Handle errors from get_bounding_box (e.g., location not found)
        return MarineForecastResponse(forecast=str(e))
    except Exception as e:
        # Catch unexpected errors
        print(f"Unexpected error in /marine-forecast: {e}") # Log the error server-side
        return MarineForecastResponse(forecast=f"An unexpected error occurred: {e}")