from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app.models.schemas import BoundingBox, WindDataResponse, WindDataPoint, GribFileInfo
from app.services.wind_service import WindService
from app.tools.polling import poll_gfs_data, stop_polling, set_download_all_files, start_polling
from typing import List
from datetime import datetime
import json

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
async def get_wind_data(bounding_box: BoundingBox):
    """
    Get wind data and visualization for a given bounding box.
    
    Args:
        bounding_box: The bounding box coordinates defining the region of interest
        
    Returns:
        WindDataResponse containing:
        - valid_time: The valid time of the data
        - data_points: List of wind data points with latitude, longitude, and wind speed
        - image_base64: Base64 encoded PNG image of the wind map
        - grib_file: Information about the GRIB file used
        
    Raises:
        HTTPException: If there's an error processing the request
    """
    try:
        if not wind_service.is_ready():
            raise HTTPException(
                status_code=503,
                detail="GRIB files not yet available. Please try again in a few minutes."
            )
            
        data_points, image_base64, valid_time, grib_info = wind_service.process_wind_data(
            bounding_box.min_lat,
            bounding_box.max_lat,
            bounding_box.min_lon,
            bounding_box.max_lon
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
