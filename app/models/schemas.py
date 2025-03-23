from pydantic import BaseModel, Field
from typing import List
from datetime import datetime

class BoundingBox(BaseModel):
    min_lat: float = Field(..., ge=-90, le=90, description="Minimum latitude")
    max_lat: float = Field(..., ge=-90, le=90, description="Maximum latitude")
    min_lon: float = Field(..., ge=-180, le=180, description="Minimum longitude")
    max_lon: float = Field(..., ge=-180, le=180, description="Maximum longitude")

class WindDataPoint(BaseModel):
    latitude: float
    longitude: float
    wind_speed_knots: float

class GribFileInfo(BaseModel):
    filename: str
    cycle_time: str  # e.g., "t12z"
    download_time: datetime
    forecast_hour: int  # e.g., 0 for f000

class WindDataResponse(BaseModel):
    valid_time: datetime
    data_points: List[WindDataPoint]
    image_base64: str
    grib_file: GribFileInfo 