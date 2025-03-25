from pydantic import BaseModel, Field, validator
from typing import List, Optional
from datetime import datetime

class LocationRequest(BaseModel):
    name: str = Field(..., description="Name of the location (e.g., 'Caribbean Sea', 'Mediterranean Sea')")

class BoundingBox(BaseModel):
    name: Optional[str] = Field(None, description="Name of the location (e.g., 'Caribbean Sea', 'Mediterranean Sea')")
    min_lat: Optional[float] = Field(None, ge=-90, le=90, description="Minimum latitude")
    max_lat: Optional[float] = Field(None, ge=-90, le=90, description="Maximum latitude")
    min_lon: Optional[float] = Field(None, ge=-180, le=180, description="Minimum longitude")
    max_lon: Optional[float] = Field(None, ge=-180, le=180, description="Maximum longitude")

    @validator('min_lat', 'max_lat', 'min_lon', 'max_lon')
    def validate_coordinates(cls, v, values):
        if 'name' not in values or not values['name']:
            if v is None:
                raise ValueError("Coordinates are required when name is not provided")
        return v

    @validator('name')
    def validate_name(cls, v, values):
        if v is None and all(values.get(field) is None for field in ['min_lat', 'max_lat', 'min_lon', 'max_lon']):
            raise ValueError("Either name or coordinates must be provided")
        return v

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