from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime

class BoundingBox(BaseModel):
    """Model for geographical bounding box coordinates"""
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float

    class Config:
        schema_extra = {
            "example": {
                "min_lat": 9.252,
                "max_lat": 22.328,
                "min_lon": -87.537,
                "max_lon": -66.356
            }
        }

    @field_validator('min_lat', 'max_lat')
    def validate_latitude(cls, v):
        if not -90 <= v <= 90:
            raise ValueError("Latitude must be between -90 and 90 degrees")
        return v

    @field_validator('min_lon', 'max_lon')
    def validate_longitude(cls, v):
        if not -180 <= v <= 180:
            raise ValueError("Longitude must be between -180 and 180 degrees")
        return v

    @field_validator('max_lat')
    def validate_lat_range(cls, v, info):
        if 'min_lat' in info.data and v <= info.data['min_lat']:
            raise ValueError("max_lat must be greater than min_lat")
        return v

    @field_validator('max_lon')
    def validate_lon_range(cls, v, info):
        if 'min_lon' in info.data and v <= info.data['min_lon']:
            raise ValueError("max_lon must be greater than min_lon")
        return v
    
class LocationRequest(BaseModel):
    """Model for location requests that can be specified by name or coordinates"""
    name: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    min_lat: Optional[float] = None
    max_lat: Optional[float] = None
    min_lon: Optional[float] = None
    max_lon: Optional[float] = None
    unit: Optional[str] = "feet"  # For wave data, can be "meters" or "feet"

    @field_validator('unit')
    def validate_unit(cls, v):
        if v not in ["meters", "feet"]:
            raise ValueError("Unit must be 'meters' or 'feet'")
        return v

    @field_validator('lat', 'lon')
    def validate_point_coordinates(cls, v, info):
        if v is not None:
            if info.field_name == 'lat' and not -90 <= v <= 90:
                raise ValueError("Latitude must be between -90 and 90 degrees")
            if info.field_name == 'lon' and not -180 <= v <= 180:
                raise ValueError("Longitude must be between -180 and 180 degrees")
        return v

    @field_validator('min_lat', 'max_lat', 'min_lon', 'max_lon')
    def validate_box_coordinates(cls, v, info):
        if v is not None:
            if info.field_name in ['min_lat', 'max_lat'] and not -90 <= v <= 90:
                raise ValueError("Latitude must be between -90 and 90 degrees")
            if info.field_name in ['min_lon', 'max_lon'] and not -180 <= v <= 180:
                raise ValueError("Longitude must be between -180 and 180 degrees")
        return v

    def to_bounding_box(self) -> BoundingBox:
        """Convert this request to a BoundingBox object"""
        if self.name:
            # This will be handled by the get_bounding_box function
            raise ValueError("Name-based requests should be handled by get_bounding_box function")
        elif self.lat is not None and self.lon is not None:
            # Create a small box around the point
            return BoundingBox(
                min_lat=self.lat - 1,
                max_lat=self.lat + 1,
                min_lon=self.lon - 1,
                max_lon=self.lon + 1
            )
        elif all(v is not None for v in [self.min_lat, self.max_lat, self.min_lon, self.max_lon]):
            return BoundingBox(
                min_lat=self.min_lat,
                max_lat=self.max_lat,
                min_lon=self.min_lon,
                max_lon=self.max_lon
            )
        else:
            raise ValueError("Must provide either name, lat/lon coordinates, or bounding box coordinates")


class PrecipitationDataPoint(BaseModel):
    latitude: float
    longitude: float
    precipitation_rate_mmh: float

class DataPoint(BaseModel):
    latitude: float
    longitude: float
    wind_speed_knots: float
    precipitation_rate_mmh: Optional[float] = None

      

# GRIB-related schemas
class AtmosMetadata(BaseModel):
    cycle: str  # e.g., "t06z"
    resolution: str  # e.g., "0p25"
    forecast_hour: str  # e.g., "f000"

class WaveMetadata(BaseModel):
    cycle: str  # e.g., "t06z"
    resolution: str  # e.g., "0p16"
    domain: str  # e.g., "global"
    forecast_hour: str  # e.g., "f000"

class GribFile(BaseModel):
    path: str
    download_time: str
    metadata: Union[AtmosMetadata, WaveMetadata]

class GribsData(BaseModel):
    atmos: Optional[GribFile] = None
    wave: Optional[GribFile] = None        


class WindDataPoint(BaseModel):
    latitude: float
    longitude: float
    wind_speed_knots: float

class WindDataResponse(BaseModel):
    valid_time: datetime
    data_points: List[WindDataPoint]
    image_base64: str
    grib_file: GribFile
    description: Optional[str] = None

# Pydantic model for wave data point
class WaveDataPoint(BaseModel):
    latitude: float
    longitude: float
    wave_height: float  # Height in the requested unit (meters or feet)
    wave_period_s: float
    wave_direction_deg: float

class WaveDataResponse(BaseModel):
    valid_time: datetime
    data_points: List[WaveDataPoint]
    image_base64: str
    grib_file: GribFile
    description: str
    
class MarineHazardsResponse(BaseModel):
    data_points: List[DataPoint]
    image_base64: str
    valid_time: datetime
    grib_info: GribFile
    storm_indicators: Dict
    description: str  