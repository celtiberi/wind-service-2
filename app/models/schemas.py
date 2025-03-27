from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any, Union
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

class MarineHazardsResponse(BaseModel):
    data_points: List[DataPoint]
    image_base64: str
    valid_time: datetime
    grib_info: GribFile
    storm_indicators: Dict
    description: str  