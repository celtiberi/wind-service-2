# app/services/weather_service.py
from typing import Tuple, List, Dict, Optional
from datetime import datetime
from app.models.schemas import GribFile, WaveDataResponse, WindDataResponse, BoundingBox, MarineHazardsResponse
from .process_wind_data import ProcessWindData
from .process_wave_data import ProcessWaveData
from .process_marine_hazards import ProcessMarineHazards
import os
from app.tools.polling import start_polling, gribs_updated_event

class WeatherService:
    def __init__(self):
        start_polling()
        
        # Initialize the data processors
        self._wind_processor = ProcessWindData()
        self._wave_processor = ProcessWaveData()
        self._marine_hazards_processor = ProcessMarineHazards()

    def is_ready(self) -> bool:
        """Check if GRIB files are available and ready for processing"""
        return self._wind_processor.is_ready()

    def process_wind_data(self, bbox: BoundingBox) -> WindDataResponse:
        """
        Process wind data for the specified region.
        
        Args:
            bbox: BoundingBox object containing the region coordinates
        
        Returns:
            WindDataResponse containing:
            - List of data points with latitude, longitude, and wind speed
            - Base64 encoded PNG image
            - Valid time of the data
            - GRIB file information
            - Text description of current conditions
        """
        return self._wind_processor.process_data(bbox)

    def process_wave_data(self, bbox: BoundingBox, unit: str = "meters") -> WaveDataResponse:
        """
        Process wave data for the specified region.
        
        Args:
            bbox: BoundingBox object containing the region coordinates
            unit: Unit for wave height ('meters' or 'feet', default: 'meters')
        
        Returns:
            WaveDataResponse containing:
            - List of data points with latitude, longitude, wave height, period, and direction
            - Base64 encoded PNG image
            - Valid time of the data
            - GRIB file information
            - Text description of current conditions
        """
        return self._wave_processor.process_data(bbox, unit=unit)

    def process_marine_hazards(self, bbox: BoundingBox) -> MarineHazardsResponse:
        """
        Process marine hazards data for a specified region.
        
        Args:
            bbox: BoundingBox object containing region coordinates
            
        Returns:
            MarineHazardsResponse containing:
            - valid_time: The valid time of the data
            - data_points: List of marine hazards data points
            - image_base64: Base64 encoded PNG image of the hazards map
            - grib_file: Information about the GRIB file used
            - storm_indicators: List of storm indicators
            - description: Text description of current hazards
        """
        data_points, image_base64, valid_time, grib_file, hazard_indicators, description = self._marine_hazards_processor.process_data(bbox)
        
        return MarineHazardsResponse(
            valid_time=valid_time,
            data_points=data_points,
            image_base64=image_base64,
            grib_file=grib_file,
            storm_indicators=hazard_indicators,
            description=description
        )