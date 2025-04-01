# app/services/weather_service.py
from typing import Tuple, List, Dict, Optional
from datetime import datetime
from app.models.schemas import GribFile
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

    def process_wind_data(self, min_lat: float, max_lat: float, min_lon: float, max_lon: float) -> Tuple[List[Dict[str, float]], str, datetime, GribFile, str]:
        """
        Process wind data for the specified region.
        
        Returns:
            Tuple containing:
            - List of data points with latitude, longitude, and wind speed
            - Base64 encoded PNG image
            - Valid time of the data
            - GRIB file information
            - Text description of current conditions
        """
        return self._wind_processor.process_data(min_lat, max_lat, min_lon, max_lon)

    def process_wave_data(self, min_lat: float, max_lat: float, min_lon: float, max_lon: float, unit: str = "meters") -> Tuple[List[Dict[str, float]], str, datetime, GribFile, str]:
        """
        Process wave data for the specified region.
        
        Args:
            min_lat: Minimum latitude
            max_lat: Maximum latitude
            min_lon: Minimum longitude
            max_lon: Maximum longitude
            unit: Unit for wave height ('meters' or 'feet', default: 'meters')
        
        Returns:
            Tuple containing:
            - List of data points with latitude, longitude, and wave height
            - Base64 encoded PNG image
            - Valid time of the data
            - GRIB file information
            - Text description of current conditions
        """
        return self._wave_processor.process_data(min_lat, max_lat, min_lon, max_lon, unit=unit)

    def process_marine_hazards(self, min_lat: float, max_lat: float, min_lon: float, max_lon: float) -> Tuple[List[Dict[str, float]], str, datetime, GribFile, Optional[Dict], str]:
        """
        Process marine hazards data for the specified region.
        
        Returns:
            Tuple containing:
            - List of data points with latitude, longitude, and precipitation rate
            - Base64 encoded PNG image
            - Valid time of the data
            - GRIB file information
            - Optional dictionary of storm indicators
            - Text description of hazards
        """
        return self._marine_hazards_processor.process_data(min_lat, max_lat, min_lon, max_lon)