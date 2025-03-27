# app/services/weather_service.py
from .process_wind_data import ProcessWindData
from .process_marine_hazards import ProcessMarineHazards
import os
from app.tools.polling import start_polling

class WeatherService:
    def __init__(self):
        start_polling()
        
        # Initialize the data processors
        self.wind_processor = ProcessWindData()
        self.marine_hazards_processor = ProcessMarineHazards()

    def is_ready(self) -> bool:
        """Check if both processors are ready"""
        return self.wind_processor.is_ready() and self.marine_hazards_processor.is_ready()

    def process_wind_data(self, min_lat: float, max_lat: float, min_lon: float, max_lon: float):
        """Process wind data for the specified bounding box"""
        return self.wind_processor.process_data(min_lat, max_lat, min_lon, max_lon)

    def process_marine_hazards(self, min_lat: float, max_lat: float, min_lon: float, max_lon: float):
        """Process marine hazards for the specified bounding box"""
        return self.marine_hazards_processor.process_data(min_lat, max_lat, min_lon, max_lon)