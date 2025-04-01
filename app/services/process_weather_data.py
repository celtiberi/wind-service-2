# app/services/process_weather_data.py
import pygrib
import numpy as np
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Tuple, Dict, Optional
from app.tools.polling import GribFile, GribsData, load_gribs_metadata, gribs_updated_event
import logging
import threading

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('weather_service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ProcessWeatherData(ABC):
    def __init__(self):
        logger.info("Initializing ProcessWeatherData")
        self._wave_grib = None  # File handle for atmospheric GRIB file
        self._wave_grib = None   # File handle for wave GRIB file
        self._atmos_grib_file_data = None  # Metadata for atmospheric GRIB file
        self._wave_grib_file_data = None   # Metadata for wave GRIB file
        self._update_thread = None  # Thread to monitor for updates
        self._reload_grib_files()  # Load initial GRIB files
        self._start_update_monitor()

    def _start_update_monitor(self):
        """Start a thread to monitor for GRIB file updates"""
        def monitor_updates():
            while True:
                gribs_updated_event.wait()
                logger.info("GRIB files updated, reloading...")
                self._reload_grib_files()
                gribs_updated_event.clear()

        self._update_thread = threading.Thread(target=monitor_updates, daemon=True)
        self._update_thread.start()
        logger.info("Started GRIB update monitor thread")

    def _reload_grib_files(self):
        """Reload GRIB files when updates are detected"""
        try:
            # Close existing file handles if open
            if self._wave_grib:
                self._wave_grib.close()
                self._wave_grib = None
            if self._wave_grib:
                self._wave_grib.close()
                self._wave_grib = None

            # Get latest GRIB files
            self._atmos_grib_file_data, self._wave_grib_file_data = self._select_latest_grib_files()
            
            # Open the GRIB files
            if self._atmos_grib_file_data:
                self._wave_grib = pygrib.open(self._atmos_grib_file_data.path)
                logger.info(f"Reloaded atmospheric GRIB file: {self._atmos_grib_file_data.path}")
            if self._wave_grib_file_data:
                self._wave_grib = pygrib.open(self._wave_grib_file_data.path)
                logger.info(f"Reloaded wave GRIB file: {self._wave_grib_file_data.path}")
        except Exception as e:
            logger.error(f"Error reloading GRIB files: {e}", exc_info=True)

    def _select_latest_grib_files(self):
        """Select the latest GRIB files"""
        gribs_data = load_gribs_metadata()
        if gribs_data.atmos:
            return gribs_data.atmos, gribs_data.wave
        else:
            logger.error("No atmospheric GRIB files found")
            return None, None

    def is_ready(self) -> bool:
        """Check if required GRIB files are available"""
        return self._wave_grib is not None and self._atmos_grib_file_data is not None

    def _slice_data_to_bounding_box(self, data_full: np.ndarray, lats_full: np.ndarray, lons_full: np.ndarray,
                                   min_lat: float, max_lat: float, min_lon: float, max_lon: float) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Slice the data to the specified bounding box"""
        logger.debug(f"Slicing data to bounding box: ({min_lat}, {max_lat}, {min_lon}, {max_lon})")
        try:
            # Convert longitudes to 0-360° range for GFS
            min_lon_gfs = min_lon + 360 if min_lon < 0 else min_lon
            max_lon_gfs = max_lon + 360 if max_lon < 0 else max_lon
            logger.debug(f"Converted longitudes: ({min_lon}, {max_lon}) to ({min_lon_gfs}, {max_lon_gfs})")

            # Find indices of the bounding box
            lat_indices = np.where((lats_full[:, 0] >= min_lat) & (lats_full[:, 0] <= max_lat))[0]
            lon_indices = np.where((lons_full[0, :] >= min_lon_gfs) & (lons_full[0, :] <= max_lon_gfs))[0]
            if len(lat_indices) == 0 or len(lon_indices) == 0:
                logger.error(f"No data within bounding box: lat_indices={lat_indices}, lon_indices={lon_indices}")
                raise ValueError("No data within the specified bounding box")
            logger.debug(f"Bounding box indices: lat={lat_indices[0]}:{lat_indices[-1]+1}, lon={lon_indices[0]}:{lon_indices[-1]+1}")

            # Slice the arrays
            sliced_data = data_full[lat_indices[0]:lat_indices[-1]+1, lon_indices[0]:lon_indices[-1]+1]
            sliced_lats = lats_full[lat_indices[0]:lat_indices[-1]+1, lon_indices[0]:lon_indices[-1]+1]
            sliced_lons = lons_full[lat_indices[0]:lat_indices[-1]+1, lon_indices[0]:lon_indices[-1]+1]

            # Convert longitudes back to -180 to 180 for plotting
            sliced_lons = np.where(sliced_lons > 180, sliced_lons - 360, sliced_lons)
            logger.debug(f"Sliced data shapes: data={sliced_data.shape}, lats={sliced_lats.shape}, lons={sliced_lons.shape}")
            return sliced_data, sliced_lats, sliced_lons
        except Exception as e:
            logger.error(f"Error slicing data to bounding box: {e}", exc_info=True)
            raise

    @abstractmethod
    def process_data(self, min_lat: float, max_lat: float, min_lon: float, max_lon: float) -> Tuple[List[dict], str, datetime, GribFile, Optional[Dict]]:
        """Process weather data for the specified bounding box"""
        pass

    @abstractmethod
    def _generate_plot(self, lats: np.ndarray, lons: np.ndarray, data_field: np.ndarray, 
                      min_lat: float, max_lat: float, min_lon: float, max_lon: float, 
                      valid_time: datetime, grib_file: GribFile, **kwargs) -> str:
        """Generate a plot for the weather data"""
        pass

    def __del__(self):
        """Cleanup when the object is destroyed"""
        if self._wave_grib:
            self._wave_grib.close()
            logger.debug("Closed atmospheric GRIB file handle")
        if self._wave_grib:
            self._wave_grib.close()
            logger.debug("Closed wave GRIB file handle")