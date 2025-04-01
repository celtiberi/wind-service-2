# app/services/process_wave_data.py
import pygrib
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import base64
from io import BytesIO
from typing import Tuple, List, Dict, Optional
from datetime import datetime
from .process_weather_data import ProcessWeatherData, logger
from app.models.schemas import GribFile

class ProcessWaveData(ProcessWeatherData):
    def process_data(self, min_lat: float, max_lat: float, min_lon: float, max_lon: float, unit: str = "feet") -> Tuple[List[dict], str, datetime, GribFile, Optional[Dict]]:
        logger.info(f"Processing wave data for bounding box: ({min_lat}, {max_lat}, {min_lon}, {max_lon}) with unit: {unit}")
        
        if not self._wave_grib or not self._wave_grib_file_data:
            raise ValueError("Wave GRIB file not available")

        # Validate unit parameter
        if unit not in ["meters", "feet"]:
            raise ValueError("Unit must be 'meters' or 'feet'")

        # Extract wave parameters
        try:
            height_grb = self._wave_grib.select(name='Significant height of combined wind waves and swell')[0]
            period_grb = self._wave_grib.select(name='Primary wave mean period')[0]
            dir_grb = self._wave_grib.select(name='Primary wave direction')[0]
            logger.info("Extracted wave height, period, and direction components")
        except Exception as e:
            logger.error(f"Error extracting wave components from {self._wave_grib_file_data.path}: {e}", exc_info=True)
            raise Exception(f"Error extracting wave components from {self._wave_grib_file_data.path}: {e}")

        # Get full data and coordinates
        try:
            height_data_full, lats_full, lons_full = height_grb.data()
            period_data_full, _, _ = period_grb.data()
            dir_data_full, _, _ = dir_grb.data()
            logger.debug(f"Full data shapes: height={height_data_full.shape}, lats={lats_full.shape}, lons={lons_full.shape}")
        except Exception as e:
            logger.error(f"Error extracting data from wave GRIB messages: {e}", exc_info=True)
            raise Exception(f"Error extracting data from wave GRIB messages: {e}")

        # Slice the data to the bounding box
        try:
            height_data, lats, lons = self._slice_data_to_bounding_box(height_data_full, lats_full, lons_full, min_lat, max_lat, min_lon, max_lon)
            period_data, _, _ = self._slice_data_to_bounding_box(period_data_full, lats_full, lons_full, min_lat, max_lat, min_lon, max_lon)
            dir_data, _, _ = self._slice_data_to_bounding_box(dir_data_full, lats_full, lons_full, min_lat, max_lat, min_lon, max_lon)
        except Exception as e:
            logger.error(f"Error slicing wave data: {e}", exc_info=True)
            raise

        # Convert wave height to feet if requested (1 meter = 3.28084 feet)
        if unit == "feet":
            height_data = height_data * 3.28084
            height_data_full = height_data_full * 3.28084

        # Create data points list
        data_points = []
        try:
            for i in range(height_data.shape[0]):
                for j in range(height_data.shape[1]):
                    # Check if any of the values are NaN
                    height = float(height_data[i, j])
                    period = float(period_data[i, j])
                    direction = float(dir_data[i, j])
                    
                    # Only add the point if none of the values are NaN
                    if not (np.isnan(height) or np.isnan(period) or np.isnan(direction)):
                        data_points.append({
                            'latitude': float(lats[i, j]),
                            'longitude': float(lons[i, j]),
                            'wave_height': height,  # Already in the requested unit (feet or meters)
                            'wave_period_s': period,
                            'wave_direction_deg': direction
                        })
            logger.info(f"Created {len(data_points)} valid wave data points")
        except Exception as e:
            logger.error(f"Error creating wave data points: {e}", exc_info=True)
            raise Exception(f"Error creating wave data points: {e}")

        # Generate and encode the plot
        try:
            image_base64 = self._generate_plot(lats, lons, height_data, min_lat, max_lat, min_lon, max_lon, 
                                              height_grb.validDate, self._wave_grib_file_data, 
                                              dir_data=dir_data, unit=unit, period_data=period_data)
            logger.info("Wave plot generated successfully")
        except Exception as e:
            logger.error(f"Error generating wave plot: {e}", exc_info=True)
            raise Exception(f"Error generating wave plot: {e}")

        return data_points, image_base64, height_grb.validDate, self._wave_grib_file_data, None
    
    def _generate_plot(self, lats: np.ndarray, lons: np.ndarray, data_field: np.ndarray, 
                      min_lat: float, max_lat: float, min_lon: float, max_lon: float, 
                      valid_time: datetime, grib_file: GribFile, **kwargs) -> str:
        logger.info("Generating wave plot")
        dir_data = kwargs.get('dir_data')
        unit = kwargs.get('unit', 'feet')  # Default to feet if not specified

        # Create figure and axis with projection
        fig = plt.figure(figsize=(12, 8))
        ax = plt.axes(projection=ccrs.PlateCarree())

        # Add geographical features
        ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
        ax.add_feature(cfeature.BORDERS, linewidth=0.3, linestyle='--')
        ax.add_feature(cfeature.LAND, facecolor='lightgray', alpha=0.3)

        # Add gridlines with labels
        gl = ax.gridlines(draw_labels=True, linestyle='--', color='gray', alpha=0.5)
        gl.top_labels = False
        gl.right_labels = False

        # Create smooth colormap: light blue -> dark blue -> purple -> red
        # More gradual transitions by spreading the color changes over a wider range
        if unit == "feet":
            colors = [
                (173/255, 216/255, 255/255),  # Light blue at 0ft
                (0/255, 0/255, 255/255),      # Dark blue at 4ft (instead of 3ft)
                (147/255, 112/255, 219/255),  # Light purple at 8ft (instead of 6ft)
                (255/255, 0/255, 0/255)       # Red at 12ft
            ]
            positions = [0.0, 4/12, 8/12, 1.0]  # Normalized positions for 0-12ft range (0, 4ft, 8ft, 12ft)
            vmin, vmax = 0, 12  # Range for wave heights in feet
            label = 'Significant Wave Height (feet)'
        else:
            # Convert transition points to meters (1 ft = 0.3048 m)
            colors = [
                (173/255, 216/255, 255/255),  # Light blue at 0m
                (0/255, 0/255, 255/255),      # Dark blue at 4ft (1.2192m)
                (147/255, 112/255, 219/255),  # Light purple at 8ft (2.4384m)
                (255/255, 0/255, 0/255)       # Red at 12ft (3.6576m)
            ]
            positions = [0.0, 1.2192/3.6576, 2.4384/3.6576, 1.0]  # Normalized positions for 0-3.6576m range
            vmin, vmax = 0, 3.6576  # Range for wave heights in meters (12ft)
            label = 'Significant Wave Height (meters)'

        cmap = mcolors.LinearSegmentedColormap.from_list('wave_cmap', list(zip(positions, colors)))
        title = (f'Significant Wave Height and Direction\n'
                f'GFS Wave {grib_file.metadata.cycle}, Resolution: {grib_file.metadata.resolution}, '
                f'Valid: {valid_time.strftime("%Y-%m-%d %H:%M UTC")}\n'
                f'Downloaded: {grib_file.download_time}')

        # Plot the wave height field
        norm = plt.cm.colors.Normalize(vmin=vmin, vmax=vmax)
        cs = ax.pcolormesh(lons, lats, data_field, 
                          transform=ccrs.PlateCarree(),
                          cmap=cmap, norm=norm)

        # Add colorbar
        cbar = plt.colorbar(cs, ax=ax, orientation='horizontal', pad=0.05)
        cbar.set_label(label, fontsize=12)

        # Calculate grid for wave direction arrows
        target_arrows = 20  # Number of arrows in each dimension
        rows, cols = lats.shape
        stride_lat = max(1, rows // target_arrows)
        stride_lon = max(1, cols // target_arrows)
        arrow_lats, arrow_lons, arrow_dx, arrow_dy = [], [], [], []
        
        for i in range(0, rows-stride_lat, stride_lat):
            for j in range(0, cols-stride_lon, stride_lon):
                lat_block = lats[i:i+stride_lat, j:j+stride_lon]
                lon_block = lons[i:i+stride_lat, j:j+stride_lon]
                dir_block = dir_data[i:i+stride_lat, j:j+stride_lon]
                
                # Average direction in the block
                mean_dir = np.mean(dir_block)
                dir_rad = np.deg2rad(mean_dir + 180)  # Flip direction by 180°
                
                # Get wave period for this location
                period = kwargs.get('period_data')[i:i+stride_lat, j:j+stride_lon]
                mean_period = np.mean(period)
                
                # Scale arrow length inversely with period
                # Shorter period = shorter arrow
                scale = mean_period / 10.0  # Normalize to max period of ~20s
                
                # Components for arrow direction (waves traveling "toward")
                dx = np.sin(dir_rad) * scale
                dy = np.cos(dir_rad) * scale
                
                arrow_lats.append(np.mean(lat_block))
                arrow_lons.append(np.mean(lon_block))
                arrow_dx.append(dx)
                arrow_dy.append(dy)

        # Add wave direction arrows
        ax.quiver(arrow_lons, arrow_lats, arrow_dx, arrow_dy,
                 transform=ccrs.PlateCarree(),
                 scale=30,  # Adjust base scale for better visibility
                 width=0.002,  # Adjusts arrow width
                 color='black',
                 alpha=0.7)

        # Set map extent
        ax.set_extent([min_lon, max_lon, min_lat, max_lat], crs=ccrs.PlateCarree())

        # Add title
        plt.title(title, pad=20, fontsize=14)

        # Save plot to bytes buffer
        buf = BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', dpi=150, 
                   pil_kwargs={'optimize': True, 'quality': 85})
        plt.close()
        buf.seek(0)
        image_base64 = base64.b64encode(buf.getvalue()).decode()
        logger.info("Wave plot saved and encoded to base64")
        return image_base64