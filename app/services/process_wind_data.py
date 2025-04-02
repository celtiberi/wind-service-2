# app/services/process_wind_data.py
import pygrib
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import base64
from io import BytesIO
from typing import Tuple, List, Dict, Optional
from datetime import datetime
from .process_weather_data import ProcessWeatherData, logger
from app.models.schemas import GribFile

class ProcessWindData(ProcessWeatherData):
    def process_data(self, min_lat: float, max_lat: float, min_lon: float, max_lon: float) -> Tuple[List[dict], str, datetime, GribFile, Optional[Dict]]:
        logger.info(f"Processing wind data for bounding box: ({min_lat}, {max_lat}, {min_lon}, {max_lon})")
        
        if not self._atmos_grib or not self._atmos_grib_file_data:
            raise ValueError("Atmospheric GRIB file not available")

        # Extract U and V wind components
        try:
            u_grb = self._atmos_grib.select(name='10 metre U wind component')[0]
            v_grb = self._atmos_grib.select(name='10 metre V wind component')[0]
            logger.info("Extracted U and V wind components")
        except Exception as e:
            logger.error(f"Error extracting wind components from {self._atmos_grib_file_data.path}: {e}", exc_info=True)
            raise Exception(f"Error extracting wind components from {self._atmos_grib_file_data.path}: {e}")

        # Get full data and coordinates
        try:
            u_data_full, lats_full, lons_full = u_grb.data()
            v_data_full, _, _ = v_grb.data()
            logger.debug(f"Full data shapes: U={u_data_full.shape}, lats={lats_full.shape}, lons={lons_full.shape}")
        except Exception as e:
            logger.error(f"Error extracting data from wind GRIB messages: {e}", exc_info=True)
            raise Exception(f"Error extracting data from wind GRIB messages: {e}")

        # Slice the data to the bounding box
        try:
            u_data, lats, lons = self._slice_data_to_bounding_box(u_data_full, lats_full, lons_full, min_lat, max_lat, min_lon, max_lon)
            v_data, _, _ = self._slice_data_to_bounding_box(v_data_full, lats_full, lons_full, min_lat, max_lat, min_lon, max_lon)
        except Exception as e:
            logger.error(f"Error slicing wind data: {e}", exc_info=True)
            raise

        # Calculate wind speed at each grid point (in m/s)
        try:
            wind_speed_ms = np.sqrt(u_data**2 + v_data**2)
            wind_speed_knots = wind_speed_ms * 1.94384
            logger.debug(f"Wind speed (knots) range: {wind_speed_knots.min()} to {wind_speed_knots.max()}")
        except Exception as e:
            logger.error(f"Error calculating wind speed: {e}", exc_info=True)
            raise Exception(f"Error calculating wind speed: {e}")

        # Convert U and V components to knots for barbs
        u_knots = u_data * 1.94384
        v_knots = v_data * 1.94384

        # Create data points list
        data_points = []
        try:
            for i in range(wind_speed_knots.shape[0]):
                for j in range(wind_speed_knots.shape[1]):
                    data_points.append({
                        'latitude': float(lats[i, j]),
                        'longitude': float(lons[i, j]),
                        'wind_speed_knots': float(wind_speed_knots[i, j])
                    })
            logger.info(f"Created {len(data_points)} wind data points")
        except Exception as e:
            logger.error(f"Error creating wind data points: {e}", exc_info=True)
            raise Exception(f"Error creating wind data points: {e}")

        # Generate and encode the plot
        try:
            image_base64 = self._generate_plot(lats, lons, wind_speed_knots, min_lat, max_lat, min_lon, max_lon, 
                                              u_grb.validDate, self._atmos_grib_file_data, u_knots=u_knots, v_knots=v_knots)
            logger.info("Wind plot generated successfully")
        except Exception as e:
            logger.error(f"Error generating wind plot: {e}", exc_info=True)
            raise Exception(f"Error generating wind plot: {e}")

        return data_points, image_base64, u_grb.validDate, self._atmos_grib_file_data, None

    def _generate_plot(self, lats: np.ndarray, lons: np.ndarray, data_field: np.ndarray, 
                      min_lat: float, max_lat: float, min_lon: float, max_lon: float, 
                      valid_time: datetime, grib_file: GribFile, **kwargs) -> str:
        logger.info("Generating wind plot")
        u_knots = kwargs.get('u_knots')
        v_knots = kwargs.get('v_knots')

        # Create figure and axis with projection
        try:
            fig = plt.figure(figsize=(12, 8))
            ax = plt.axes(projection=ccrs.PlateCarree())
            logger.debug("Created figure and axis with PlateCarree projection")
        except Exception as e:
            logger.error(f"Error creating figure and axis: {e}", exc_info=True)
            raise Exception(f"Error creating figure and axis: {e}")

        # Add geographical features
        try:
            ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
            ax.add_feature(cfeature.BORDERS, linewidth=0.3, linestyle='--')
            ax.add_feature(cfeature.LAND, facecolor='lightgray', alpha=0.3)
            logger.debug("Added geographical features to plot")
        except Exception as e:
            logger.error(f"Error adding geographical features: {e}", exc_info=True)
            raise Exception(f"Error adding geographical features: {e}")

        # Add gridlines with labels
        try:
            gl = ax.gridlines(draw_labels=True, linestyle='--', color='gray', alpha=0.5)
            gl.top_labels = False
            gl.right_labels = False
            logger.debug("Added gridlines with labels")
        except Exception as e:
            logger.error(f"Error adding gridlines: {e}", exc_info=True)
            raise Exception(f"Error adding gridlines: {e}")

        # Create custom colormap for wind (blue -> green -> yellow -> red)
        try:
            N = 256
            vals = np.ones((N, 4))
            n1 = N // 4  # 0-10 knots
            n2 = N // 2  # 10-20 knots
            n3 = 4 * N // 4  # 20-30 knots
            vals[:n1, 0] = 0
            vals[:n1, 1] = np.linspace(0, 1, n1)
            vals[:n1, 2] = np.linspace(1, 0, n1)
            vals[n1:n2, 0] = np.linspace(0, 1, n2-n1)
            vals[n1:n2, 1] = 1
            vals[n1:n2, 2] = 0
            vals[n2:n3, 0] = 1
            vals[n2:n3, 1] = np.linspace(1, 0, n3-n2)
            vals[n2:n3, 2] = 0
            vals[n3:, 0] = 1
            vals[n3:, 1] = 0
            vals[n3:, 2] = 0
            cmap = plt.cm.colors.ListedColormap(vals)
            vmin, vmax = 0, 40
            label = 'Wind Speed (knots)'
            title = (f'Wind Speed and Direction\n'
                     f'GFS {grib_file.metadata.cycle}, Resolution: {grib_file.metadata.resolution}, Valid: {valid_time.strftime("%Y-%m-%d %H:%M UTC")}\n'
                     f'Downloaded: {grib_file.download_time}')
            logger.debug("Set up wind colormap and parameters")
        except Exception as e:
            logger.error(f"Error setting up wind colormap: {e}", exc_info=True)
            raise Exception(f"Error setting up wind colormap: {e}")

        # Plot the data field
        try:
            norm = plt.cm.colors.Normalize(vmin=vmin, vmax=vmax)
            cs = ax.pcolormesh(lons, lats, data_field, 
                              transform=ccrs.PlateCarree(),
                              cmap=cmap, norm=norm)
            logger.debug("Plotted wind data field with pcolormesh")
        except Exception as e:
            logger.error(f"Error plotting wind data field with pcolormesh: {e}", exc_info=True)
            raise Exception(f"Error plotting wind data field with pcolormesh: {e}")

        # Add colorbar
        try:
            cbar = plt.colorbar(cs, ax=ax, orientation='horizontal', pad=0.05)
            cbar.set_label(label, fontsize=12)
            logger.debug("Added colorbar")
        except Exception as e:
            logger.error(f"Error adding colorbar: {e}", exc_info=True)
            raise Exception(f"Error adding colorbar: {e}")

        # Calculate grid for wind barbs
        try:
            target_barbs = 20
            rows, cols = lats.shape
            stride_lat = max(1, rows // target_barbs)
            stride_lon = max(1, cols // target_barbs)
            barb_lats = []
            barb_lons = []
            barb_u = []
            barb_v = []
            for i in range(0, rows-stride_lat, stride_lat):
                for j in range(0, cols-stride_lon, stride_lon):
                    lat_block = lats[i:i+stride_lat, j:j+stride_lon]
                    lon_block = lons[i:i+stride_lat, j:j+stride_lon]
                    u_block = u_knots[i:i+stride_lat, j:j+stride_lon]
                    v_block = v_knots[i:i+stride_lat, j:j+stride_lon]
                    barb_lats.append(np.mean(lat_block))
                    barb_lons.append(np.mean(lon_block))
                    barb_u.append(np.mean(u_block))
                    barb_v.append(np.mean(v_block))
            barb_lats = np.array(barb_lats)
            barb_lons = np.array(barb_lons)
            barb_u = np.array(barb_u)
            barb_v = np.array(barb_v)
            logger.debug(f"Computed wind barbs: {len(barb_lats)} points")
        except Exception as e:
            logger.error(f"Error computing wind barbs: {e}", exc_info=True)
            raise Exception(f"Error computing wind barbs: {e}")

        # Add wind barbs
        try:
            ax.barbs(barb_lons, barb_lats, barb_u, barb_v,
                    transform=ccrs.PlateCarree(),
                    length=5, sizes=dict(emptybarb=0.15, spacing=0.15, width=0.3))
            logger.debug("Added wind barbs to plot")
        except Exception as e:
            logger.error(f"Error adding wind barbs: {e}", exc_info=True)
            raise Exception(f"Error adding wind barbs: {e}")

        # Set map extent
        try:
            ax.set_extent([min_lon, max_lon, min_lat, max_lat], crs=ccrs.PlateCarree())
            logger.debug(f"Set map extent: ({min_lon}, {max_lon}, {min_lat}, {max_lat})")
        except Exception as e:
            logger.error(f"Error setting map extent: {e}", exc_info=True)
            raise Exception(f"Error setting map extent: {e}")

        # Add title
        try:
            plt.title(title, pad=20, fontsize=14)
            logger.debug("Added plot title")
        except Exception as e:
            logger.error(f"Error adding title: {e}", exc_info=True)
            raise Exception(f"Error adding title: {e}")

        # Save plot to bytes buffer
        try:
            buf = BytesIO()
            plt.savefig(buf, format='png', bbox_inches='tight', dpi=150, 
                       pil_kwargs={'optimize': True, 'quality': 85})
            plt.close()
            buf.seek(0)
            image_base64 = base64.b64encode(buf.getvalue()).decode()
            logger.info("Wind plot saved and encoded to base64")
            return image_base64
        except Exception as e:
            logger.error(f"Error saving wind plot to buffer: {e}", exc_info=True)
            raise Exception(f"Error saving wind plot to buffer: {e}")