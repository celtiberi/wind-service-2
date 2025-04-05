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
from app.models.schemas import GribFile, WaveDataResponse, WaveDataPoint, BoundingBox

class ProcessWaveData(ProcessWeatherData):
    def process_data(self, bbox: BoundingBox, unit: str = "meters") -> WaveDataResponse:
        logger.info(f"Processing wave data for bounding box: {bbox} with unit: {unit}")
        
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
            height_data, lats, lons = self._slice_data_to_bounding_box(height_data_full, lats_full, lons_full, bbox.min_lat, bbox.max_lat, bbox.min_lon, bbox.max_lon)
            period_data, _, _ = self._slice_data_to_bounding_box(period_data_full, lats_full, lons_full, bbox.min_lat, bbox.max_lat, bbox.min_lon, bbox.max_lon)
            dir_data, _, _ = self._slice_data_to_bounding_box(dir_data_full, lats_full, lons_full, bbox.min_lat, bbox.max_lat, bbox.min_lon, bbox.max_lon)
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
                        data_points.append(WaveDataPoint(
                            latitude=float(lats[i, j]),
                            longitude=float(lons[i, j]),
                            wave_height=height,  # Already in the requested unit (feet or meters)
                            wave_period_s=period,
                            wave_direction_deg=direction
                        ))
            logger.info(f"Created {len(data_points)} valid wave data points")
        except Exception as e:
            logger.error(f"Error creating wave data points: {e}", exc_info=True)
            raise Exception(f"Error creating wave data points: {e}")

        # Generate and encode the plot
        try:
            image_base64 = self._generate_plot(lats, lons, height_data, bbox.min_lat, bbox.max_lat, bbox.min_lon, bbox.max_lon, 
                                              height_grb.validDate, self._wave_grib_file_data, 
                                              dir_data=dir_data, unit=unit, period_data=period_data)
            logger.info("Wave plot generated successfully")
        except Exception as e:
            logger.error(f"Error generating wave plot: {e}", exc_info=True)
            raise Exception(f"Error generating wave plot: {e}")

        # Generate text description
        try:
            max_height = np.nanmax(height_data)
            min_height = np.nanmin(height_data)
            mean_height = np.nanmean(height_data)
            mean_period = np.nanmean(period_data)
            mean_direction = np.nanmean(dir_data)
            
            # Convert direction to cardinal directions
            directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 
                         'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']
            dir_index = round(mean_direction / 22.5) % 16
            cardinal_dir = directions[dir_index]
            
            # Generate description based on wave heights
            if unit == "feet":
                if max_height < 2:
                    height_desc = "calm to light chop"
                elif max_height < 4:
                    height_desc = "moderate waves"
                elif max_height < 8:
                    height_desc = "rough seas"
                else:
                    height_desc = "very rough to high seas"
            else:
                if max_height < 0.6:
                    height_desc = "calm to light chop"
                elif max_height < 1.2:
                    height_desc = "moderate waves"
                elif max_height < 2.4:
                    height_desc = "rough seas"
                else:
                    height_desc = "very rough to high seas"
            
            description = (
                f"Wave conditions in the region show {height_desc} with significant wave heights "
                f"ranging from {min_height:.1f} to {max_height:.1f} {unit} "
                f"(average {mean_height:.1f} {unit}). "
                f"Waves are moving {cardinal_dir} with an average period of {mean_period:.1f} seconds."
            )
            logger.info("Generated wave conditions description")
        except Exception as e:
            logger.error(f"Error generating description: {e}", exc_info=True)
            description = "Unable to generate wave conditions description"

        return WaveDataResponse(
            valid_time=height_grb.validDate,
            data_points=data_points,
            image_base64=image_base64,
            grib_file=self._wave_grib_file_data,
            description=description
        )
    
    def _generate_plot(self, lats: np.ndarray, lons: np.ndarray, data_field: np.ndarray, 
                      min_lat: float, max_lat: float, min_lon: float, max_lon: float, 
                      valid_time: datetime, grib_file: GribFile, **kwargs) -> str:
        logger.info("Generating wave plot")
        dir_data = kwargs.get('dir_data')
        period_data = kwargs.get('period_data') # Get period data
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

        # Calculate cell edges for pcolormesh
        try:
            dlat = np.mean(np.diff(lats[:, 0]))
            dlon = np.mean(np.diff(lons[0, :]))
            lat_edges = np.concatenate([
                [lats[0, 0] - dlat/2], (lats[:-1, 0] + lats[1:, 0])/2, [lats[-1, 0] + dlat/2]
            ])
            lon_edges = np.concatenate([
                [lons[0, 0] - dlon/2], (lons[0, :-1] + lons[0, 1:])/2, [lons[0, -1] + dlon/2]
            ])
            lon_mesh, lat_mesh = np.meshgrid(lon_edges, lat_edges)

            # Plot the wave height field using mesh edges
            norm = plt.cm.colors.Normalize(vmin=vmin, vmax=vmax)
            cs = ax.pcolormesh(lon_mesh, lat_mesh, data_field, 
                              transform=ccrs.PlateCarree(),
                              cmap=cmap, norm=norm)
            logger.debug("Plotted wave height field with pcolormesh using explicit edges")
            
            # Determine extent from the calculated mesh edges
            plot_min_lon = lon_edges.min()
            plot_max_lon = lon_edges.max()
            plot_min_lat = lat_edges.min()
            plot_max_lat = lat_edges.max()

        except Exception as e:
            logger.error(f"Error plotting wave height field: {e}", exc_info=True)
            raise Exception(f"Error plotting wave height field: {e}")

        # Add colorbar
        cbar = plt.colorbar(cs, ax=ax, orientation='horizontal', pad=0.05)
        cbar.set_label(label, fontsize=12)

        # Calculate grid for wave direction arrows (fixed number)
        try:
            target_arrows_per_dim = 15 # Match wind barb density
            rows, cols = lats.shape
            stride_lat = max(1, rows // target_arrows_per_dim)
            stride_lon = max(1, cols // target_arrows_per_dim)
            
            # Add offset for centering
            offset_lat = stride_lat // 2
            offset_lon = stride_lon // 2
            
            # Select subset using strides
            arrow_lats_subset = lats[offset_lat::stride_lat, offset_lon::stride_lon]
            arrow_lons_subset = lons[offset_lat::stride_lat, offset_lon::stride_lon]
            arrow_dir_subset = dir_data[offset_lat::stride_lat, offset_lon::stride_lon]
            arrow_period_subset = period_data[offset_lat::stride_lat, offset_lon::stride_lon]

            # Flatten for plotting
            arrow_lats_flat = arrow_lats_subset.flatten()
            arrow_lons_flat = arrow_lons_subset.flatten()
            arrow_dir_flat = arrow_dir_subset.flatten()
            arrow_period_flat = arrow_period_subset.flatten()
            
            # Calculate dx, dy components for the subset
            arrow_dx = []
            arrow_dy = []
            for k in range(len(arrow_dir_flat)):
                if not np.isnan(arrow_dir_flat[k]) and not np.isnan(arrow_period_flat[k]):
                    dir_rad = np.deg2rad(arrow_dir_flat[k] + 180) # Flip direction
                    scale = arrow_period_flat[k] / 10.0 # Scale by period
                    arrow_dx.append(np.sin(dir_rad) * scale)
                    arrow_dy.append(np.cos(dir_rad) * scale)
                else:
                    # Append NaN if data is missing to maintain array alignment
                    arrow_dx.append(np.nan) 
                    arrow_dy.append(np.nan)
            
            # Filter out NaN coordinates before plotting (quiver doesn't handle them)
            valid_indices = ~np.isnan(arrow_lats_flat) & ~np.isnan(arrow_lons_flat) & ~np.isnan(arrow_dx) & ~np.isnan(arrow_dy)
            plot_lons = arrow_lons_flat[valid_indices]
            plot_lats = arrow_lats_flat[valid_indices]
            plot_dx = np.array(arrow_dx)[valid_indices]
            plot_dy = np.array(arrow_dy)[valid_indices]

            logger.debug(f"Targeting ~{target_arrows_per_dim}x{target_arrows_per_dim} arrows. Strides: lat={stride_lat}, lon={stride_lon}. Number of arrows: {len(plot_lons)}")

            # Add wave direction arrows for the subset
            ax.quiver(plot_lons, plot_lats, plot_dx, plot_dy,
                     transform=ccrs.PlateCarree(),
                     scale=30,  # Adjust base scale for better visibility
                     width=0.002,  # Adjusts arrow width
                     headwidth=5,  # Increase headwidth (default is 3)
                     headlength=5, # Increase headlength (default is 5, but tied to headwidth)
                     color='black',
                     alpha=0.7)
            logger.debug("Added wave direction arrows (fixed number) to plot at original grid points (subset)")
        except Exception as e:
            logger.error(f"Error computing/plotting wave arrows: {e}", exc_info=True)
            # Continue without arrows if there's an error

        # Set map extent based on calculated mesh edges
        try:
            ax.set_extent([plot_min_lon, plot_max_lon, plot_min_lat, plot_max_lat], crs=ccrs.PlateCarree())
            logger.debug(f"Set map extent from calculated mesh edges: ({plot_min_lon}, {plot_max_lon}, {plot_min_lat}, {plot_max_lat})")
        except Exception as e:
            logger.error(f"Error setting map extent: {e}", exc_info=True)
            raise Exception(f"Error setting map extent: {e}")

        # Add title
        plt.title(title, pad=20, fontsize=14)

        # Save plot to bytes buffer (without bbox_inches='tight')
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=150, 
                   pil_kwargs={'optimize': True, 'quality': 85})
        plt.close()
        buf.seek(0)
        image_base64 = base64.b64encode(buf.getvalue()).decode()
        logger.info("Wave plot saved and encoded to base64")
        return image_base64