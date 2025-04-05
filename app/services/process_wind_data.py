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
from app.models.schemas import GribFile, WindDataResponse, WindDataPoint, BoundingBox

class ProcessWindData(ProcessWeatherData):
    def process_data(self, bbox: BoundingBox) -> WindDataResponse:
        logger.info(f"Processing wind data for bounding box: {bbox}")
        
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
            u_data, lats, lons = self._slice_data_to_bounding_box(u_data_full, lats_full, lons_full, bbox.min_lat, bbox.max_lat, bbox.min_lon, bbox.max_lon)
            v_data, _, _ = self._slice_data_to_bounding_box(v_data_full, lats_full, lons_full, bbox.min_lat, bbox.max_lat, bbox.min_lon, bbox.max_lon)
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
                    data_points.append(WindDataPoint(
                        latitude=float(lats[i, j]),
                        longitude=float(lons[i, j]),
                        wind_speed_knots=float(wind_speed_knots[i, j])
                    ))
            logger.info(f"Created {len(data_points)} wind data points")
        except Exception as e:
            logger.error(f"Error creating wind data points: {e}", exc_info=True)
            raise Exception(f"Error creating wind data points: {e}")

        # Generate and encode the plot
        try:
            image_base64 = self._generate_plot(lats, lons, wind_speed_knots, bbox.min_lat, bbox.max_lat, bbox.min_lon, bbox.max_lon, 
                                              u_grb.validDate, self._atmos_grib_file_data, u_knots=u_knots, v_knots=v_knots)
            logger.info("Wind plot generated successfully")
        except Exception as e:
            logger.error(f"Error generating wind plot: {e}", exc_info=True)
            raise Exception(f"Error generating wind plot: {e}")

        # Generate text description
        try:
            max_speed = np.nanmax(wind_speed_knots)
            min_speed = np.nanmin(wind_speed_knots)
            mean_speed = np.nanmean(wind_speed_knots)
            
            # Generate description based on wind speeds
            if max_speed < 10:
                wind_desc = "light winds"
            elif max_speed < 20:
                wind_desc = "moderate winds"
            elif max_speed < 30:
                wind_desc = "strong winds"
            else:
                wind_desc = "very strong winds"
            
            description = (
                f"Wind conditions in the region show {wind_desc} with speeds "
                f"ranging from {min_speed:.1f} to {max_speed:.1f} knots "
                f"(average {mean_speed:.1f} knots)."
            )
            logger.info("Generated wind conditions description")
        except Exception as e:
            logger.error(f"Error generating description: {e}", exc_info=True)
            description = "Unable to generate wind conditions description"

        return WindDataResponse(
            valid_time=u_grb.validDate,
            data_points=data_points,
            image_base64=image_base64,
            grib_file=self._atmos_grib_file_data,
            description=description
        )

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
            # Let aspect ratio be determined by extent and figure size
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

        # Create custom colormap for wind (blue -> green -> yellow -> red -> purple, white above)
        try:
            N = 256
            vals = np.ones((N, 4)) # RGBA
            
            # Define knot ranges for colors (within 0-40 knot vmin/vmax)
            knot_ranges = [(0, 10), (10, 20), (20, 30), (30, 35), (35, 40)]
            colors = [
                (0, 0, 1),    # Blue
                (0, 1, 0),    # Green
                (1, 1, 0),    # Yellow
                (1, 0, 0),    # Red
                (0.5, 0, 0.5) # Purple
            ]
            vmin, vmax = 0, 40

            # Map knot ranges to normalized colormap positions (0.0 to 1.0)
            norm_breaks = [r[0] / vmax for r in knot_ranges] + [knot_ranges[-1][1] / vmax]
            norm_indices = [int(b * (N-1)) for b in norm_breaks]

            # Create smooth transitions between colors
            for i in range(len(colors)):
                start_idx = norm_indices[i]
                end_idx = norm_indices[i+1]
                if i < len(colors) - 1:
                     # Interpolate RGB values between current color and next color
                    r_interp = np.linspace(colors[i][0], colors[i+1][0], end_idx - start_idx)
                    g_interp = np.linspace(colors[i][1], colors[i+1][1], end_idx - start_idx)
                    b_interp = np.linspace(colors[i][2], colors[i+1][2], end_idx - start_idx)
                    vals[start_idx:end_idx, 0] = r_interp
                    vals[start_idx:end_idx, 1] = g_interp
                    vals[start_idx:end_idx, 2] = b_interp
                else:
                    # Last segment uses the last color
                    vals[start_idx:end_idx+1, 0] = colors[i][0]
                    vals[start_idx:end_idx+1, 1] = colors[i][1]
                    vals[start_idx:end_idx+1, 2] = colors[i][2]
            
            cmap = plt.cm.colors.ListedColormap(vals)
            cmap.set_over('white') # Set color for values > vmax

            label = 'Wind Speed (knots)'
            title = (f'Wind Speed and Direction\n'
                     f'GFS {grib_file.metadata.cycle}, Resolution: {grib_file.metadata.resolution}, Valid: {valid_time.strftime("%Y-%m-%d %H:%M UTC")}\n'
                     f'Downloaded: {grib_file.download_time}')
            logger.debug("Set up refined wind colormap (blue-green-yellow-red-purple, white over) and parameters")
        except Exception as e:
            logger.error(f"Error setting up wind colormap: {e}", exc_info=True)
            raise Exception(f"Error setting up wind colormap: {e}")

        # Calculate cell edges for pcolormesh
        try:
            # Calculate the grid spacing
            dlat = np.mean(np.diff(lats[:, 0]))
            dlon = np.mean(np.diff(lons[0, :]))
            
            # Create edge coordinates for the mesh that extend slightly beyond the data points
            lat_edges = np.concatenate([
                [lats[0, 0] - dlat/2],  # Add one edge before first point
                (lats[:-1, 0] + lats[1:, 0])/2,  # Midpoints between data points
                [lats[-1, 0] + dlat/2]  # Add one edge after last point
            ])
            lon_edges = np.concatenate([
                [lons[0, 0] - dlon/2],  # Add one edge before first point
                (lons[0, :-1] + lons[0, 1:])/2,  # Midpoints between data points
                [lons[0, -1] + dlon/2]  # Add one edge after last point
            ])
            
            # Create 2D coordinate arrays for pcolormesh
            lon_mesh, lat_mesh = np.meshgrid(lon_edges, lat_edges)
            
            # Plot the data field using pcolormesh with explicit edges
            norm = plt.cm.colors.Normalize(vmin=vmin, vmax=vmax)
            cs = ax.pcolormesh(lon_mesh, lat_mesh, data_field, 
                              transform=ccrs.PlateCarree(),
                              cmap=cmap, norm=norm)
            logger.debug("Plotted wind data field with pcolormesh using explicit edges")
            
            # Determine extent from the calculated mesh edges
            plot_min_lon = lon_edges.min()
            plot_max_lon = lon_edges.max()
            plot_min_lat = lat_edges.min()
            plot_max_lat = lat_edges.max()
            
        except Exception as e:
            logger.error(f"Error plotting wind data field with pcolormesh: {e}", exc_info=True)
            raise Exception(f"Error plotting wind data field with pcolormesh: {e}")

        # Add colorbar
        try:
            # Add extend='max' to show the color for values > vmax
            cbar = plt.colorbar(cs, ax=ax, orientation='horizontal', pad=0.05, extend='max')
            cbar.set_label(label, fontsize=12)
            logger.debug("Added colorbar with extension for values > vmax")
        except Exception as e:
            logger.error(f"Error adding colorbar: {e}", exc_info=True)
            raise Exception(f"Error adding colorbar: {e}")

        # Calculate grid for wind barbs based on a fixed number for visual consistency
        try:
            target_barbs_per_dim = 15  # Aim for roughly 20x20 barbs
            rows, cols = lats.shape
            
            stride_lat = max(1, rows // target_barbs_per_dim)
            stride_lon = max(1, cols // target_barbs_per_dim)
            
            # Select a subset of coordinates and data using the calculated strides
            # Add offset to try and center the selection within the grid cells
            offset_lat = stride_lat // 2
            offset_lon = stride_lon // 2
            barb_lats_subset = lats[offset_lat::stride_lat, offset_lon::stride_lon]
            barb_lons_subset = lons[offset_lat::stride_lat, offset_lon::stride_lon]
            barb_u_subset = u_knots[offset_lat::stride_lat, offset_lon::stride_lon]
            barb_v_subset = v_knots[offset_lat::stride_lat, offset_lon::stride_lon]

            # Flatten the arrays for plotting
            barb_lats_flat = barb_lats_subset.flatten()
            barb_lons_flat = barb_lons_subset.flatten()
            barb_u_flat = barb_u_subset.flatten()
            barb_v_flat = barb_v_subset.flatten()

            logger.debug(f"Targeting ~{target_barbs_per_dim}x{target_barbs_per_dim} barbs. Strides: lat={stride_lat}, lon={stride_lon}. Number of barbs: {len(barb_lats_flat)}")
        except Exception as e:
            logger.error(f"Error computing wind barbs: {e}", exc_info=True)
            raise Exception(f"Error computing wind barbs: {e}")

        # Add wind barbs
        try:
            # Plot the selected subset of barbs at their original locations
            ax.barbs(barb_lons_flat, barb_lats_flat, barb_u_flat, barb_v_flat,
                    transform=ccrs.PlateCarree(),
                    length=7,  # Keep increased length
                    sizes=dict(emptybarb=0.15, spacing=0.15, width=0.3))
            logger.debug("Added larger wind barbs (fixed number) to plot at original grid points (subset)")
        except Exception as e:
            logger.error(f"Error adding wind barbs: {e}", exc_info=True)
            raise Exception(f"Error adding wind barbs: {e}")

        # Set map extent exactly to the calculated mesh boundaries
        try:
            ax.set_extent([plot_min_lon, plot_max_lon, plot_min_lat, plot_max_lat], crs=ccrs.PlateCarree())
            logger.debug(f"Set map extent from calculated mesh edges: ({plot_min_lon}, {plot_max_lon}, {plot_min_lat}, {plot_max_lat})")
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
            # Save without bbox_inches='tight' for now to check alignment
            plt.savefig(buf, format='png', dpi=150,
                       pil_kwargs={'optimize': True, 'quality': 85})
            plt.close()
            buf.seek(0)
            image_base64 = base64.b64encode(buf.getvalue()).decode()
            logger.info("Wind plot saved and encoded to base64")
            return image_base64
        except Exception as e:
            logger.error(f"Error saving wind plot to buffer: {e}", exc_info=True)
            raise Exception(f"Error saving wind plot to buffer: {e}")