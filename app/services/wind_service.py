import pygrib
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import pandas as pd
import base64
from io import BytesIO
from typing import Tuple, List, Dict
from datetime import datetime
import os
import re
from app.models.schemas import GribFileInfo

class WindService:
    def __init__(self):
        # Get the project root directory (2 levels up from this file)
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        # Construct the path to the gfs_atmos_p25 directory
        self.grib_dir = os.path.join(project_root, 'gfs_atmos_p25')

        # Verify the directory exists
        if not os.path.exists(self.grib_dir):
            raise FileNotFoundError(f"GRIB directory not found at: {self.grib_dir}")

    def is_ready(self) -> bool:
        """Check if GRIB files are available for processing"""
        if not os.path.exists(self.grib_dir):
            return False
        
        f000_files = [f for f in os.listdir(self.grib_dir) if re.match(r'gfs\.t\d{2}z\.pgrb2\.0p25\.f000$', f)]
        return len(f000_files) > 0

    def _get_grib_file_info(self, filename: str) -> GribFileInfo:
        """Extract information from GRIB filename and file metadata"""
        # Parse filename (e.g., gfs.t12z.pgrb2.0p25.f000)
        parts = filename.split('.')
        cycle_time = parts[1]  # e.g., t12z
        forecast_hour = int(parts[4][1:])  # e.g., 0 from f000
        
        # Get file modification time
        file_path = os.path.join(self.grib_dir, filename)
        download_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        
        return GribFileInfo(
            filename=filename,
            cycle_time=cycle_time,
            download_time=download_time,
            forecast_hour=forecast_hour
        )

    def process_wind_data(self, min_lat: float, max_lat: float, min_lon: float, max_lon: float) -> Tuple[List[dict], str, datetime, GribFileInfo]:
        if not self.is_ready():
            raise FileNotFoundError("GRIB files not yet available. Please try again in a few minutes.")

        # Find the latest f000 file in the gfs_atmos_p25 directory
        f000_files = [f for f in os.listdir(self.grib_dir) if re.match(r'gfs\.t\d{2}z\.pgrb2\.0p25\.f000$', f)]
        if not f000_files:
            raise FileNotFoundError(f"No gfs.tXXz.pgrb2.0p25.f000 files found in {self.grib_dir}")

        # Sort files by cycle time (e.g., t18z > t12z)
        def get_cycle_key(filename):
            cycle = filename.split('.')[1]  # e.g., 't12z'
            cycle_hour = int(cycle[1:3])  # e.g., 12
            return cycle_hour

        latest_f000 = max(f000_files, key=get_cycle_key)
        grib_file = os.path.join(self.grib_dir, latest_f000)
        print(f"Using latest f000 file: {grib_file}")

        # Get GRIB file information
        grib_info = self._get_grib_file_info(latest_f000)

        # Convert longitudes to 0-360° range for GFS
        min_lon_gfs = min_lon + 360 if min_lon < 0 else min_lon
        max_lon_gfs = max_lon + 360 if max_lon < 0 else max_lon

        # Open the GRIB file
        try:
            grbs = pygrib.open(grib_file)
        except Exception as e:
            raise Exception(f"Error opening {grib_file}: {e}")

        # Extract U and V wind components
        u_grb = grbs.select(name='10 metre U wind component')[0]
        v_grb = grbs.select(name='10 metre V wind component')[0]

        # Get full data and coordinates
        u_data_full, lats_full, lons_full = u_grb.data()
        v_data_full, _, _ = v_grb.data()

        # Find the indices of the bounding box in the full grid
        lat_indices = np.where((lats_full[:, 0] >= min_lat) & (lats_full[:, 0] <= max_lat))[0]
        lon_indices = np.where((lons_full[0, :] >= min_lon_gfs) & (lons_full[0, :] <= max_lon_gfs))[0]

        # Slice the 2D arrays to the bounding box
        u_data = u_data_full[lat_indices[0]:lat_indices[-1]+1, lon_indices[0]:lon_indices[-1]+1]
        v_data = v_data_full[lat_indices[0]:lat_indices[-1]+1, lon_indices[0]:lon_indices[-1]+1]
        lats = lats_full[lat_indices[0]:lat_indices[-1]+1, lon_indices[0]:lon_indices[-1]+1]
        lons = lons_full[lat_indices[0]:lat_indices[-1]+1, lon_indices[0]:lon_indices[-1]+1]

        # Convert longitudes back to -180 to 180 for plotting
        lons = np.where(lons > 180, lons - 360, lons)

        # Calculate wind speed at each grid point (in m/s)
        wind_speed_ms = np.sqrt(u_data**2 + v_data**2)
        wind_speed_knots = wind_speed_ms * 1.94384

        # Convert U and V components to knots for barbs
        u_knots = u_data * 1.94384
        v_knots = v_data * 1.94384

        # Create data points list
        data_points = []
        for i in range(wind_speed_knots.shape[0]):
            for j in range(wind_speed_knots.shape[1]):
                data_points.append({
                    'latitude': float(lats[i, j]),
                    'longitude': float(lons[i, j]),
                    'wind_speed_knots': float(wind_speed_knots[i, j])
                })

        # Generate and encode the plot
        image_base64 = self._generate_plot(lats, lons, wind_speed_knots, u_knots, v_knots, 
                                         min_lat, max_lat, min_lon, max_lon, u_grb.validDate)

        grbs.close()
        return data_points, image_base64, u_grb.validDate, grib_info

    def _generate_plot(self, lats, lons, wind_speed_knots, u_knots, v_knots, 
                      min_lat, max_lat, min_lon, max_lon, valid_time):
        """Generate a wind map plot with wind barbs"""
        # Create figure and axis with projection
        fig = plt.figure(figsize=(12, 8))
        ax = plt.axes(projection=ccrs.PlateCarree())
        
        # Add coastlines
        ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
        
        # Create custom colormap with smooth gradient
        N = 256
        # Create array of colors transitioning from blue -> green -> yellow -> red
        vals = np.ones((N, 4))
        
        # Split the range into three segments
        n1 = N // 4  # 0-10 knots (blue to green)
        n2 = N // 2  # 10-20 knots (green to yellow)
        n3 = 4 * N // 4  # 20-30 knots (yellow to red)
        
        # 0-10 knots: blue (0,0,1) to green (0,1,0)
        vals[:n1, 0] = 0  # red component
        vals[:n1, 1] = np.linspace(0, 1, n1)  # green component
        vals[:n1, 2] = np.linspace(1, 0, n1)  # blue component
        
        # 10-20 knots: green (0,1,0) to yellow (1,1,0)
        vals[n1:n2, 0] = np.linspace(0, 1, n2-n1)  # red component
        vals[n1:n2, 1] = 1  # green component
        vals[n1:n2, 2] = 0  # blue component
        
        # 20-30 knots: yellow (1,1,0) to red (1,0,0)
        vals[n2:n3, 0] = 1  # red component
        vals[n2:n3, 1] = np.linspace(1, 0, n3-n2)  # green component
        vals[n2:n3, 2] = 0  # blue component
        
        # 30-40 knots: stay red (1,0,0)
        vals[n3:, 0] = 1  # red component
        vals[n3:, 1] = 0  # green component
        vals[n3:, 2] = 0  # blue component
        
        cmap = plt.cm.colors.ListedColormap(vals)
        
        # Set up normalization to map wind speeds to colormap
        vmin, vmax = 0, 40  # Range of wind speeds to display
        norm = plt.cm.colors.Normalize(vmin=vmin, vmax=vmax)
        
        # Plot wind speed with custom colormap
        cs = ax.pcolormesh(lons, lats, wind_speed_knots, 
                          transform=ccrs.PlateCarree(),
                          cmap=cmap, norm=norm)
        
        # Add colorbar
        cbar = plt.colorbar(cs, ax=ax, orientation='horizontal', pad=0.05)
        cbar.set_label('Wind Speed (knots)', fontsize=10)
        
        # Calculate grid for wind barbs
        target_barbs = 20  # Target number of barbs in each direction
        
        # Calculate stride based on array shape and target number of barbs
        rows, cols = lats.shape
        stride_lat = max(1, rows // target_barbs)
        stride_lon = max(1, cols // target_barbs)
        
        # Create arrays for averaged wind barb positions and components
        barb_lats = []
        barb_lons = []
        barb_u = []
        barb_v = []
        
        # Average wind components over stride-sized blocks
        for i in range(0, rows-stride_lat, stride_lat):
            for j in range(0, cols-stride_lon, stride_lon):
                # Get the block of values
                lat_block = lats[i:i+stride_lat, j:j+stride_lon]
                lon_block = lons[i:i+stride_lat, j:j+stride_lon]
                u_block = u_knots[i:i+stride_lat, j:j+stride_lon]
                v_block = v_knots[i:i+stride_lat, j:j+stride_lon]
                
                # Calculate averages
                barb_lats.append(np.mean(lat_block))
                barb_lons.append(np.mean(lon_block))
                barb_u.append(np.mean(u_block))
                barb_v.append(np.mean(v_block))
        
        # Convert lists to numpy arrays
        barb_lats = np.array(barb_lats)
        barb_lons = np.array(barb_lons)
        barb_u = np.array(barb_u)
        barb_v = np.array(barb_v)
        
        # Add wind barbs using averaged values
        ax.barbs(barb_lons, barb_lats, barb_u, barb_v,
                transform=ccrs.PlateCarree(),
                length=5, sizes=dict(emptybarb=0.15, spacing=0.15, width=0.3))
        
        # Set map extent
        ax.set_extent([min_lon, max_lon, min_lat, max_lat], crs=ccrs.PlateCarree())
        
        # Add title with valid time
        title = f'Wind Speed and Direction\nValid: {valid_time.strftime("%Y-%m-%d %H:%M UTC")}'
        plt.title(title, pad=20)
        
        # Save plot to bytes buffer
        buf = BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', dpi=300)
        plt.close()
        
        # Convert to base64
        buf.seek(0)
        return base64.b64encode(buf.getvalue()).decode()