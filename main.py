import pygrib
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import pandas as pd

# Define the bounding box (5°x5° around 40°N, -70°W)
min_lat, max_lat = 37.5, 42.5
min_lon, max_lon = -72.5, -67.5  # In -180 to 180 range

# Convert longitudes to 0-360° range for GFS
min_lon_gfs = min_lon + 360 if min_lon < 0 else min_lon  # -72.5 becomes 287.5
max_lon_gfs = max_lon + 360 if max_lon < 0 else max_lon  # -67.5 becomes 292.5

# Path to the GFS GRIB file
grib_file = 'gfs.t12z.pgrb2.0p25.f000.grib2'

# Open the GRIB file
try:
    grbs = pygrib.open(grib_file)
    print(f"Opened {grib_file} successfully")
except Exception as e:
    print(f"Error opening {grib_file}: {e}")
    exit()

# Extract U and V wind components
u_grb = grbs.select(name='10 metre U wind component')[0]
v_grb = grbs.select(name='10 metre V wind component')[0]

# Get full data and coordinates
u_data_full, lats_full, lons_full = u_grb.data()
v_data_full, _, _ = v_grb.data()  # lats/lons are the same for U and V

# Create a mask for the bounding box while preserving 2D structure
mask_2d = (lats_full >= min_lat) & (lats_full <= max_lat) & \
          (lons_full >= min_lon_gfs) & (lons_full <= max_lon_gfs)

# Check if any points are found
if not np.any(mask_2d):
    print("Error: No data points found within the bounding box.")
    print(f"Lat range: {min_lat} to {max_lat}, Lon range: {min_lon_gfs} to {max_lon_gfs}")
    exit()

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

# Convert wind speed to knots (1 m/s = 1.94384 knots)
wind_speed_knots = wind_speed_ms * 1.94384

# Convert U and V components to knots for barbs
u_knots = u_data * 1.94384
v_knots = v_data * 1.94384

# Print wind speed data for all points
print("\nWind Speed Data (knots) for all points in the bounding box:")
print("Latitude (°N) | Longitude (°W) | Wind Speed (knots)")
for i in range(wind_speed_knots.shape[0]):
    for j in range(wind_speed_knots.shape[1]):
        print(f"{lats[i, j]:.2f}         | {lons[i, j]:.2f}         | {wind_speed_knots[i, j]:.2f}")

# Save wind speed data to a CSV file
data_dict = {
    'Latitude (°N)': lats.flatten(),
    'Longitude (°W)': lons.flatten(),
    'Wind Speed (knots)': wind_speed_knots.flatten()
}
df = pd.DataFrame(data_dict)
df.to_csv('wind_speed_data.csv', index=False)
print("Wind speed data saved to 'wind_speed_data.csv'")

# Subsample for barbs (every 1° instead of 0.25° to avoid clutter)
step = 4  # 0.25° resolution, so every 4th point is ~1°
u_knots_sub = u_knots[::step, ::step]
v_knots_sub = v_knots[::step, ::step]
lats_sub = lats[::step, ::step]
lons_sub = lons[::step, ::step]

# Create a figure with a map projection
fig = plt.figure(figsize=(10, 8))
ax = plt.axes(projection=ccrs.PlateCarree())

# Set the map extent to the bounding box
ax.set_extent([min_lon, max_lon, min_lat, max_lat], crs=ccrs.PlateCarree())

# Plot wind speed with pcolormesh for smooth interpolation
mesh = ax.pcolormesh(lons, lats, wind_speed_knots, transform=ccrs.PlateCarree(), cmap='viridis', shading='auto')

# Add wind barbs on top
barb_plot = ax.barbs(lons_sub, lats_sub, u_knots_sub, v_knots_sub, transform=ccrs.PlateCarree(),
                     length=5, barbcolor='black', linewidth=0.5)

# Add coastlines for geographical context
ax.coastlines()

# Add a colorbar to show wind speed values in knots
cbar = plt.colorbar(mesh, ax=ax, orientation='vertical', pad=0.05, label='Wind Speed (knots)')

# Set the title with the valid time
ax.set_title(f'10m Wind Speed (knots) with Wind Barbs, Valid Time: {u_grb.validDate} UTC')

# Save the plot to a file
plt.savefig('wind_map.png', dpi=300, bbox_inches='tight')
print("Map saved as 'wind_map.png'")

# Option to show the plot
show_plot = True
if show_plot:
    plt.show()
else:
    plt.close()

# Close the GRIB file
grbs.close()