import pygrib

# Path to your downloaded file
grib_file = 'gfs.t12z.pgrb2.0p25.f000.grib2'  # Adjust if renamed (e.g., add .grib2)

# Open the GRIB file
try:
    grbs = pygrib.open(grib_file)
    print(f"Opened {grib_file} successfully")
except Exception as e:
    print(f"Error opening {grib_file}: {e}")
    exit()

# List all parameters
print("\nAvailable parameters in the file:")
for grb in grbs:
    print(f"Name: {grb.name}, Level: {grb.level} {grb.typeOfLevel}, Units: {grb.units}, Forecast Hour: {grb.forecastTime}")

# Close the file
grbs.close()