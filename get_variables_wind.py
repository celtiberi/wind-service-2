import pygrib

# Path to your downloaded file
grib_file = 'atmos/gfs.t12z.pgrb2.0p25.f000'  # Adjust if renamed (e.g., add .grib2)

# Open the GRIB file
try:
    grbs = pygrib.open(grib_file)
    print(f"Opened {grib_file} successfully")
except Exception as e:
    print(f"Error opening {grib_file}: {e}")
    exit()

# List all parameters
print("\nAvailable parameters in the file:")
# Open file for writing
with open('variables.txt', 'w') as f:
    for grb in grbs:
        line = f"Name: {grb.name}, Level: {grb.level} {grb.typeOfLevel}, Units: {grb.units}, Forecast Hour: {grb.forecastTime}"
        print(line)
        f.write(line + '\n')

# Close the file
grbs.close()