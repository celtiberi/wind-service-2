import geopandas as gpd

# Load the oceans shapefile
oceans = gpd.read_file("app/natural_earth/ne_10m_lakes.shp")
# Print the first few rows to see the data
# print(oceans.head())

# # Print the column names
# print(oceans.columns)

# Print the full GeoDataFrame to see all rows
print(oceans)

# Check the CRS (coordinate reference system) and basic info
print(oceans.crs)
print(oceans.info())

# Print all country names
print("All country names:")
for name in oceans["name"]:
    print(name)