import geopandas as gpd
from shapely.geometry import Point
import pandas as pd

# Create a dataset with only points definitely in Brazil
output_file = r"D:\IPHAN_project\processed_data\inpe\fire_points_brazil_only.geojson"

# Sample Brazilian coordinates (we know these are correct)
brazil_fire_data = [
    # Amazon region
    {'lon': -60.0250, 'lat': -3.1190, 'estado': 'AM', 'municipio': 'Manaus'},
    {'lon': -67.8099, 'lat': -9.9749, 'estado': 'AC', 'municipio': 'Rio Branco'},
    {'lon': -51.9253, 'lat': -14.2350, 'estado': 'MT', 'municipio': 'Cuiabá'},

    # Northeast
    {'lon': -38.5108, 'lat': -12.9714, 'estado': 'BA', 'municipio': 'Salvador'},
    {'lon': -34.8771, 'lat': -8.0578, 'estado': 'PE', 'municipio': 'Recife'},
    {'lon': -35.2094, 'lat': -5.7945, 'estado': 'RN', 'municipio': 'Natal'},

    # Southeast
    {'lon': -43.1729, 'lat': -22.9068, 'estado': 'RJ', 'municipio': 'Rio de Janeiro'},
    {'lon': -46.6333, 'lat': -23.5505, 'estado': 'SP', 'municipio': 'São Paulo'},
    {'lon': -43.9378, 'lat': -19.9208, 'estado': 'MG', 'municipio': 'Belo Horizonte'},

    # South
    {'lon': -49.2732, 'lat': -25.4284, 'estado': 'PR', 'municipio': 'Curitiba'},
    {'lon': -51.2177, 'lat': -30.0346, 'estado': 'RS', 'municipio': 'Porto Alegre'},

    # Center-West
    {'lon': -47.8825, 'lat': -15.7942, 'estado': 'DF', 'municipio': 'Brasília'},
]

# Create DataFrame
df = pd.DataFrame(brazil_fire_data)

# Add some metadata
df['data_hora'] = '2025-07-14 12:00:00'
df['collection_time'] = '2025-07-14 12:00:00'
df['satelite'] = 'TEST_SATELLITE'
df['bioma'] = 'Test'

# Create geometry
geometry = [Point(row['lon'], row['lat']) for _, row in df.iterrows()]
gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')

# Save
gdf.to_file(output_file, driver='GeoJSON')

print(f"Brazil-only test file created: {output_file}")
print(f"Points: {len(gdf)}")
print("These are all major Brazilian cities - they should appear correctly in QGIS")