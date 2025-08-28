import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

# Read the original data
input_file = r"D:\IPHAN_project\processed_data\inpe\current_fire_points.geojson"
output_file = r"D:\IPHAN_project\processed_data\inpe\fire_points_final_clean.geojson"

try:
    # Load data
    gdf = gpd.read_file(input_file)
    print(f"Original points: {len(gdf)}")

    # Extract coordinates explicitly
    coords_data = []
    for idx, row in gdf.iterrows():
        lon = float(row.geometry.x)
        lat = float(row.geometry.y)

        # Only keep points definitely in Brazil mainland
        if (-70 <= lon <= -35) and (-30 <= lat <= 5):
            coords_data.append({
                'longitude': lon,
                'latitude': lat,
                'estado': row.get('estado', 'Unknown'),
                'municipio': row.get('municipio', 'Unknown'),
                'data_hora': row.get('data_hora', 'Unknown')
            })

    print(f"Valid points after filtering: {len(coords_data)}")

    if len(coords_data) > 0:
        # Create new DataFrame with explicit coordinate columns
        df_clean = pd.DataFrame(coords_data)

        # Create geometry explicitly
        geometry = [Point(row['longitude'], row['latitude']) for _, row in df_clean.iterrows()]

        # Create GeoDataFrame with explicit CRS
        gdf_final = gpd.GeoDataFrame(df_clean, geometry=geometry, crs='EPSG:4326')

        # Save with explicit driver settings
        gdf_final.to_file(output_file, driver='GeoJSON', encoding='utf-8')

        print(f"Final clean data saved to: {output_file}")
        print(f"Coordinate ranges:")
        print(f"  Longitude: {df_clean['longitude'].min():.3f} to {df_clean['longitude'].max():.3f}")
        print(f"  Latitude: {df_clean['latitude'].min():.3f} to {df_clean['latitude'].max():.3f}")

        # Verify the saved file
        verify_gdf = gpd.read_file(output_file)
        print(f"Verification - file contains {len(verify_gdf)} points")

    else:
        print("ERROR: No valid points found!")

except Exception as e:
    print(f"Error: {e}")