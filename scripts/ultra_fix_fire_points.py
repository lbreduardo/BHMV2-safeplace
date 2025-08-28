import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

# Read the data
input_file = r"D:\IPHAN_project\processed_data\inpe\current_fire_points.geojson"
output_file = r"D:\IPHAN_project\processed_data\inpe\current_fire_points_ultra_clean.geojson"

try:
    # Load data
    gdf = gpd.read_file(input_file)
    print(f"Original points: {len(gdf)}")

    # Extract coordinates
    gdf['lon'] = gdf.geometry.x
    gdf['lat'] = gdf.geometry.y

    print(f"Coordinate ranges:")
    print(f"  Longitude: {gdf['lon'].min():.3f} to {gdf['lon'].max():.3f}")
    print(f"  Latitude: {gdf['lat'].min():.3f} to {gdf['lat'].max():.3f}")

    # ULTRA AGGRESSIVE filtering - only keep points definitely in Brazil mainland
    ultra_strict_mask = (
            (gdf['lon'] >= -70) & (gdf['lon'] <= -38) &  # Mainland Brazil longitude
            (gdf['lat'] >= -30) & (gdf['lat'] <= 3)  # Mainland Brazil latitude
    )

    # Apply ultra-strict filter
    gdf_ultra = gdf[ultra_strict_mask].copy()

    print(f"Points after ultra-strict filtering: {len(gdf_ultra)}")
    print(f"Points removed: {len(gdf) - len(gdf_ultra)}")

    # Show coordinate ranges after filtering
    if len(gdf_ultra) > 0:
        print(f"Final coordinate ranges:")
        print(f"  Longitude: {gdf_ultra['lon'].min():.3f} to {gdf_ultra['lon'].max():.3f}")
        print(f"  Latitude: {gdf_ultra['lat'].min():.3f} to {gdf_ultra['lat'].max():.3f}")

        # Recreate geometry
        gdf_ultra['geometry'] = [Point(lon, lat) for lon, lat in zip(gdf_ultra['lon'], gdf_ultra['lat'])]

        # Remove helper columns
        gdf_ultra = gdf_ultra.drop(['lon', 'lat'], axis=1)

        # Save ultra-clean version
        gdf_ultra.to_file(output_file, driver='GeoJSON')
        print(f"Ultra-clean data saved to: {output_file}")
    else:
        print("ERROR: No points left after filtering!")

except Exception as e:
    print(f"Error: {e}")