import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

# Read the problematic data
input_file = r"D:\IPHAN_project\processed_data\inpe\current_fire_points.geojson"
output_file = r"D:\IPHAN_project\processed_data\inpe\current_fire_points_fixed.geojson"

try:
    # Load data
    gdf = gpd.read_file(input_file)
    print(f"Original points: {len(gdf)}")

    # Extract coordinates
    gdf['lon'] = gdf.geometry.x
    gdf['lat'] = gdf.geometry.y

    # ULTRA-STRICT Brazil filtering
    # Using very conservative bounds
    brazil_mask = (
            (gdf['lon'] >= -74) & (gdf['lon'] <= -34) &
            (gdf['lat'] >= -34) & (gdf['lat'] <= 6)
    )

    # Additional filter: remove obvious ocean points
    ocean_mask = ~(
            (gdf['lon'] > -30) |  # Too far east (Atlantic)
            (gdf['lat'] > 6) |  # Too far north
            (gdf['lat'] < -35) |  # Too far south
            (gdf['lon'] < -75)  # Too far west
    )

    # Combine filters
    valid_mask = brazil_mask & ocean_mask

    # Apply filter
    gdf_clean = gdf[valid_mask].copy()

    print(f"Points after filtering: {len(gdf_clean)}")
    print(f"Points removed: {len(gdf) - len(gdf_clean)}")

    # Recreate geometry to ensure consistency
    gdf_clean['geometry'] = [Point(lon, lat) for lon, lat in zip(gdf_clean['lon'], gdf_clean['lat'])]

    # Remove helper columns
    gdf_clean = gdf_clean.drop(['lon', 'lat'], axis=1)

    # Save clean version
    gdf_clean.to_file(output_file, driver='GeoJSON')

    print(f"Clean data saved to: {output_file}")

    # Verify the cleaned data
    print(f"\nVerification:")
    print(f"Longitude range: {gdf_clean.geometry.x.min():.3f} to {gdf_clean.geometry.x.max():.3f}")
    print(f"Latitude range: {gdf_clean.geometry.y.min():.3f} to {gdf_clean.geometry.y.max():.3f}")

except Exception as e:
    print(f"Error: {e}")