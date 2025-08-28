import pandas as pd
import geopandas as gpd

# Load your current fire points data
file_path = r"D:\IPHAN_project\processed_data\inpe\current_fire_points.geojson"

try:
    # Read the GeoJSON
    gdf = gpd.read_file(file_path)

    print(f"Total points: {len(gdf)}")
    print(f"Columns: {list(gdf.columns)}")

    # Extract coordinates
    gdf['lon'] = gdf.geometry.x
    gdf['lat'] = gdf.geometry.y

    # Check coordinate ranges
    print(f"\nCoordinate Ranges:")
    print(f"Longitude: {gdf['lon'].min():.3f} to {gdf['lon'].max():.3f}")
    print(f"Latitude: {gdf['lat'].min():.3f} to {gdf['lat'].max():.3f}")

    # Brazil bounds for reference
    brazil_bounds = {
        'west': -73.98, 'east': -34.79,
        'south': -33.75, 'north': 5.27
    }

    # Check points outside Brazil
    outside_points = gdf[
        (gdf['lon'] < brazil_bounds['west']) |
        (gdf['lon'] > brazil_bounds['east']) |
        (gdf['lat'] < brazil_bounds['south']) |
        (gdf['lat'] > brazil_bounds['north'])
        ]

    print(f"\nPoints outside Brazil bounds: {len(outside_points)}")

    if len(outside_points) > 0:
        print("\nSample of problematic points:")
        for idx, row in outside_points.head(10).iterrows():
            print(f"  Point {idx}: Lon={row['lon']:.3f}, Lat={row['lat']:.3f}")

    # Check for potential coordinate swaps
    swapped = gdf[
        (gdf['lat'] < brazil_bounds['west']) |
        (gdf['lat'] > brazil_bounds['east']) |
        (gdf['lon'] < brazil_bounds['south']) |
        (gdf['lon'] > brazil_bounds['north'])
        ]

    print(f"\nPossible coordinate swaps: {len(swapped)}")

except Exception as e:
    print(f"Error reading file: {e}")