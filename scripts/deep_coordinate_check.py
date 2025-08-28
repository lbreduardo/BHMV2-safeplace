import geopandas as gpd
import pandas as pd

# Read the file and check EVERY point
file_path = r"D:\IPHAN_project\processed_data\inpe\fire_points_final_clean.geojson"

try:
    gdf = gpd.read_file(file_path)

    print("=== DEEP COORDINATE INSPECTION ===")
    print(f"Total points: {len(gdf)}")

    # Check every single point
    valid_points = []
    invalid_points = []

    for idx, row in gdf.iterrows():
        lon = row.geometry.x
        lat = row.geometry.y

        # Very strict Brazil boundaries
        if (-74 <= lon <= -34) and (-34 <= lat <= 6):
            valid_points.append(idx)
        else:
            invalid_points.append({
                'index': idx,
                'longitude': lon,
                'latitude': lat,
                'estado': row.get('estado', 'Unknown')
            })

    print(f"Valid points: {len(valid_points)}")
    print(f"Invalid points: {len(invalid_points)}")

    print("\nINVALID POINTS DETAILS:")
    for point in invalid_points:
        print(
            f"  Index {point['index']}: Lon={point['longitude']:.6f}, Lat={point['latitude']:.6f}, Estado={point['estado']}")

        # Check if coordinates might be swapped
        swapped_lon = point['latitude']
        swapped_lat = point['longitude']
        if (-74 <= swapped_lon <= -34) and (-34 <= swapped_lat <= 6):
            print(f"    -> SWAPPED coordinates would be valid: Lon={swapped_lon:.6f}, Lat={swapped_lat:.6f}")

    # Create a clean version with only valid points
    if len(valid_points) > 0:
        clean_gdf = gdf.iloc[valid_points].copy()
        output_file = r"D:\IPHAN_project\processed_data\inpe\fire_points_truly_clean.geojson"
        clean_gdf.to_file(output_file, driver='GeoJSON')
        print(f"\nClean file saved with {len(clean_gdf)} valid points: {output_file}")

except Exception as e:
    print(f"Error: {e}")