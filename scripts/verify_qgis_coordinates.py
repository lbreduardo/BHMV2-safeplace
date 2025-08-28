import geopandas as gpd

# Read the final clean file
file_path = r"D:\IPHAN_project\processed_data\inpe\fire_points_final_clean.geojson"

try:
    gdf = gpd.read_file(file_path)

    print("=== COORDINATE VERIFICATION ===")
    print(f"Total points: {len(gdf)}")
    print(f"CRS: {gdf.crs}")

    # Check first 5 points
    print("\nFirst 5 points coordinates:")
    for i in range(min(5, len(gdf))):
        x, y = gdf.iloc[i].geometry.x, gdf.iloc[i].geometry.y
        print(f"  Point {i + 1}: Longitude={x:.6f}, Latitude={y:.6f}")

        # Check if in Brazil
        in_brazil = (-73 <= x <= -35) and (-33 <= y <= 5)
        print(f"    In Brazil: {in_brazil}")

    # Check for any points outside Brazil
    outside_count = 0
    for idx, row in gdf.iterrows():
        x, y = row.geometry.x, row.geometry.y
        if not ((-73 <= x <= -35) and (-33 <= y <= 5)):
            outside_count += 1
            if outside_count <= 3:  # Show first 3 problematic points
                print(f"  OUTSIDE BRAZIL: Point {idx}: Lon={x:.6f}, Lat={y:.6f}")

    print(f"\nTotal points outside Brazil: {outside_count}")

    if outside_count == 0:
        print("✅ ALL POINTS ARE WITHIN BRAZIL BOUNDARIES")
    else:
        print("❌ SOME POINTS ARE OUTSIDE BRAZIL")

except Exception as e:
    print(f"Error: {e}")