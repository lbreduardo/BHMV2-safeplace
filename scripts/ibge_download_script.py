import geopandas as gpd
import os
from zipfile import ZipFile
import requests
import tempfile
from tqdm import tqdm
import sys
import shutil


def download_file(url, output_path):
    """Download a file with progress tracking"""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        block_size = 8192

        with open(output_path, 'wb') as f:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading") as pbar:
                for chunk in response.iter_content(chunk_size=block_size):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
        return True
    except Exception as e:
        print(f"\nError downloading file: {e}")
        return False


def get_ibge_states():
    """Download and extract IBGE state boundaries"""
    # Updated working URL as of July 2024
    url = "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/Brasil/BR/BR_UF_2022.zip"

    with tempfile.TemporaryDirectory() as temp_dir:
        zip_path = os.path.join(temp_dir, "BR_UF_2022.zip")

        print("\nDownloading IBGE state boundaries...")
        if not download_file(url, zip_path):
            print("\nFailed to download from primary source. Trying alternative...")
            alt_url = "https://github.com/kelvins/Municipios-Brasileiros/raw/main/geojson/estados.geojson"
            try:
                states = gpd.read_file(alt_url)
                print("Successfully loaded from GitHub alternative source")
                return states[['sigla', 'geometry']].rename(columns={'sigla': 'state_code'})
            except Exception as e:
                print(f"Failed to use alternative source: {e}")
                sys.exit(1)

        try:
            with ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            # Find the shapefile
            shp_file = None
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    if file.endswith('.shp'):
                        shp_file = os.path.join(root, file)
                        print(f"Found shapefile: {shp_file}")
                        break
                if shp_file:
                    break

            if not shp_file:
                print("Could not find shapefile in downloaded package")
                sys.exit(1)

            states = gpd.read_file(shp_file)
            # Handle different column names
            if 'SIGLA_UF' in states.columns:
                return states[['SIGLA_UF', 'geometry']].rename(columns={'SIGLA_UF': 'state_code'})
            elif 'sigla' in states.columns:
                return states[['sigla', 'geometry']].rename(columns={'sigla': 'state_code'})
            else:
                print("Error: Could not find state code column in shapefile")
                sys.exit(1)

        except Exception as e:
            print(f"Error processing downloaded file: {e}")
            sys.exit(1)


def main():
    print("\nStarting IPHAN-IBGE spatial join processing...")

    # 1. Get IBGE state boundaries
    states = get_ibge_states()
    print(f"\nLoaded state boundaries with {len(states)} features")

    # 2. Load IPHAN data with flexible path handling
    iphan_paths = [
        os.path.join("project_data", "iphan", "filtered_data", "filtered_SICG_bem_poligono.gpkg"),
        os.path.join("..", "project_data", "iphan", "filtered_data", "filtered_SICG_bem_poligono.gpkg"),
        os.path.join("D:", "IPHAN_project", "project_data", "iphan", "filtered_data", "filtered_SICG_bem_poligono.gpkg")
    ]

    iphan_data = None
    for path in iphan_paths:
        if os.path.exists(path):
            try:
                print(f"\nLoading IPHAN data from: {path}")
                iphan_data = gpd.read_file(path)
                print(f"Successfully loaded {len(iphan_data)} features")
                break
            except Exception as e:
                print(f"Error loading {path}: {e}")
                continue

    if iphan_data is None:
        print("\nError: Could not find IPHAN data at any of these locations:")
        for path in iphan_paths:
            print(f"- {path}")
        print("\nPlease ensure:")
        print("1. The file exists in one of these paths")
        print("2. You have proper read permissions")
        print("3. The filename is exactly 'filtered_SICG_bem_poligono.gpkg'")
        sys.exit(1)

    # 3. Process CRS
    print("\nProcessing coordinate systems...")
    print(f"IPHAN original CRS: {iphan_data.crs}")
    print(f"IBGE states CRS: {states.crs}")

    iphan_data = iphan_data.to_crs(states.crs)
    print("CRS alignment complete")

    # 4. Spatial join
    print("\nPerforming spatial join...")
    result = gpd.sjoin(
        iphan_data,
        states,
        how='left',
        predicate='within'
    )

    # 5. Save results
    output_dir = os.path.join("project_data", "output_results")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "iphan_data_with_states.gpkg")

    try:
        print(f"\nSaving results to: {output_path}")
        result.to_file(output_path, driver="GPKG")

        # Report results
        matched = len(result[~result['state_code'].isna()])
        total = len(result)

        print("\n========================================")
        print("PROCESSING COMPLETE - RESULTS SUMMARY")
        print("========================================")
        print(f"Total features processed: {total}")
        print(f"Features matched to states: {matched} ({matched / total:.1%})")
        print(f"Features without state match: {total - matched}")

        if (total - matched) > 0:
            print("\nNote: Some features didn't match state boundaries.")
            print("Check 'unmatched_features.gpkg' for investigation.")

            # Save unmatched features
            unmatched_path = os.path.join(output_dir, "unmatched_features.gpkg")
            result[result['state_code'].isna()].to_file(unmatched_path, driver="GPKG")
            print(f"Unmatched features saved to: {unmatched_path}")

        print("\nScript finished successfully!")

    except Exception as e:
        print(f"\nError saving results: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()