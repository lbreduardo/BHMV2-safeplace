# generate_map.py - A complete data pipeline for heritage risk mapping
# This script processes data from various sources and prepares it for QGIS

import os
import shutil
import json
from datetime import datetime
import sys

# STEP 1: SET UP PATHS
# These paths tell the script where to find and save files
# The script will automatically find your project's main folder
current_script_path = os.path.abspath(__file__)  # Get the path of this script
scripts_folder = os.path.dirname(current_script_path)  # Get the scripts folder
PROJECT_DIR = os.path.dirname(scripts_folder)  # Go up one level to the main project folder

# Define where to find and save data
RAW_DATA_DIR = os.path.join(PROJECT_DIR, "project_data")  # Where your raw data is stored
OUTPUT_DIR = os.path.join(PROJECT_DIR, "processed_data")  # Where to save processed data
TEMP_DIR = os.path.join(PROJECT_DIR, "temp")  # For temporary files

# Make sure the output and temp directories exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)


# STEP 2: DEFINE PROCESSING FUNCTIONS
# Each function handles one dataset

def process_iphan_data():
    """Process IPHAN heritage site data"""
    print("\n=== Processing IPHAN heritage site data ===")

    # Set up paths
    iphan_dir = os.path.join(RAW_DATA_DIR, "iphan")
    output_dir = os.path.join(OUTPUT_DIR, "iphan")
    os.makedirs(output_dir, exist_ok=True)

    # Find and copy GeoJSON files from final_output folder
    final_output_dir = os.path.join(iphan_dir, "final_output")
    if os.path.exists(final_output_dir):
        file_count = 0
        for filename in os.listdir(final_output_dir):
            if filename.endswith((".geojson", ".gpkg")):
                source = os.path.join(final_output_dir, filename)
                destination = os.path.join(output_dir, filename)
                shutil.copy2(source, destination)
                print(f"  Copied: {filename}")
                file_count += 1

        if file_count > 0:
            print(f"  Successfully processed {file_count} IPHAN files")
        else:
            print("  WARNING: No GeoJSON or GPKG files found in final_output folder")
    else:
        print(f"  ERROR: Could not find final_output folder in {iphan_dir}")

    return True


def process_cemaden_data():
    """Process CEMADEN risk alert data"""
    print("\n=== Processing CEMADEN alert data ===")

    # Set up paths
    cemaden_dir = os.path.join(RAW_DATA_DIR, "cemaden")
    output_dir = os.path.join(OUTPUT_DIR, "cemaden")
    os.makedirs(output_dir, exist_ok=True)

    # Find and copy the most recent data files
    if os.path.exists(cemaden_dir):
        file_count = 0
        for filename in os.listdir(cemaden_dir):
            # Look for the main data files (not timestamped versions)
            if filename.startswith("real_time_cemaden_data") and not "_202" in filename:
                source = os.path.join(cemaden_dir, filename)
                destination = os.path.join(output_dir, filename)
                shutil.copy2(source, destination)
                print(f"  Copied: {filename}")
                file_count += 1

        if file_count > 0:
            print(f"  Successfully processed {file_count} CEMADEN files")
        else:
            print("  WARNING: No real-time CEMADEN data files found")
    else:
        print(f"  ERROR: Could not find CEMADEN folder at {cemaden_dir}")

    return True


def process_inpe_data():
    """Process INPE fire risk data"""
    print("\n=== Processing INPE fire data ===")

    # Set up paths
    inpe_dir = os.path.join(RAW_DATA_DIR, "inpe_data")
    output_dir = os.path.join(OUTPUT_DIR, "inpe")
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Import and run the collector
        sys.path.append(scripts_folder)  # Ensure scripts folder is in path
        from inpe_collector import INPEFireCollector

        # Run the collector to ensure fresh data
        print("  Running INPE fire data collector...")
        collector = INPEFireCollector(base_dir=RAW_DATA_DIR)
        results = collector.run(days=1)

        if results['success']:
            print(f"  INPE collector successfully retrieved {len(results['geodataframe'])} fire points")

            # Copy the latest fire data files to the output directory
            geojson_dir = os.path.join(inpe_dir, "geojson")
            if os.path.exists(geojson_dir):
                # Copy GeoJSON for web mapping
                geojson_path = os.path.join(geojson_dir, "current_fire_points.geojson")
                if os.path.exists(geojson_path):
                    dest_geojson = os.path.join(output_dir, "current_fire_points.geojson")
                    shutil.copy2(geojson_path, dest_geojson)
                    print(f"  Copied current fire points GeoJSON to output directory")

                # Also copy the latest shapefile
                processed_dir = os.path.join(inpe_dir, "processed")
                if os.path.exists(processed_dir):
                    shp_files = sorted(
                        [f for f in os.listdir(processed_dir) if f.endswith('.shp') and f.startswith('fire_points_')],
                        reverse=True)
                    if shp_files:
                        latest_shp = shp_files[0]
                        base_name = os.path.splitext(latest_shp)[0]
                        for ext in ['.shp', '.shx', '.dbf', '.prj', '.cpg']:
                            src = os.path.join(processed_dir, f"{base_name}{ext}")
                            if os.path.exists(src):
                                dest = os.path.join(output_dir, f"{base_name}{ext}")
                                shutil.copy2(src, dest)
                                print(f"  Copied {base_name}{ext} to output directory")

                # Also copy the summary file
                summary_dir = os.path.join(inpe_dir, "summaries")
                if os.path.exists(summary_dir):
                    summary_path = os.path.join(summary_dir, "current_summaries.json")
                    if os.path.exists(summary_path):
                        dest_summary = os.path.join(output_dir, "fire_summary.json")
                        shutil.copy2(summary_path, dest_summary)
                        print(f"  Copied fire summary to output directory")

                data_quality = "REAL-TIME" if not results.get('is_test_data', False) else "TEST"
                print(f"  Successfully processed INPE fire data ({data_quality} DATA)")
                return True
            else:
                print(f"  ERROR: Could not find geojson directory in {inpe_dir}")
                return False
        else:
            print("  ERROR: INPE data collection failed")
            return False

    except Exception as e:
        print(f"  ERROR processing INPE data: {str(e)}")

        # Fallback to old method if collector integration fails
        print("  Falling back to direct file copy method...")
        processed_dir = os.path.join(inpe_dir, "processed")
        if os.path.exists(processed_dir):
            file_count = 0
            for filename in os.listdir(processed_dir):
                if filename.startswith("fire_") and filename.endswith((".shp", ".shx", ".dbf", ".prj", ".cpg")):
                    source = os.path.join(processed_dir, filename)
                    destination = os.path.join(output_dir, filename)
                    shutil.copy2(source, destination)
                    print(f"  Copied: {filename}")
                    file_count += 1

            if file_count > 0:
                print(f"  Successfully processed {file_count} INPE files (FALLBACK METHOD)")
                return True
            else:
                print("  WARNING: No fire data files found in the processed folder")
                return False
        else:
            print(f"  ERROR: Could not find processed folder in {inpe_dir}")
            return False


def process_snisb_data():
    """Process SNISB dam risk data"""
    print("\n=== Processing SNISB dam data ===")

    # Set up paths
    snisb_dir = os.path.join(RAW_DATA_DIR, "snisb_data")
    geojson_dir = os.path.join(snisb_dir, "GeoJSON")
    output_dir = os.path.join(OUTPUT_DIR, "snisb")
    os.makedirs(output_dir, exist_ok=True)

    # Find and copy GeoJSON files
    if os.path.exists(geojson_dir):
        file_count = 0
        for filename in os.listdir(geojson_dir):
            if filename.endswith(".geojson"):
                source = os.path.join(geojson_dir, filename)
                destination = os.path.join(output_dir, filename)
                shutil.copy2(source, destination)
                print(f"  Copied: {filename}")
                file_count += 1

        if file_count > 0:
            print(f"  Successfully processed {file_count} SNISB files")
        else:
            print("  WARNING: No GeoJSON files found in the GeoJSON folder")
    else:
        print(f"  ERROR: Could not find GeoJSON folder in {snisb_dir}")

    return True


def process_ibge_data():
    """Process IBGE boundary data"""
    print("\n=== Processing IBGE boundary data ===")

    # Set up paths
    output_dir = os.path.join(OUTPUT_DIR, "ibge")
    os.makedirs(output_dir, exist_ok=True)

    # Look for state boundary files
    state_files_found = 0
    state_base_name = "BR_UF_2022"

    # Check for files in the main project_data folder
    for extension in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
        source_path = os.path.join(RAW_DATA_DIR, f"{state_base_name}{extension}")
        if os.path.exists(source_path):
            destination = os.path.join(output_dir, f"{state_base_name}{extension}")
            shutil.copy2(source_path, destination)
            print(f"  Copied: {state_base_name}{extension}")
            state_files_found += 1

    if state_files_found > 0:
        print(f"  Successfully processed state boundary files")
    else:
        print("  WARNING: State boundary files not found")
        print("  NOTE: You will need to add municipal boundaries later")

    return True


def create_summary_report():
    """Create a summary report of processed data"""
    print("\n=== Creating summary report ===")

    # Get data quality from INPE summaries if available
    inpe_data_quality = "UNKNOWN"
    inpe_summary_path = os.path.join(OUTPUT_DIR, "inpe", "fire_summary.json")
    if os.path.exists(inpe_summary_path):
        try:
            with open(inpe_summary_path, 'r') as f:
                inpe_summary = json.load(f)
                if "_metadata" in inpe_summary:
                    inpe_data_quality = inpe_summary["_metadata"].get("data_quality", "UNKNOWN")
        except:
            pass

    # Create a comprehensive report structure
    report = {
        "report_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "processed_datasets": {
            "iphan": {
                "status": os.path.exists(os.path.join(OUTPUT_DIR, "iphan")),
                "files": [f for f in os.listdir(os.path.join(OUTPUT_DIR, "iphan"))] if os.path.exists(
                    os.path.join(OUTPUT_DIR, "iphan")) else []
            },
            "cemaden": {
                "status": os.path.exists(os.path.join(OUTPUT_DIR, "cemaden")),
                "files": [f for f in os.listdir(os.path.join(OUTPUT_DIR, "cemaden"))] if os.path.exists(
                    os.path.join(OUTPUT_DIR, "cemaden")) else []
            },
            "inpe": {
                "status": os.path.exists(os.path.join(OUTPUT_DIR, "inpe")),
                "files": [f for f in os.listdir(os.path.join(OUTPUT_DIR, "inpe"))] if os.path.exists(
                    os.path.join(OUTPUT_DIR, "inpe")) else [],
                "data_quality": inpe_data_quality
            },
            "snisb": {
                "status": os.path.exists(os.path.join(OUTPUT_DIR, "snisb")),
                "files": [f for f in os.listdir(os.path.join(OUTPUT_DIR, "snisb"))] if os.path.exists(
                    os.path.join(OUTPUT_DIR, "snisb")) else []
            },
            "ibge": {
                "status": os.path.exists(os.path.join(OUTPUT_DIR, "ibge")),
                "files": [f for f in os.listdir(os.path.join(OUTPUT_DIR, "ibge"))] if os.path.exists(
                    os.path.join(OUTPUT_DIR, "ibge")) else []
            }
        },
        "notes": "This report shows which datasets were successfully processed. "
                 "The next step is to create a QGIS project using these datasets.",
        "pipeline_version": "1.2",
        "last_update": "July 14, 2025"
    }

    # Save the report
    report_path = os.path.join(OUTPUT_DIR, "processing_report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"  Report saved to: {report_path}")
    return True


# STEP 3: MAIN FUNCTION TO RUN THE PIPELINE
def main():
    """Run the complete data pipeline"""
    start_time = datetime.now()
    print("\n============================================")
    print("HERITAGE RISK MAP DATA PROCESSING PIPELINE")
    print("============================================")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Project directory: {PROJECT_DIR}")

    # Process each dataset
    process_iphan_data()
    process_cemaden_data()
    process_inpe_data()
    process_snisb_data()
    process_ibge_data()

    # Create a summary report
    create_summary_report()

    # Calculate execution time
    end_time = datetime.now()
    execution_time = end_time - start_time

    print("\n============================================")
    print("PIPELINE EXECUTION COMPLETE")
    print("============================================")
    print(f"Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total execution time: {execution_time}")
    print(f"Processed data is available in: {OUTPUT_DIR}")
    print("============================================")

    print("\nNext steps:")
    print("1. Open QGIS and create a new project")
    print("2. Add the processed data layers from the processed_data folder")
    print("3. Style the layers according to your requirements")
    print("4. Save the QGIS project in your main project folder")


# This line makes the script run when you execute it
if __name__ == "__main__":
    main()