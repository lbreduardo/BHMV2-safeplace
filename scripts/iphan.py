import requests
import geopandas as gpd
import pandas as pd
import time
import json
import os
import xml.etree.ElementTree as ET
from urllib.parse import urlencode, unquote
import logging
from typing import Dict, List, Optional
import warnings
from shapely.geometry import Point
from datetime import datetime

warnings.filterwarnings('ignore')

# ADD DEBUG PRINT
print("Starting IPHAN script...", flush=True)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class IPHANDataExtractor:
    def __init__(self):
        self.base_url = "http://portal.iphan.gov.br/geoserver/wfs"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

        # Brazilian states codes
        self.states = {
            'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapá', 'AM': 'Amazonas',
            'BA': 'Bahia', 'CE': 'Ceará', 'DF': 'Distrito Federal', 'ES': 'Espírito Santo',
            'GO': 'Goiás', 'MA': 'Maranhão', 'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul',
            'MG': 'Minas Gerais', 'PA': 'Pará', 'PB': 'Paraíba', 'PR': 'Paraná',
            'PE': 'Pernambuco', 'PI': 'Piauí', 'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte',
            'RS': 'Rio Grande do Sul', 'RO': 'Rondônia', 'RR': 'Roraima', 'SC': 'Santa Catarina',
            'SP': 'São Paulo', 'SE': 'Sergipe', 'TO': 'Tocantins'
        }

        # ===== MODIFIED: Better directory structure for client =====
        # Get the parent directory of the script (project root)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)  # Go up one level from scripts folder
        self.output_dir = os.path.join(project_root, "project_data", "iphan")
        os.makedirs(self.output_dir, exist_ok=True)

        # Create subdirectories for organization
        os.makedirs(f"{self.output_dir}/raw_data", exist_ok=True)
        os.makedirs(f"{self.output_dir}/filtered_data", exist_ok=True)
        os.makedirs(f"{self.output_dir}/final_output", exist_ok=True)
        os.makedirs(f"{self.output_dir}/edited", exist_ok=True)  # New directory for edited files

        # ===== ADDED: Client-specific requirements =====
        # Fields client specifically wants
        self.required_fields = [
            'identificacao_bem',
            'ds_natureza',
            'ds_tipo_protecao',
            'sintese_bem'
        ]

        # Target layer for client (from their feedback)
        self.target_layer = "Proteção Bens Materiais"  # Client mentioned this specifically

        # Available layers (will be discovered)
        self.available_layers = []

        # Client-ready data for final integration
        self.client_ready_data = []

    def discover_layers(self) -> List[str]:
        """Discover available layers from WFS capabilities"""
        try:
            params = {
                'service': 'WFS',
                'version': '2.0.0',
                'request': 'GetCapabilities'
            }

            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()

            # Parse XML to find layer names
            root = ET.fromstring(response.content)

            # Find all FeatureType elements
            namespaces = {
                'wfs': 'http://www.opengis.net/wfs/2.0',
                'ows': 'http://www.opengis.net/ows/1.1'
            }

            layers = []
            for feature_type in root.findall('.//wfs:FeatureType', namespaces):
                name_elem = feature_type.find('wfs:Name', namespaces)
                title_elem = feature_type.find('wfs:Title', namespaces)

                if name_elem is not None:
                    layer_name = name_elem.text
                    layer_title = title_elem.text if title_elem is not None else layer_name

                    # ===== MODIFIED: Priority to client's target layer =====
                    if any(keyword in layer_name.lower() or keyword in layer_title.lower()
                           for keyword in ['bem', 'material', 'patrimonio', 'protecao', 'tombamento']):
                        layers.append({
                            'name': layer_name,
                            'title': layer_title,
                            'priority': 'protecao' in layer_name.lower() or 'protecao' in layer_title.lower()
                        })
                        logger.info(f"Found layer: {layer_name} - {layer_title}")

            # Save capabilities for reference
            with open(os.path.join(f"{self.output_dir}/raw_data", 'capabilities.xml'), 'w', encoding='utf-8') as f:
                f.write(response.text)

            # Sort layers by priority (client's target first)
            layers.sort(key=lambda x: x['priority'], reverse=True)

            self.available_layers = layers
            logger.info(f"Discovered {len(layers)} relevant layers")
            return layers

        except Exception as e:
            logger.error(f"Error discovering layers: {e}")
            return []

    def test_layer_access(self, layer_name: str) -> bool:
        """Test if we can access a layer by getting a small sample"""
        try:
            params = {
                'service': 'WFS',
                'version': '2.0.0',
                'request': 'GetFeature',
                'typename': layer_name,
                'outputFormat': 'application/json',
                'maxFeatures': '1'
            }

            response = self.session.get(self.base_url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if 'features' in data:
                    logger.info(f"✅ Layer {layer_name} is accessible")
                    return True

            logger.warning(f"❌ Layer {layer_name} returned status {response.status_code}")
            return False

        except Exception as e:
            logger.error(f"❌ Error testing layer {layer_name}: {e}")
            return False

    def get_layer_data(self, layer_name: str, max_features: int = 10000) -> Optional[gpd.GeoDataFrame]:
        """Get all data for a specific layer"""
        try:
            params = {
                'service': 'WFS',
                'version': '2.0.0',
                'request': 'GetFeature',
                'typename': layer_name,
                'outputFormat': 'application/json',
                'srsName': 'EPSG:4326',
                'maxFeatures': str(max_features)
            }

            logger.info(f"Requesting data for layer: {layer_name}")
            response = self.session.get(self.base_url, params=params, timeout=120)
            response.raise_for_status()

            # Parse JSON response
            data = response.json()

            if 'features' in data and len(data['features']) > 0:
                gdf = gpd.GeoDataFrame.from_features(data['features'])
                gdf.crs = 'EPSG:4326'

                logger.info(f"✅ Retrieved {len(gdf)} features for {layer_name}")

                # Print column names for debugging
                logger.info(f"Columns in {layer_name}: {list(gdf.columns)}")

                return gdf
            else:
                logger.warning(f"No features found for {layer_name}")
                return None

        except Exception as e:
            logger.error(f"Error retrieving data for {layer_name}: {e}")
            return None

    # ===== ADDED: Client-specific data filtering =====
    def filter_client_data(self, gdf: gpd.GeoDataFrame, layer_name: str) -> Optional[gpd.GeoDataFrame]:
        """Filter data according to client requirements"""
        if gdf is None or len(gdf) == 0:
            return None

        logger.info(f"🔍 Filtering data for client requirements...")

        # Check available columns
        available_columns = list(gdf.columns)
        logger.info(f"Available columns: {available_columns}")

        # Find matching columns (handle case variations)
        matched_fields = {}
        for required_field in self.required_fields:
            # Try exact match first
            if required_field in available_columns:
                matched_fields[required_field] = required_field
            else:
                # Try case-insensitive match
                for col in available_columns:
                    if required_field.lower() == col.lower():
                        matched_fields[required_field] = col
                        break

        # Add geometry column
        if 'geometry' in available_columns:
            matched_fields['geometry'] = 'geometry'

        logger.info(f"Matched fields: {matched_fields}")

        if len(matched_fields) > 1:  # At least one data field + geometry
            # Select only the matched columns
            columns_to_keep = list(matched_fields.values())
            filtered_gdf = gdf[columns_to_keep].copy()

            # Rename columns to standard names
            rename_dict = {v: k for k, v in matched_fields.items()}
            filtered_gdf.rename(columns=rename_dict, inplace=True)

            logger.info(f"✅ Filtered data: {len(filtered_gdf)} records with {len(columns_to_keep)} columns")
            return filtered_gdf
        else:
            logger.warning(f"❌ Required fields not found in {layer_name}")
            return None

    def check_ds_natureza_values(self, gdf: gpd.GeoDataFrame) -> Dict:
        """Check ds_natureza field values as requested by client"""
        if 'ds_natureza' not in gdf.columns:
            return {'error': 'ds_natureza field not found'}

        # Get unique values
        unique_values = gdf['ds_natureza'].value_counts().to_dict()

        # Check for client's target values
        target_values = ['Bem Imóvel', 'Bem Móvel ou Integrado']
        found_values = {val: val in unique_values for val in target_values}

        return {
            'unique_values': unique_values,
            'target_values_found': found_values,
            'total_records': len(gdf)
        }

    def save_data(self, gdf: gpd.GeoDataFrame, filename: str, subfolder: str = "raw_data"):
        """Save GeoDataFrame to multiple formats"""
        try:
            output_path = os.path.join(self.output_dir, subfolder)

            # Save as GeoJSON
            geojson_path = os.path.join(output_path, f"{filename}.geojson")
            gdf.to_file(geojson_path, driver='GeoJSON', encoding='utf-8')

            # Save as GPKG
            gpkg_path = os.path.join(output_path, f"{filename}.gpkg")
            gdf.to_file(gpkg_path, driver='GPKG', encoding='utf-8')

            # Save as CSV (for client's reference)
            csv_path = os.path.join(output_path, f"{filename}.csv")
            df = gdf.drop('geometry', axis=1) if 'geometry' in gdf.columns else gdf
            df.to_csv(csv_path, index=False, encoding='utf-8')

            # Save as Shapefile (handle long field names)
            try:
                shp_path = os.path.join(output_path, f"{filename}.shp")
                gdf.to_file(shp_path, driver='ESRI Shapefile', encoding='utf-8')
            except Exception as e:
                logger.warning(f"Could not save shapefile for {filename}: {e}")

            logger.info(f"💾 Saved {len(gdf)} features to {filename}")

        except Exception as e:
            logger.error(f"Error saving {filename}: {e}")

    def filter_by_state(self, gdf: gpd.GeoDataFrame, state_code: str) -> Optional[gpd.GeoDataFrame]:
        """Filter GeoDataFrame by state code"""
        if gdf is None or len(gdf) == 0:
            return None

        # Try different possible state column names
        state_columns = ['uf', 'UF', 'estado', 'Estado', 'sigla_uf', 'SIGLA_UF', 'cod_uf', 'COD_UF']

        for col in state_columns:
            if col in gdf.columns:
                state_data = gdf[gdf[col] == state_code]
                if len(state_data) > 0:
                    logger.info(f"Found {len(state_data)} features for {state_code} using column '{col}'")
                    return state_data

        logger.warning(f"No state column found or no data for {state_code}")
        return None

    # ===== NEW: Coordinate validation function =====
    def validate_coordinates(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Ensure all coordinates are within Brazil's boundaries with buffer"""
        if gdf is None or len(gdf) == 0:
            logger.warning("No data to validate coordinates")
            return gdf

        logger.info("🗺️ Validating coordinates to ensure all points are within Brazil...")

        initial_count = len(gdf)

        # Extract coordinates
        if not isinstance(gdf, gpd.GeoDataFrame):
            logger.warning("Input is not a GeoDataFrame, skipping coordinate validation")
            return gdf

        # Make a copy to avoid modification warnings
        gdf_copy = gdf.copy()

        # Add latitude/longitude columns if they don't exist
        if 'latitude' not in gdf_copy.columns or 'longitude' not in gdf_copy.columns:
            gdf_copy['latitude'] = gdf_copy.geometry.y
            gdf_copy['longitude'] = gdf_copy.geometry.x

        # ULTRA-STRICT Brazil boundary check
        brazil_bounds = {
            'north': 4.5,  # Northern extent
            'south': -33.0,  # Southern extent
            'east': -35.5,  # Eastern extent
            'west': -72.5  # Western extent
        }

        # Apply strict filtering
        valid_gdf = gdf_copy[
            (gdf_copy['latitude'] >= brazil_bounds['south']) &
            (gdf_copy['latitude'] <= brazil_bounds['north']) &
            (gdf_copy['longitude'] >= brazil_bounds['west']) &
            (gdf_copy['longitude'] <= brazil_bounds['east'])
            ].copy()

        # Add border buffer
        border_buffer = 0.5  # Degrees
        valid_gdf = valid_gdf[
            (valid_gdf['latitude'] >= (brazil_bounds['south'] + border_buffer)) &
            (valid_gdf['latitude'] <= (brazil_bounds['north'] - border_buffer)) &
            (valid_gdf['longitude'] >= (brazil_bounds['west'] + border_buffer)) &
            (valid_gdf['longitude'] <= (brazil_bounds['east'] - border_buffer))
            ]

        # Add extra buffer for Amazon region (northwest)
        amazon_buffer = 1.0  # 1 degree buffer for northwest region
        northwest_region = (
                (valid_gdf['latitude'] >= 0) &
                (valid_gdf['longitude'] <= -65)
        )

        # Create a mask that keeps points OUTSIDE the northwest (unchanged)
        # or that are in the northwest AND have the extra buffer
        extra_buffer_mask = ~northwest_region | (
                northwest_region &
                (valid_gdf['longitude'] >= (brazil_bounds['west'] + amazon_buffer))
        )

        # Apply the extra buffer
        valid_gdf = valid_gdf[extra_buffer_mask]

        removed_count = initial_count - len(valid_gdf)

        logger.info(f"Coordinate validation results:")
        logger.info(f"  Original sites: {initial_count}")
        logger.info(f"  Valid sites within Brazil: {len(valid_gdf)}")
        logger.info(f"  Removed sites with invalid coordinates: {removed_count}")

        # Add metadata
        valid_gdf['coordinates_validated'] = True
        valid_gdf['coordinates_removed'] = removed_count

        return valid_gdf

    # ===== NEW: Clustering function =====
    def apply_clustering(self, gdf: gpd.GeoDataFrame, min_distance_km=5) -> gpd.GeoDataFrame:
        """Apply adaptive clustering to reduce dense clusters of heritage sites"""
        logger.info(f"📊 Applying adaptive clustering to reduce point density...")

        if gdf is None or len(gdf) <= 1:
            logger.warning("Not enough data for clustering")
            return gdf

        try:
            # Make a copy to avoid modification warnings
            gdf_copy = gdf.copy()

            # Project to a metric CRS for accurate distance calculation
            gdf_projected = gdf_copy.to_crs('EPSG:5880')  # SIRGAS 2000 / Brazil Polyconic

            # Convert min_distance_km to meters
            min_distance = min_distance_km * 1000

            # Identify dense areas (more than 10 points within 5km)
            dense_areas = []
            for idx, point in enumerate(gdf_projected.geometry):
                nearby = gdf_projected[gdf_projected.geometry.distance(point) <= min_distance]
                if len(nearby) > 10:  # If more than 10 points in 5km radius
                    dense_areas.append({
                        'center_idx': idx,
                        'point_count': len(nearby),
                        'center': point
                    })

            # Sort dense areas by point count (most dense first)
            dense_areas.sort(key=lambda x: x['point_count'], reverse=True)

            # Create representative points for dense areas
            processed_indices = set()
            representative_points = []

            # Process dense areas
            for area in dense_areas:
                if area['center_idx'] in processed_indices:
                    continue

                center = area['center']
                nearby = gdf_projected[gdf_projected.geometry.distance(center) <= min_distance]

                # Skip if all points in this area have been processed
                if all(idx in processed_indices for idx in nearby.index):
                    continue

                # Mark these points as processed
                for idx in nearby.index:
                    processed_indices.add(idx)

                # Use representative point with aggregated info
                rep_point = gdf_copy.loc[area['center_idx']].copy()
                rep_point['site_count'] = len(nearby)

                # Get site names if available
                name_field = 'identificacao_bem' if 'identificacao_bem' in gdf_copy.columns else 'nome'
                if name_field in nearby.columns:
                    site_names = nearby[name_field].astype(str).fillna('Unnamed site').tolist()
                    if len(site_names) > 3:
                        rep_point['site_names'] = ', '.join(site_names[:3]) + f" (+ {len(site_names) - 3} more)"
                    else:
                        rep_point['site_names'] = ', '.join(site_names)
                else:
                    rep_point['site_names'] = f"Cluster of {len(nearby)} heritage sites"

                rep_point['is_cluster'] = True
                representative_points.append(rep_point)

            # Add remaining individual points
            for idx, row in gdf_copy.iterrows():
                if idx not in processed_indices:
                    row_copy = row.copy()
                    row_copy['site_count'] = 1
                    name_field = 'identificacao_bem' if 'identificacao_bem' in row else 'nome'
                    if name_field in row and pd.notna(row[name_field]):
                        row_copy['site_names'] = row[name_field]
                    else:
                        row_copy['site_names'] = "Unnamed heritage site"
                    row_copy['is_cluster'] = False
                    representative_points.append(row_copy)

            # Create new GeoDataFrame
            if representative_points:
                result_gdf = gpd.GeoDataFrame(representative_points, crs=gdf.crs)

                logger.info(f"Clustering results:")
                logger.info(f"  Original points: {len(gdf)}")
                logger.info(f"  After clustering: {len(result_gdf)}")
                logger.info(f"  Clusters created: {sum(result_gdf['is_cluster'])}")
                logger.info(f"  Individual points: {sum(~result_gdf['is_cluster'])}")

                return result_gdf
            else:
                return gdf_copy

        except Exception as e:
            logger.error(f"Error in clustering: {str(e)}")
            return gdf

    # ===== NEW: Check for edited files =====
    def check_for_edited_file(self) -> Optional[gpd.GeoDataFrame]:
        """Check if a manually edited file exists and use it instead of collecting new data"""
        edited_file = os.path.join(self.output_dir, "edited", "iphan_heritage_sites_edited.geojson")

        # ADD DEBUG PRINTS
        print(f"Checking for edited file at: {edited_file}", flush=True)
        print(f"File exists: {os.path.exists(edited_file)}", flush=True)

        if os.path.exists(edited_file):
            # ADD DEBUG PRINT
            print("⚠️ Found manually edited data file. Using this instead of collecting new data.", flush=True)

            logger.info("⚠️ Found manually edited data file. Using this instead of collecting new data.")
            logger.info(f"Using: {edited_file}")

            try:
                # Load the edited GeoJSON
                edited_gdf = gpd.read_file(edited_file)

                # Save a copy to the final_output directory
                final_output = os.path.join(self.output_dir, "final_output", "iphan_heritage_sites.geojson")
                edited_gdf.to_file(final_output, driver='GeoJSON')

                logger.info(f"✅ Successfully loaded and used manually edited data!")
                logger.info(f"Total heritage sites: {len(edited_gdf)}")
                logger.info(f"Data available at: {final_output}")

                return edited_gdf

            except Exception as e:
                logger.error(f"❌ Error loading edited file: {str(e)}")
                logger.info("Continuing with normal data collection...")

        return None

    def create_integrated_dataset(self) -> Optional[gpd.GeoDataFrame]:
        """Create an integrated dataset from all client-ready data"""
        logger.info("🔄 Creating integrated IPHAN heritage dataset...")

        if not self.client_ready_data:
            logger.warning("No client-ready data to integrate")
            return None

        all_gdfs = []
        for data_info in self.client_ready_data:
            all_gdfs.append(data_info['gdf'])

        if not all_gdfs:
            logger.warning("No GeoDataFrames to integrate")
            return None

        # Combine all data
        integrated_gdf = pd.concat(all_gdfs, ignore_index=True)
        logger.info(f"📊 Created integrated dataset with {len(integrated_gdf)} features")

        # Apply final validation and clustering
        integrated_gdf = self.validate_coordinates(integrated_gdf)
        integrated_gdf = self.apply_clustering(integrated_gdf, min_distance_km=5)

        # Save the integrated dataset
        output_file = os.path.join(self.output_dir, "final_output", "iphan_heritage_sites.geojson")
        integrated_gdf.to_file(output_file, driver='GeoJSON')
        logger.info(f"💾 Saved integrated dataset to {output_file}")

        # Create edited directory for future manual edits
        edited_dir = os.path.join(self.output_dir, "edited")
        os.makedirs(edited_dir, exist_ok=True)

        # Add instructions file for manual editing
        instructions_file = os.path.join(edited_dir, "EDITING_INSTRUCTIONS.txt")
        with open(instructions_file, 'w') as f:
            f.write("""
=======================================================
IPHAN HERITAGE DATA - MANUAL EDITING INSTRUCTIONS
=======================================================

To preserve your manual edits when rerunning the data collection:

1. Open the main integrated GeoJSON file in QGIS:
   {main_file}

2. Make your edits:
   - Remove unwanted points outside Brazil borders
   - Adjust positions if needed
   - Add additional information

3. Save the edited layer to this location:
   {edited_file}

4. Next time you run this script, it will automatically
   detect this edited file and use it instead of collecting
   new data from IPHAN.

IMPORTANT NOTES:
- The script applied coordinate validation to keep points within
  Brazil's borders with a safety buffer
- Dense clusters were reduced using adaptive clustering
- These automated steps may not catch all issues, so manual
  review is still recommended

=======================================================
""".format(
                main_file=output_file,
                edited_file=os.path.join(edited_dir, "iphan_heritage_sites_edited.geojson")
            ))

        return integrated_gdf

    def extract_all_data(self):
        """Main extraction process - MODIFIED for client requirements with coordinate validation and clustering"""
        logger.info("🚀 Starting IPHAN data extraction for client project")

        # Check for edited file first
        edited_data = self.check_for_edited_file()
        if edited_data is not None:
            logger.info("✅ Using manually edited data instead of collecting new data")
            return True

        # Step 1: Discover available layers
        layers = self.discover_layers()

        if not layers:
            logger.error("No layers found! Check the GeoServer URL and connectivity.")
            return

        # Step 2: Test layer accessibility
        accessible_layers = []
        for layer in layers:
            if self.test_layer_access(layer['name']):
                accessible_layers.append(layer)

        if not accessible_layers:
            logger.error("No accessible layers found!")
            return

        logger.info(f"Found {len(accessible_layers)} accessible layers")

        # Step 3: Extract data with focus on client requirements
        extraction_summary = {
            'extraction_date': pd.Timestamp.now().isoformat(),
            'client_requirements': {
                'target_layer': self.target_layer,
                'required_fields': self.required_fields
            },
            'accessible_layers': len(accessible_layers),
            'data_summary': {}
        }

        client_data_found = False

        for layer in accessible_layers:
            layer_name = layer['name']
            layer_title = layer['title']

            logger.info(f"📊 Processing: {layer_name}")

            # Get complete dataset
            gdf = self.get_layer_data(layer_name)

            if gdf is not None and len(gdf) > 0:
                # Clean layer name for filename
                clean_name = layer_name.replace(':', '_').replace(' ', '_').replace('-', '_')

                # Save raw data
                self.save_data(gdf, f"raw_{clean_name}", "raw_data")

                # Apply client filtering
                filtered_gdf = self.filter_client_data(gdf, layer_name)

                if filtered_gdf is not None:
                    # Save filtered data
                    self.save_data(filtered_gdf, f"filtered_{clean_name}", "filtered_data")

                    # Check ds_natureza values if present
                    ds_natureza_analysis = None
                    if 'ds_natureza' in filtered_gdf.columns:
                        ds_natureza_analysis = self.check_ds_natureza_values(filtered_gdf)

                        # ===== NEW: Apply coordinate validation and clustering =====
                        validated_gdf = self.validate_coordinates(filtered_gdf)
                        clustered_gdf = self.apply_clustering(validated_gdf, min_distance_km=5)

                        # Save validated and clustered data with a new prefix
                        self.save_data(clustered_gdf, f"optimized_{clean_name}", "final_output")

                        # Also save as client_ready to maintain compatibility with existing code
                        self.save_data(clustered_gdf, f"client_ready_{clean_name}", "final_output")

                        # Add to client-ready data for later integration
                        self.client_ready_data.append({
                            'layer': layer_name,
                            'gdf': clustered_gdf
                        })
                        client_data_found = True

                        # Log ds_natureza analysis
                        logger.info(f"📋 ds_natureza analysis for {layer_name}:")
                        logger.info(f"   Unique values: {ds_natureza_analysis['unique_values']}")
                        logger.info(f"   Target values found: {ds_natureza_analysis['target_values_found']}")

                        extraction_summary['data_summary'][layer_name] = {
                            'total_features': len(gdf),
                            'filtered_features': len(filtered_gdf),
                            'validated_features': len(validated_gdf),
                            'clustered_features': len(clustered_gdf),
                            'ds_natureza_analysis': ds_natureza_analysis,
                            'client_ready': True
                        }
                    else:
                        # Just save the filtered data as client ready (no ds_natureza)
                        self.save_data(filtered_gdf, f"client_ready_{clean_name}", "final_output")
                        extraction_summary['data_summary'][layer_name] = {
                            'total_features': len(gdf),
                            'filtered_features': len(filtered_gdf),
                            'ds_natureza_analysis': None,
                            'client_ready': True
                        }
                        client_data_found = True
                else:
                    extraction_summary['data_summary'][layer_name] = {
                        'total_features': len(gdf),
                        'filtered_features': 0,
                        'ds_natureza_analysis': None,
                        'client_ready': False
                    }

        # ===== NEW: Create integrated dataset =====
        if self.client_ready_data:
            integrated_gdf = self.create_integrated_dataset()
            if integrated_gdf is not None:
                extraction_summary['integrated_dataset'] = {
                    'total_features': len(integrated_gdf),
                    'clusters': sum(integrated_gdf['is_cluster']) if 'is_cluster' in integrated_gdf.columns else 0,
                    'individual_points': sum(
                        ~integrated_gdf['is_cluster']) if 'is_cluster' in integrated_gdf.columns else len(
                        integrated_gdf)
                }

        # Save comprehensive summary
        with open(os.path.join(self.output_dir, 'client_extraction_summary.json'), 'w', encoding='utf-8') as f:
            json.dump(extraction_summary, f, indent=2, ensure_ascii=False)

        self.print_client_summary(extraction_summary, client_data_found)

    def print_client_summary(self, summary: Dict, client_data_found: bool):
        """Print client-focused extraction summary"""
        print("\n" + "=" * 70)
        print("🏛️  IPHAN DATA EXTRACTION - CLIENT PROJECT SUMMARY")
        print("=" * 70)

        print(f"📋 CLIENT REQUIREMENTS:")
        print(f"   Target layer: {summary['client_requirements']['target_layer']}")
        print(f"   Required fields: {', '.join(summary['client_requirements']['required_fields'])}")
        print()

        total_features = 0
        filtered_features = 0
        validated_features = 0
        clustered_features = 0
        client_ready_layers = 0

        for layer_name, info in summary['data_summary'].items():
            features = info['total_features']
            filtered = info.get('filtered_features', 0)
            validated = info.get('validated_features', filtered)
            clustered = info.get('clustered_features', validated)
            client_ready = info['client_ready']

            total_features += features
            filtered_features += filtered
            validated_features += validated
            clustered_features += clustered
            if client_ready:
                client_ready_layers += 1

            print(f"📊 {layer_name}")
            print(f"   Raw features: {features}")
            print(f"   Filtered features: {filtered}")
            if 'validated_features' in info:
                print(f"   Validated features: {validated}")
            if 'clustered_features' in info:
                print(f"   Clustered features: {clustered}")
            print(f"   Client ready: {'✅' if client_ready else '❌'}")

            if info['ds_natureza_analysis']:
                analysis = info['ds_natureza_analysis']
                print(f"   ds_natureza values: {len(analysis['unique_values'])} unique")
                target_found = analysis['target_values_found']
                print(
                    f"   Target values: Bem Imóvel={target_found.get('Bem Imóvel', False)}, Bem Móvel={target_found.get('Bem Móvel ou Integrado', False)}")
            print()

        print(f"📈 FINAL RESULTS:")
        print(f"   Total raw features: {total_features}")
        print(f"   Filtered features: {filtered_features}")
        if validated_features > 0:
            print(f"   Validated features (within Brazil): {validated_features}")
        if clustered_features > 0:
            print(f"   Clustered features (reduced density): {clustered_features}")
        print(f"   Client-ready layers: {client_ready_layers}")

        if 'integrated_dataset' in summary:
            integrated = summary['integrated_dataset']
            print(f"   Integrated dataset: {integrated['total_features']} features")
            print(f"   - Clusters: {integrated.get('clusters', 0)}")
            print(f"   - Individual points: {integrated.get('individual_points', integrated['total_features'])}")

        print(f"   Data ready for client: {'✅ YES' if client_data_found else '❌ NO'}")
        print("=" * 70)

    def list_files(self):
        """List all created files organized by purpose"""
        print(f"\n📁 CREATED FILES:")

        for subfolder in ['raw_data', 'filtered_data', 'final_output', 'edited']:
            path = os.path.join(self.output_dir, subfolder)
            if os.path.exists(path):
                files = [f for f in os.listdir(path) if f.endswith(('.geojson', '.gpkg', '.shp', '.csv'))]
                if files:
                    print(f"\n   📂 {subfolder.upper()}:")
                    for file in sorted(files):
                        print(f"      {file}")

    def create_editing_instructions(self):
        """Create instructions for manual editing in QGIS"""
        edited_dir = os.path.join(self.output_dir, "edited")
        os.makedirs(edited_dir, exist_ok=True)

        main_file = os.path.join(self.output_dir, "final_output", "iphan_heritage_sites.geojson")
        edited_file = os.path.join(edited_dir, "iphan_heritage_sites_edited.geojson")

        instructions = f"""
=======================================================
IPHAN HERITAGE DATA - MANUAL EDITING INSTRUCTIONS
=======================================================

To preserve your manual edits when rerunning the data collection:

1. Open the main integrated GeoJSON file in QGIS:
   {main_file}

2. Make your edits:
   - Remove unwanted points outside Brazil borders
   - Adjust positions if needed
   - Add additional information

3. Save the edited layer to this location:
   {edited_file}

4. Next time you run this script, it will automatically
   detect this edited file and use it instead of collecting
   new data from IPHAN.

IMPORTANT NOTES:
- The script applied coordinate validation to keep points within
  Brazil's borders with a safety buffer
- Dense clusters were reduced using adaptive clustering
- These automated steps may not catch all issues, so manual
  review is still recommended

=======================================================
"""

        instructions_file = os.path.join(edited_dir, "EDITING_INSTRUCTIONS.txt")
        with open(instructions_file, 'w', encoding='utf-8') as f:
            f.write(instructions)

        logger.info(f"📝 Created editing instructions at {instructions_file}")


def main():
    """Main execution function"""
    # ADD DEBUG PRINT
    print("Main function starting...", flush=True)

    extractor = IPHANDataExtractor()

    try:
        extractor.extract_all_data()
        extractor.list_files()
        extractor.create_editing_instructions()

        print(f"\n✅ IPHAN data extraction completed for client project!")
        print(f"📂 Check the '{extractor.output_dir}' directory:")
        print(f"   • raw_data/ - Original downloaded data")
        print(f"   • filtered_data/ - Data with client's required fields")
        print(f"   • final_output/ - Client-ready data files with validation and clustering")
        print(f"   • edited/ - Place for manually edited files (see EDITING_INSTRUCTIONS.txt)")

        # Display path to integrated dataset if it exists
        integrated_path = os.path.join(extractor.output_dir, "final_output", "iphan_heritage_sites.geojson")
        if os.path.exists(integrated_path):
            print(f"\n🌟 INTEGRATED DATASET:")
            print(f"   {integrated_path}")

        print(f"\n💡 TIP: If you need to manually adjust points in QGIS, save your edits to:")
        print(f"   {os.path.join(extractor.output_dir, 'edited', 'iphan_heritage_sites_edited.geojson')}")
        print(f"   The script will use your edited file next time instead of fetching new data.")

    except KeyboardInterrupt:
        logger.info("Extraction interrupted by user")
        print("\n⚠️  Extraction interrupted")

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        print(f"\n❌ Extraction failed: {e}")


# ADD DEBUG PRINT
print("About to check if __name__ == '__main__'", flush=True)

if __name__ == "__main__":
    # ADD DEBUG PRINT
    print("Script is running as main", flush=True)
    main()
    # ADD DEBUG PRINT
    print("Script completed", flush=True)