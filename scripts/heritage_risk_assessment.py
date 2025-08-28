import requests
import geopandas as gpd
import pandas as pd
import json
import os
import time
import zipfile
import io
from datetime import datetime
import logging
from typing import Dict, List, Optional, Tuple
import warnings
from shapely.geometry import Point
import numpy as np
from scipy.spatial.distance import cdist

warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

print("🏛️ Starting COMPLETE Heritage Risk Assessment Pipeline (ALL 4 DATA SOURCES)...", flush=True)


class EnhancedHeritageRiskAssessment:
    def __init__(self):
        # Get project structure
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.project_root = os.path.dirname(script_dir)

        # Main directories
        # Clean directory structure following your pipeline methodology
        self.ibge_dir = os.path.join(self.project_root, "project_data", "ibge_boundaries")
        self.cemaden_dir = os.path.join(self.project_root, "project_data", "cemaden")
        self.iphan_dir = os.path.join(self.project_root, "project_data", "iphan")
        self.snisb_dir = os.path.join(self.project_root, "project_data", "snisb_data")
        self.inpe_dir = os.path.join(self.project_root, "project_data", "inpe_data")
        self.output_dir = os.path.join(self.project_root, "project_data", "comprehensive_assessment")

        # Create directories
        for directory in [self.ibge_dir, self.output_dir]:
            os.makedirs(directory, exist_ok=True)

        # Enhanced risk scoring system
        self.risk_scores = {
            'Muito Alto': 5,
            'Alto': 4,
            'Moderado': 3,
            'Baixo': 2,
            'Muito Baixo': 1
        }

        # Risk types weights (enhanced for all 4 sources)
        self.risk_weights = {
            'Risco Hidrológico': 1.2,  # CEMADEN - Flood risks
            'Risco Geológico': 1.5,  # CEMADEN - Landslide risks
            'Mov. Massa': 1.5,  # CEMADEN - Landslide alternative
            'Risco Hidro': 1.2,  # CEMADEN - Flood alternative
            'Fire Risk': 1.3,  # INPE - Fire risks
            'Dam Proximity': 1.4  # SNISB - Dam proximity risks
        }

        logger.info(f"Initialized Enhanced Heritage Risk Assessment")
        logger.info(f"Working directory: {self.output_dir}")

    def check_all_data_sources(self) -> Dict[str, bool]:
        """Check all 4 data sources"""
        status = {}

        # Check CEMADEN
        cemaden_file = os.path.join(self.cemaden_dir, "real_time_cemaden_data.json")
        status['cemaden'] = self._check_file_exists(cemaden_file, "CEMADEN disaster alerts")

        # Check IPHAN
        iphan_file = os.path.join(self.iphan_dir, "final_output", "iphan_heritage_sites.geojson")
        status['iphan'] = self._check_file_exists(iphan_file, "IPHAN heritage sites")

        # Check SNISB
        snisb_geojson_dir = os.path.join(self.snisb_dir, "GeoJSON")
        if os.path.exists(snisb_geojson_dir):
            snisb_files = [f for f in os.listdir(snisb_geojson_dir) if f.endswith('.geojson')]
            if snisb_files:
                logger.info(f"✅ Found SNISB dam data: {len(snisb_files)} files")
                status['snisb'] = True
            else:
                logger.warning("❌ SNISB GeoJSON directory exists but no files found")
                status['snisb'] = False
        else:
            logger.warning(f"❌ SNISB data not found at: {snisb_geojson_dir}")
            status['snisb'] = False

        # Check INPE
        inpe_geojson_dir = os.path.join(self.inpe_dir, "geojson")
        if os.path.exists(inpe_geojson_dir):
            inpe_files = [f for f in os.listdir(inpe_geojson_dir) if f.endswith('.geojson')]
            if inpe_files:
                logger.info(f"✅ Found INPE fire data: {len(inpe_files)} files")
                status['inpe'] = True
            else:
                logger.warning("❌ INPE GeoJSON directory exists but no files found")
                status['inpe'] = False
        else:
            logger.warning(f"❌ INPE data not found at: {inpe_geojson_dir}")
            status['inpe'] = False

        return status

    def _check_file_exists(self, file_path: str, description: str) -> bool:
        """Helper to check if file exists and has data"""
        if os.path.exists(file_path):
            try:
                if file_path.endswith('.json'):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    if data and len(data) > 0:
                        logger.info(f"✅ Found {description}: {len(data)} records")
                        return True
                elif file_path.endswith('.geojson'):
                    gdf = gpd.read_file(file_path)
                    if len(gdf) > 0:
                        logger.info(f"✅ Found {description}: {len(gdf)} features")
                        return True
                logger.warning(f"❌ {description} file exists but is empty")
                return False
            except Exception as e:
                logger.error(f"❌ Error reading {description}: {str(e)}")
                return False
        else:
            logger.warning(f"❌ {description} not found at: {file_path}")
            return False

    def load_snisb_dam_data(self) -> gpd.GeoDataFrame:
        """Load SNISB dam safety data"""
        logger.info("🏗️ Loading SNISB dam safety data...")

        snisb_geojson_dir = os.path.join(self.snisb_dir, "GeoJSON")
        dam_gdfs = []

        try:
            # Load all GeoJSON files in SNISB directory
            for file in os.listdir(snisb_geojson_dir):
                if file.endswith('.geojson'):
                    file_path = os.path.join(snisb_geojson_dir, file)
                    print(f"🔍 Loading SNISB file: {file}")
                    try:
                        gdf = gpd.read_file(file_path)
                        print(f"✅ Successfully loaded: {file} ({len(gdf)} features)")
                    except Exception as e:
                        print(f"❌ CORRUPTED FILE FOUND: {file}")
                        print(f"❌ Error: {str(e)}")
                        continue

                    # Ensure CRS is WGS84
                    if gdf.crs != 'EPSG:4326':
                        gdf = gdf.to_crs('EPSG:4326')

                    dam_gdfs.append(gdf)
                    logger.info(f"   📂 Loaded {file}: {len(gdf)} features")

            if dam_gdfs:
                # Combine all dam data
                all_dams = gpd.GeoDataFrame(pd.concat(dam_gdfs, ignore_index=True))

                # Add risk information
                all_dams['risk_type'] = 'Dam Proximity'
                all_dams['risk_level'] = 'Moderado'  # Default for dam proximity

                logger.info(f"🏗️ Total SNISB dam features loaded: {len(all_dams)}")
                return all_dams
            else:
                logger.warning("❌ No SNISB dam data found")
                return gpd.GeoDataFrame()

        except Exception as e:
            logger.error(f"❌ Error loading SNISB data: {str(e)}")
            return gpd.GeoDataFrame()

    def load_inpe_fire_data(self) -> gpd.GeoDataFrame:
        """Load INPE fire risk data"""
        logger.info("🔥 Loading INPE fire risk data...")

        inpe_geojson_dir = os.path.join(self.inpe_dir, "geojson")
        fire_gdfs = []

        try:
            # Load all GeoJSON files in INPE directory
            for file in os.listdir(inpe_geojson_dir):
                if file.endswith('.geojson'):
                    file_path = os.path.join(inpe_geojson_dir, file)
                    print(f"🔍 Loading INPE file: {file}")
                    try:
                        gdf = gpd.read_file(file_path)
                        print(f"✅ Successfully loaded: {file} ({len(gdf)} features)")
                    except Exception as e:
                        print(f"❌ CORRUPTED FILE FOUND: {file}")
                        print(f"❌ Error: {str(e)}")
                        continue

                    # Ensure CRS is WGS84
                    if gdf.crs != 'EPSG:4326':
                        gdf = gdf.to_crs('EPSG:4326')

                    fire_gdfs.append(gdf)
                    logger.info(f"   📂 Loaded {file}: {len(gdf)} features")

            if fire_gdfs:
                # Combine all fire data
                all_fires = gpd.GeoDataFrame(pd.concat(fire_gdfs, ignore_index=True))

                # Add risk information
                all_fires['risk_type'] = 'Fire Risk'
                # Map fire risk levels if available, otherwise default
                if 'risk_level' not in all_fires.columns:
                    all_fires['risk_level'] = 'Moderado'  # Default fire risk level

                logger.info(f"🔥 Total INPE fire features loaded: {len(all_fires)}")
                return all_fires
            else:
                logger.warning("❌ No INPE fire data found")
                return gpd.GeoDataFrame()

        except Exception as e:
            logger.error(f"❌ Error loading INPE data: {str(e)}")
            return gpd.GeoDataFrame()

    def download_ibge_boundaries(self) -> bool:
        """Load IBGE municipality boundaries from local data"""
        logger.info("📍 Loading IBGE municipality boundaries...")

        try:
            # Check if already processed
            boundaries_file = os.path.join(self.ibge_dir, "municipalities.geojson")
            if os.path.exists(boundaries_file):
                logger.info("✅ IBGE boundaries already exist")
                return True

            # Look for manually extracted shapefile
            extract_path = os.path.join(self.ibge_dir, "temp")
            logger.info(f"🔍 Looking for extracted data in: {extract_path}")

            if not os.path.exists(extract_path):
                logger.error("❌ No extracted IBGE data found. Please extract the ZIP file to ibge_boundaries/temp/")
                return False

            # Find the shapefile
            shp_files = []
            for root, dirs, files in os.walk(extract_path):
                shp_files.extend([os.path.join(root, f) for f in files if f.endswith('.shp')])

            if not shp_files:
                logger.error("❌ No shapefile found in extracted data")
                return False

            shp_path = shp_files[0]  # Use first shapefile found
            logger.info(f"📂 Processing shapefile: {os.path.basename(shp_path)}")

            gdf = gpd.read_file(shp_path)

            # Ensure CRS is WGS84
            if gdf.crs != 'EPSG:4326':
                gdf = gdf.to_crs('EPSG:4326')

            # Clean up column names
            if 'NM_MUN' in gdf.columns:
                gdf['municipality'] = gdf['NM_MUN'].str.title()
            if 'SIGLA_UF' in gdf.columns:
                gdf['uf'] = gdf['SIGLA_UF'].str.upper()

            # Save as GeoJSON
            gdf.to_file(boundaries_file, driver='GeoJSON')
            logger.info(f"✅ Processed {len(gdf)} municipality boundaries")
            return True

        except Exception as e:
            logger.error(f"❌ Error processing IBGE boundaries: {str(e)}")
            return False

    def load_all_data(self) -> Tuple[
        gpd.GeoDataFrame, pd.DataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """Load all 4 data sources"""
        logger.info("📂 Loading ALL data sources...")

        # Load IPHAN heritage sites
        iphan_file = os.path.join(self.iphan_dir, "final_output", "iphan_heritage_sites.geojson")
        heritage_gdf = gpd.read_file(iphan_file)
        logger.info(f"📍 Loaded {len(heritage_gdf)} heritage sites")

        # Load CEMADEN disaster data
        cemaden_file = os.path.join(self.cemaden_dir, "real_time_cemaden_data.json")
        with open(cemaden_file, 'r', encoding='utf-8') as f:
            cemaden_data = json.load(f)
        disaster_df = pd.DataFrame(cemaden_data)
        logger.info(f"⚠️ Loaded {len(disaster_df)} disaster alerts")

        # Load IBGE boundaries
        boundaries_file = os.path.join(self.ibge_dir, "municipalities.geojson")
        boundaries_gdf = gpd.read_file(boundaries_file)
        logger.info(f"🗺️ Loaded {len(boundaries_gdf)} municipality boundaries")

        # Load SNISB dam data
        dam_gdf = self.load_snisb_dam_data()

        # Load INPE fire data
        fire_gdf = self.load_inpe_fire_data()

        return heritage_gdf, disaster_df, boundaries_gdf, dam_gdf, fire_gdf

    def calculate_comprehensive_risk(self, heritage_gdf: gpd.GeoDataFrame,
                                     disaster_df: pd.DataFrame,
                                     dam_gdf: gpd.GeoDataFrame,
                                     fire_gdf: gpd.GeoDataFrame,
                                     boundaries_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Calculate comprehensive risk from all 4 data sources"""
        logger.info("🔄 Calculating comprehensive risk assessment...")

        # Step 1: Spatial join heritage sites with municipalities
        heritage_with_admin = self._spatial_join_heritage_municipalities(heritage_gdf, boundaries_gdf)

        # Step 2: Calculate CEMADEN risks (municipal level)
        municipal_cemaden_risk = self._calculate_municipal_cemaden_risk(disaster_df)

        # Step 3: Calculate dam proximity risks (site level)
        heritage_with_dam_risk = self._calculate_dam_proximity_risk(heritage_with_admin, dam_gdf)

        # Step 4: Calculate fire risks (site level)
        heritage_with_fire_risk = self._calculate_fire_risk(heritage_with_dam_risk, fire_gdf)

        # Step 5: Combine all risks
        final_risk_assessment = self._combine_all_risks(heritage_with_fire_risk, municipal_cemaden_risk)

        return final_risk_assessment

    def _spatial_join_heritage_municipalities(self, heritage_gdf: gpd.GeoDataFrame,
                                              boundaries_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Spatially join heritage sites with municipalities"""
        logger.info("🔄 Performing spatial join: heritage sites ↔ municipalities...")

        if heritage_gdf.crs != boundaries_gdf.crs:
            heritage_gdf = heritage_gdf.to_crs(boundaries_gdf.crs)

        heritage_with_admin = gpd.sjoin(
            heritage_gdf,
            boundaries_gdf[['municipality', 'uf', 'geometry']],
            how='left',
            predicate='within'
        )

        # Handle unmatched sites
        unmatched = heritage_with_admin[heritage_with_admin['municipality'].isna()]
        if len(unmatched) > 0:
            logger.info(f"🔍 Finding nearest municipalities for {len(unmatched)} unmatched sites...")

            for idx in unmatched.index:
                try:
                    point = heritage_gdf.loc[idx, 'geometry']
                    if point is None or point.is_empty:
                        continue

                    distances = boundaries_gdf.geometry.distance(point)
                    nearest_idx = distances.idxmin()

                    heritage_with_admin.loc[idx, 'municipality'] = boundaries_gdf.loc[nearest_idx, 'municipality']
                    heritage_with_admin.loc[idx, 'uf'] = boundaries_gdf.loc[nearest_idx, 'uf']
                except Exception as e:
                    logger.warning(f"⚠️ Could not match site {idx}: {str(e)}")
                    heritage_with_admin.loc[idx, 'municipality'] = 'Unknown'
                    heritage_with_admin.loc[idx, 'uf'] = 'Unknown'

        heritage_with_admin = heritage_with_admin.drop(columns=['index_right'], errors='ignore')
        logger.info(f"✅ Matched {len(heritage_with_admin)} heritage sites with municipalities")
        return heritage_with_admin

    def _calculate_municipal_cemaden_risk(self, disaster_df: pd.DataFrame) -> pd.DataFrame:
        """Calculate CEMADEN risk scores by municipality"""
        logger.info("📊 Calculating CEMADEN municipal risk scores...")

        municipal_risks = []
        for (municipality, uf), group in disaster_df.groupby(['municipio', 'uf']):
            risk_score = 0
            for _, alert in group.iterrows():
                base_score = self.risk_scores.get(alert['nivel'], 2)
                weight = self.risk_weights.get(alert['tipo_alerta'], 1.0)
                risk_score += base_score * weight

            municipal_risks.append({
                'municipality': municipality,
                'uf': uf,
                'cemaden_risk_score': min(risk_score / 2, 10),
                'cemaden_alerts': len(group)
            })

        return pd.DataFrame(municipal_risks)

    def _calculate_dam_proximity_risk(self, heritage_gdf: gpd.GeoDataFrame,
                                      dam_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Calculate dam proximity risk for heritage sites"""
        logger.info("🏗️ Calculating dam proximity risks...")

        if len(dam_gdf) == 0:
            logger.info("   No dam data available, skipping dam risk calculation")
            heritage_gdf['dam_risk_score'] = 0
            heritage_gdf['nearest_dam_distance'] = np.inf
            return heritage_gdf

        # Create 5km buffer around dams (as per client requirements)
        dam_buffers = dam_gdf.copy()
        dam_buffers['geometry'] = dam_gdf.geometry.buffer(0.045)  # ~5km in degrees

        # Find heritage sites within dam risk zones
        heritage_in_dam_zones = gpd.sjoin(
            heritage_gdf,
            dam_buffers,
            how='left',
            predicate='within'
        )

        # Calculate dam risk scores
        heritage_gdf['dam_risk_score'] = 0
        heritage_gdf['nearest_dam_distance'] = np.inf

        for idx in heritage_in_dam_zones.index:
            if not pd.isna(heritage_in_dam_zones.loc[idx, 'index_right']):
                # Site is within 5km of a dam
                heritage_gdf.loc[idx, 'dam_risk_score'] = 3  # Moderate risk
                # Calculate actual distance to nearest dam
                site_point = heritage_gdf.loc[idx, 'geometry']
                distances = dam_gdf.geometry.distance(site_point)
                heritage_gdf.loc[idx, 'nearest_dam_distance'] = distances.min() * 111  # Convert to km

        dam_risk_sites = len(heritage_gdf[heritage_gdf['dam_risk_score'] > 0])
        logger.info(f"🏗️ Found {dam_risk_sites} heritage sites within 5km of dams")
        return heritage_gdf

    def _calculate_fire_risk(self, heritage_gdf: gpd.GeoDataFrame,
                             fire_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Calculate fire risk for heritage sites"""
        logger.info("🔥 Calculating fire risks...")

        if len(fire_gdf) == 0:
            logger.info("   No fire data available, skipping fire risk calculation")
            heritage_gdf['fire_risk_score'] = 0
            return heritage_gdf

        # Spatial join with fire risk areas
        heritage_with_fire = gpd.sjoin(
            heritage_gdf,
            fire_gdf,
            how='left',
            predicate='within'
        )

        # Calculate fire risk scores
        heritage_gdf['fire_risk_score'] = 0

        for idx in heritage_with_fire.index:
            if not pd.isna(heritage_with_fire.loc[idx, 'index_right']):
                # Site is in a fire risk area
                fire_level = heritage_with_fire.loc[idx, 'risk_level']
                heritage_gdf.loc[idx, 'fire_risk_score'] = self.risk_scores.get(fire_level, 2)

        fire_risk_sites = len(heritage_gdf[heritage_gdf['fire_risk_score'] > 0])
        logger.info(f"🔥 Found {fire_risk_sites} heritage sites in fire risk areas")
        return heritage_gdf

    def _combine_all_risks(self, heritage_gdf: gpd.GeoDataFrame,
                           municipal_cemaden_risk: pd.DataFrame) -> gpd.GeoDataFrame:
        """Combine all risk assessments into final score"""
        logger.info("🎯 Combining all risk assessments...")

        # Merge with CEMADEN municipal risks
        heritage_final = heritage_gdf.merge(
            municipal_cemaden_risk,
            left_on=['municipality', 'uf'],
            right_on=['municipality', 'uf'],
            how='left'
        )

        # Fill missing values
        heritage_final['cemaden_risk_score'] = heritage_final['cemaden_risk_score'].fillna(0)
        heritage_final['cemaden_alerts'] = heritage_final['cemaden_alerts'].fillna(0)

        # Calculate comprehensive risk score (weighted average)
        heritage_final['comprehensive_risk_score'] = (
                heritage_final['cemaden_risk_score'] * 0.4 +  # 40% weight for geological/hydrological
                heritage_final['dam_risk_score'] * 0.3 +  # 30% weight for dam proximity
                heritage_final['fire_risk_score'] * 0.3  # 30% weight for fire risk
        )

        # Categorize comprehensive risk
        heritage_final['comprehensive_risk_category'] = heritage_final['comprehensive_risk_score'].apply(
            self._categorize_risk_score
        )

        # Add risk colors for mapping
        risk_colors = {
            'Muito Alto': '#8B0000',  # Dark red
            'Alto': '#FF0000',  # Red
            'Moderado': '#FFA500',  # Orange
            'Baixo': '#FFFF00',  # Yellow
            'Muito Baixo': '#90EE90'  # Light green
        }
        heritage_final['risk_color'] = heritage_final['comprehensive_risk_category'].map(risk_colors)

        # Log comprehensive risk distribution
        risk_dist = heritage_final['comprehensive_risk_category'].value_counts()
        logger.info("📊 Comprehensive risk distribution:")
        for category, count in risk_dist.items():
            logger.info(f"   {category}: {count} sites")

        return heritage_final

    def _categorize_risk_score(self, score: float) -> str:
        """Categorize numerical risk score into text categories"""
        if score >= 8:
            return "Muito Alto"
        elif score >= 6:
            return "Alto"
        elif score >= 4:
            return "Moderado"
        elif score >= 2:
            return "Baixo"
        else:
            return "Muito Baixo"

    def create_comprehensive_analysis(self, heritage_final: gpd.GeoDataFrame) -> Dict:
        """Create comprehensive analysis with all 4 data sources"""
        logger.info("📈 Creating comprehensive analysis...")

        analysis = {
            'summary': {
                'total_heritage_sites': len(heritage_final),
                'municipalities_covered': len(heritage_final['municipality'].unique()),
                'states_covered': len(heritage_final['uf'].unique()),
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data_sources_integrated': 4
            },
            'risk_breakdown_by_source': {
                'cemaden_affected_sites': len(heritage_final[heritage_final['cemaden_risk_score'] > 0]),
                'dam_affected_sites': len(heritage_final[heritage_final['dam_risk_score'] > 0]),
                'fire_affected_sites': len(heritage_final[heritage_final['fire_risk_score'] > 0]),
                'multiple_risks_sites': len(heritage_final[
                                                (heritage_final['cemaden_risk_score'] > 0) &
                                                (heritage_final['dam_risk_score'] > 0) |
                                                (heritage_final['fire_risk_score'] > 0)
                                                ])
            },
            'comprehensive_risk_distribution': heritage_final['comprehensive_risk_category'].value_counts().to_dict(),
            'highest_risk_sites': [],
            'state_summaries': {}
        }

        # Top 10 highest comprehensive risk sites
        top_risk_sites = heritage_final.nlargest(10, 'comprehensive_risk_score')
        for _, site in top_risk_sites.iterrows():
            site_info = {
                'site_name': site.get('identificacao_bem', 'Unknown'),
                'municipality': site['municipality'],
                'uf': site['uf'],
                'comprehensive_risk_score': float(site['comprehensive_risk_score']),
                'comprehensive_risk_category': site['comprehensive_risk_category'],
                'cemaden_risk': float(site['cemaden_risk_score']),
                'dam_risk': float(site['dam_risk_score']),
                'fire_risk': float(site['fire_risk_score']),
                'nearest_dam_km': float(site['nearest_dam_distance']) if site[
                                                                             'nearest_dam_distance'] != np.inf else None
            }
            analysis['highest_risk_sites'].append(site_info)

        # State-level summaries
        for uf in heritage_final['uf'].unique():
            state_data = heritage_final[heritage_final['uf'] == uf]
            analysis['state_summaries'][uf] = {
                'heritage_sites': len(state_data),
                'average_comprehensive_risk': float(state_data['comprehensive_risk_score'].mean()),
                'high_risk_sites': len(
                    state_data[state_data['comprehensive_risk_category'].isin(['Alto', 'Muito Alto'])]),
                'cemaden_affected': len(state_data[state_data['cemaden_risk_score'] > 0]),
                'dam_affected': len(state_data[state_data['dam_risk_score'] > 0]),
                'fire_affected': len(state_data[state_data['fire_risk_score'] > 0])
            }

        return analysis

    def save_comprehensive_results(self, heritage_final: gpd.GeoDataFrame, analysis: Dict) -> None:
        """Save comprehensive results"""
        logger.info("💾 Saving comprehensive results...")

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Save comprehensive heritage risk assessment
        heritage_output = os.path.join(self.output_dir, f"comprehensive_heritage_risk_{timestamp}.geojson")
        heritage_final.to_file(heritage_output, driver='GeoJSON')

        heritage_latest = os.path.join(self.output_dir, "comprehensive_heritage_risk.geojson")
        heritage_final.to_file(heritage_latest, driver='GeoJSON')

        # Save comprehensive analysis
        analysis_output = os.path.join(self.output_dir, f"comprehensive_analysis_{timestamp}.json")
        with open(analysis_output, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, ensure_ascii=False, indent=2, default=str)

        analysis_latest = os.path.join(self.output_dir, "comprehensive_analysis.json")
        with open(analysis_latest, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, ensure_ascii=False, indent=2, default=str)

        # Save CSV version
        heritage_csv = heritage_final.drop(columns='geometry').copy()
        heritage_csv_output = os.path.join(self.output_dir, "comprehensive_heritage_risk.csv")
        heritage_csv.to_csv(heritage_csv_output, index=False, encoding='utf-8')

        logger.info(f"✅ Comprehensive results saved to: {self.output_dir}")

    def print_comprehensive_summary(self, analysis: Dict) -> None:
        """Print comprehensive executive summary"""
        print("\n" + "=" * 80)
        print("🏛️  COMPREHENSIVE HERITAGE RISK ASSESSMENT - ALL 4 DATA SOURCES")
        print("=" * 80)

        summary = analysis['summary']
        risk_breakdown = analysis['risk_breakdown_by_source']
        comprehensive_dist = analysis['comprehensive_risk_distribution']

        print(f"📊 OVERVIEW:")
        print(f"   Total Heritage Sites Analyzed: {summary['total_heritage_sites']:,}")
        print(f"   Municipalities Covered: {summary['municipalities_covered']:,}")
        print(f"   States Covered: {summary['states_covered']}")
        print(f"   Data Sources Integrated: {summary['data_sources_integrated']}/4 ✅")

        print(f"\n🔄 RISK SOURCE BREAKDOWN:")
        print(f"   CEMADEN (Geological/Hydrological): {risk_breakdown['cemaden_affected_sites']:,} sites")
        print(f"   SNISB (Dam Proximity): {risk_breakdown['dam_affected_sites']:,} sites")
        print(f"   INPE (Fire Risk): {risk_breakdown['fire_affected_sites']:,} sites")
        print(f"   Multiple Risk Types: {risk_breakdown['multiple_risks_sites']:,} sites")

        print(f"\n🚨 COMPREHENSIVE RISK DISTRIBUTION:")
        for category, count in comprehensive_dist.items():
            percentage = (count / summary['total_heritage_sites']) * 100
            print(f"   {category}: {count:,} sites ({percentage:.1f}%)")

        print(f"\n⚠️ TOP 5 HIGHEST RISK HERITAGE SITES:")
        for i, site in enumerate(analysis['highest_risk_sites'][:5], 1):
            print(f"   {i}. {site['site_name'][:50]}...")
            print(f"      📍 {site['municipality']}/{site['uf']}")
            print(
                f"      🔥 Overall Risk: {site['comprehensive_risk_category']} ({site['comprehensive_risk_score']:.1f}/10)")
            print(
                f"      ⚠️ CEMADEN: {site['cemaden_risk']:.1f} | 🏗️ Dam: {site['dam_risk']:.1f} | 🔥 Fire: {site['fire_risk']:.1f}")
            if site['nearest_dam_km']:
                print(f"      🏗️ Nearest Dam: {site['nearest_dam_km']:.1f} km")

        print(f"\n🗺️ STATE-LEVEL COMPREHENSIVE ANALYSIS:")
        state_summaries = analysis['state_summaries']
        sorted_states = sorted(state_summaries.items(),
                               key=lambda x: x[1]['average_comprehensive_risk'], reverse=True)

        for uf, data in sorted_states[:10]:  # Top 10 states
            print(f"   {uf}: {data['heritage_sites']} sites | "
                  f"Risk: {data['average_comprehensive_risk']:.1f}/10 | "
                  f"High Risk: {data['high_risk_sites']} | "
                  f"C:{data['cemaden_affected']} D:{data['dam_affected']} F:{data['fire_affected']}")

        print("=" * 80)
        print(f"📊 Comprehensive analysis completed: {summary['analysis_date']}")
        print(f"📂 Results available in: {self.output_dir}")
        print("=" * 80)

    def run_comprehensive_assessment(self) -> bool:
        """Run the complete comprehensive heritage risk assessment"""
        logger.info("🚀 Starting COMPREHENSIVE Heritage Risk Assessment Pipeline...")

        try:
            # Step 1: Check all data sources
            print("📋 Step 1: Checking ALL data sources...", flush=True)
            data_status = self.check_all_data_sources()

            missing_sources = [source for source, available in data_status.items() if not available]
            if missing_sources:
                print(f"❌ Missing data sources: {', '.join(missing_sources)}")
                print("💡 Make sure all data collection scripts have been run!")
                return False

            print("✅ All 4 data sources available!")

            # Step 2: Download IBGE boundaries
            # Step 2: Load IBGE boundaries
            print("📋 Step 2: Loading IBGE boundaries...", flush=True)
            if not self.download_ibge_boundaries():
                print("❌ Failed to load IBGE boundaries!")
                return False

            # Step 3: Load all data
            print("📋 Step 3: Loading ALL data sources...", flush=True)
            heritage_gdf, disaster_df, boundaries_gdf, dam_gdf, fire_gdf = self.load_all_data()

            # Step 4: Calculate comprehensive risk
            print("📋 Step 4: Calculating comprehensive risk assessment...", flush=True)
            heritage_final = self.calculate_comprehensive_risk(
                heritage_gdf, disaster_df, dam_gdf, fire_gdf, boundaries_gdf
            )

            # Step 5: Create comprehensive analysis
            print("📋 Step 5: Creating comprehensive analysis...", flush=True)
            analysis = self.create_comprehensive_analysis(heritage_final)

            # Step 6: Save results
            print("📋 Step 6: Saving comprehensive results...", flush=True)
            self.save_comprehensive_results(heritage_final, analysis)

            # Step 7: Print summary
            self.print_comprehensive_summary(analysis)

            logger.info("✅ COMPREHENSIVE Heritage Risk Assessment completed successfully!")
            return True

        except Exception as e:
            logger.error(f"❌ Comprehensive assessment failed: {str(e)}")
            print(f"❌ Error: {str(e)}")
            return False


def main():
    """Main execution function"""
    print("🏛️ COMPREHENSIVE Heritage Risk Assessment Pipeline", flush=True)
    print("📊 Integrating ALL 4 Data Sources: IPHAN + CEMADEN + SNISB + INPE", flush=True)
    print("=" * 70, flush=True)

    assessor = EnhancedHeritageRiskAssessment()
    success = assessor.run_comprehensive_assessment()

    if success:
        print("\n🎉 SUCCESS: Comprehensive Heritage Risk Assessment completed!")
        print(f"📊 Check results in: {assessor.output_dir}")
        print("\n💡 Next steps:")
        print("   • Open comprehensive_heritage_risk.geojson in QGIS")
        print("   • Export to interactive web map using QGIS2Web")
        print("   • Deploy to GitHub Pages for client")
        print("   • Review comprehensive_analysis.json for detailed insights")
        print("\n🎯 CLIENT DELIVERABLE READY:")
        print("   ✅ All 4 data sources integrated")
        print("   ✅ Comprehensive risk scoring")
        print("   ✅ Multi-threat assessment")
        print("   ✅ Ready for web export")
    else:
        print("\n❌ Comprehensive assessment failed. Check logs for details.")
        print("💡 Ensure all data collection scripts have been run:")
        print("   • IPHAN heritage sites collector")
        print("   • CEMADEN disaster alerts scraper")
        print("   • SNISB dam safety data collector")
        print("   • INPE fire risk data collector")


if __name__ == "__main__":
    main()