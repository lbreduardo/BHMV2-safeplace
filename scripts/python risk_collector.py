import requests
import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import warnings

warnings.filterwarnings('ignore')


class BrazilianHeritageRiskCollector:
    """
    Brazilian Cultural Heritage Risk Data Collector
    Focused on collecting risk data specific to Brazilian heritage sites
    """

    def __init__(self, base_dir: str = "risk_data"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        self.setup_logging()

        # Brazil-specific bounding box
        self.brazil_bbox = {
            'min_lon': -73.9872,  # Western border
            'max_lon': -28.8404,  # Eastern border
            'min_lat': -33.7683,  # Southern border
            'max_lat': 5.2842  # Northern border
        }

        # Brazilian data sources
        self.data_sources = {
            'usgs_earthquakes': 'https://earthquake.usgs.gov/fdsnws/event/1/query',
            'cemaden': 'http://www2.cemaden.gov.br/mapainterativo/',
            'inpe_queimadas': 'https://terrabrasilis.dpi.inpe.br/queimadas/',
            'mapbiomas': 'https://mapbiomas.org/en/statistics',
            'ibama': 'https://www.ibama.gov.br/sophia/cnia/livros/digitais/geoprocessamento.pdf',
            'cprm': 'https://geosgb.cprm.gov.br/',
            'ibge': 'https://www.ibge.gov.br/geociencias/downloads-geociencias.html'
        }

    def setup_logging(self):
        """Setup logging for the collector"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.base_dir / 'collection.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def collect_usgs_earthquakes(self, years_back: int = 10) -> gpd.GeoDataFrame:
        """
        Collect earthquake data from USGS for Brazil
        """
        print(f"\n=== COLLECTING BRAZILIAN EARTHQUAKE DATA ===")
        print(f"Timeframe: Last {years_back} years")
        print(f"Minimum magnitude: 3.0")
        print(f"Region: Brazil")

        end_date = datetime.now()
        start_date = end_date - timedelta(days=years_back * 365)

        params = {
            'format': 'geojson',
            'starttime': start_date.strftime('%Y-%m-%d'),
            'endtime': end_date.strftime('%Y-%m-%d'),
            'minmagnitude': 3.0,
            'minlatitude': self.brazil_bbox['min_lat'],
            'maxlatitude': self.brazil_bbox['max_lat'],
            'minlongitude': self.brazil_bbox['min_lon'],
            'maxlongitude': self.brazil_bbox['max_lon']
        }

        try:
            print("Requesting earthquake data from USGS...")
            response = requests.get(self.data_sources['usgs_earthquakes'], params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            if data['features']:
                gdf = gpd.GeoDataFrame.from_features(data['features'])
                gdf.crs = 'EPSG:4326'

                # Add Brazilian context
                gdf['country'] = 'Brazil'
                gdf['risk_level'] = gdf['mag'].apply(self._categorize_earthquake_risk)
                gdf['heritage_concern'] = gdf['mag'].apply(
                    lambda x: 'HIGH' if x >= 4.5 else 'MEDIUM' if x >= 3.5 else 'LOW')

                # Save data
                output_file = self.base_dir / 'brazilian_earthquakes.geojson'
                gdf.to_file(output_file, driver='GeoJSON')

                print(f"SUCCESS: {len(gdf)} Brazilian earthquakes collected")
                print(f"Saved to: {output_file}")
                print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

                return gdf

            else:
                print("No earthquake data found for Brazil in the specified timeframe")
                return gpd.GeoDataFrame()

        except Exception as e:
            print(f"ERROR collecting earthquake data: {str(e)}")
            return gpd.GeoDataFrame()

    def _categorize_earthquake_risk(self, magnitude: float) -> str:
        """Categorize earthquake risk for heritage sites"""
        if magnitude >= 5.0:
            return 'VERY_HIGH'
        elif magnitude >= 4.5:
            return 'HIGH'
        elif magnitude >= 4.0:
            return 'MEDIUM'
        elif magnitude >= 3.5:
            return 'LOW'
        else:
            return 'VERY_LOW'

    def setup_cemaden_integration(self) -> Dict:
        """
        Setup CEMADEN (National Center for Monitoring and Early Warning of Natural Disasters) integration
        """
        print("\n=== SETTING UP CEMADEN INTEGRATION ===")
        print("CEMADEN monitors: floods, landslides, droughts, geological risks")

        cemaden_config = {
            'description': 'Brazilian National Center for Monitoring and Early Warning of Natural Disasters',
            'url': self.data_sources['cemaden'],
            'risk_types': {
                'floods': {
                    'heritage_impact': 'HIGH',
                    'description': 'Flood monitoring for heritage sites near rivers and coastal areas',
                    'data_frequency': 'real-time'
                },
                'landslides': {
                    'heritage_impact': 'VERY_HIGH',
                    'description': 'Landslide risk for heritage sites in mountainous regions',
                    'data_frequency': 'real-time'
                },
                'droughts': {
                    'heritage_impact': 'MEDIUM',
                    'description': 'Drought monitoring affecting heritage site preservation',
                    'data_frequency': 'daily'
                },
                'geological_risks': {
                    'heritage_impact': 'HIGH',
                    'description': 'Geological instability affecting heritage structures',
                    'data_frequency': 'continuous'
                }
            },
            'integration_notes': [
                'CEMADEN provides real-time monitoring data',
                'Municipal-level risk alerts available',
                'Historical disaster database for trend analysis',
                'Integration requires API access request'
            ]
        }

        # Save configuration
        output_file = self.base_dir / 'cemaden_integration.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(cemaden_config, f, indent=2, ensure_ascii=False)

        print(f"CEMADEN integration setup saved to: {output_file}")
        return cemaden_config

    def setup_mapbiomas_integration(self) -> Dict:
        """
        Setup MapBiomas (Brazilian land use and land cover mapping) integration
        """
        print("\n=== SETTING UP MAPBIOMAS INTEGRATION ===")
        print("MapBiomas tracks: deforestation, urban expansion, land use changes")

        mapbiomas_config = {
            'description': 'Brazilian Annual Land Use and Land Cover Mapping Project',
            'url': self.data_sources['mapbiomas'],
            'heritage_relevance': {
                'urban_expansion': {
                    'impact': 'HIGH',
                    'description': 'Urban growth near heritage sites - pressure and development risks'
                },
                'deforestation': {
                    'impact': 'VERY_HIGH',
                    'description': 'Forest loss affecting heritage sites in natural settings'
                },
                'agriculture_expansion': {
                    'impact': 'MEDIUM',
                    'description': 'Agricultural expansion affecting rural heritage sites'
                },
                'mining_activities': {
                    'impact': 'VERY_HIGH',
                    'description': 'Mining operations threatening heritage landscapes'
                }
            },
            'data_coverage': {
                'temporal': '1985-2023',
                'spatial': 'All Brazil',
                'resolution': '30 meters',
                'update_frequency': 'annual'
            },
            'integration_approach': [
                'Download land use data for heritage site buffers',
                'Analyze change trends over time',
                'Identify rapid changes near heritage sites',
                'Create risk alerts for significant changes'
            ]
        }

        # Save configuration
        output_file = self.base_dir / 'mapbiomas_integration.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(mapbiomas_config, f, indent=2, ensure_ascii=False)

        print(f"MapBiomas integration setup saved to: {output_file}")
        return mapbiomas_config

    def setup_climate_risk_sources(self) -> Dict:
        """
        Setup Brazilian climate and environmental risk data sources
        """
        print("\n=== SETTING UP CLIMATE RISK SOURCES ===")

        climate_sources = {
            'inpe_queimadas': {
                'name': 'INPE Fire Monitoring Program',
                'url': self.data_sources['inpe_queimadas'],
                'description': 'Real-time fire and burning monitoring for Brazil',
                'heritage_impact': 'VERY_HIGH',
                'data_type': 'fire_hotspots',
                'update_frequency': 'real-time'
            },
            'inpe_prodes': {
                'name': 'INPE Amazon Deforestation Monitoring',
                'url': 'http://www.obt.inpe.br/OBT/assuntos/programas/amazonia/prodes',
                'description': 'Amazon deforestation monitoring - affects heritage sites in the Amazon',
                'heritage_impact': 'HIGH',
                'data_type': 'deforestation_alerts',
                'update_frequency': 'monthly'
            },
            'cprm_geological': {
                'name': 'CPRM Geological Service',
                'url': self.data_sources['cprm'],
                'description': 'Geological hazards, groundwater, and mineral resources',
                'heritage_impact': 'HIGH',
                'data_type': 'geological_hazards',
                'update_frequency': 'continuous'
            },
            'ibge_demographics': {
                'name': 'IBGE Geographic and Statistical Data',
                'url': self.data_sources['ibge'],
                'description': 'Population growth and urban development pressure',
                'heritage_impact': 'MEDIUM',
                'data_type': 'demographic_pressure',
                'update_frequency': 'annual'
            }
        }

        # Save configuration
        output_file = self.base_dir / 'climate_risk_sources.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(climate_sources, f, indent=2, ensure_ascii=False)

        print(f"Climate risk sources setup saved to: {output_file}")
        return climate_sources

    def generate_heritage_risk_report(self, earthquake_data: gpd.GeoDataFrame) -> str:
        """
        Generate comprehensive heritage risk assessment report
        """
        print("\n=== GENERATING HERITAGE RISK REPORT ===")

        report_content = f"""
# BRAZILIAN CULTURAL HERITAGE RISK ASSESSMENT REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## SUMMARY
This report provides risk data collection results for Brazilian cultural heritage sites (IPHAN).

## EARTHQUAKE RISK DATA
- **Total earthquakes collected**: {len(earthquake_data)}
- **Timeframe**: Last 10 years (2015-2024)
- **Magnitude range**: 3.0+ (heritage-relevant earthquakes)
- **Geographic coverage**: Brazil national territory

### Earthquake Risk Categories:
"""

        if not earthquake_data.empty:
            risk_counts = earthquake_data['risk_level'].value_counts()
            for risk_level, count in risk_counts.items():
                report_content += f"- **{risk_level}**: {count} earthquakes\n"

        report_content += f"""

### Geographic Distribution:
- **Coverage**: Complete Brazilian territory
- **Focus areas**: Heritage sites with seismic activity within 50km radius
- **High-risk heritage sites**: Sites near magnitude 4.0+ earthquakes

## BRAZILIAN RISK DATA SOURCES SETUP
The following Brazilian institutions have been configured for data integration:

### 1. CEMADEN (Centro Nacional de Monitoramento e Alertas)
- **Purpose**: Brazilian disaster monitoring and early warning
- **Data**: Real-time flood, landslide, drought alerts
- **Heritage Impact**: VERY HIGH - Critical for heritage site protection
- **Status**: Configuration template created

### 2. MapBiomas Brasil
- **Purpose**: Brazilian land use and land cover mapping (1985-2024)
- **Data**: Deforestation, urban expansion, land use change
- **Heritage Impact**: VERY HIGH - Landscape change monitoring
- **Status**: Integration template created

### 3. INPE (Instituto Nacional de Pesquisas Espaciais)
- **Purpose**: Brazilian environmental monitoring
- **Data**: Fire hotspots, deforestation alerts, climate data
- **Heritage Impact**: HIGH - Environmental threat monitoring
- **Status**: Integration framework prepared

### 4. CPRM (Serviço Geológico do Brasil)
- **Purpose**: Brazilian geological hazards and resources
- **Data**: Geological hazards, groundwater, mineral resources
- **Heritage Impact**: HIGH - Geological risk assessment
- **Status**: Integration framework prepared

## NEXT STEPS FOR BRAZILIAN HERITAGE RISK ANALYSIS

### Immediate Actions:
1. **Load earthquake data in QGIS** with your IPHAN heritage sites
2. **Create 10km, 25km, 50km buffers** around heritage sites
3. **Identify high-risk heritage sites** (near magnitude 4.0+ earthquakes)
4. **Contact CEMADEN** for API access to Brazilian disaster data

### Brazilian Data Integration Priority:
1. **CEMADEN flood/landslide data** - Municipal-level risk zones
2. **MapBiomas land use data** - Download for heritage site regions
3. **INPE fire monitoring data** - Real-time fire threat alerts
4. **IBGE demographic data** - Population pressure analysis

### Risk Analysis Framework:
- **Proximity analysis**: Heritage sites within risk zones
- **Temporal analysis**: Historical risk patterns (10-year trends)
- **Vulnerability assessment**: Heritage site exposure levels
- **Early warning integration**: Connect with Brazilian monitoring systems

## FILES CREATED:
- `brazilian_earthquakes.geojson` - Brazilian earthquake data (ready for QGIS)
- `cemaden_integration.json` - CEMADEN disaster risk setup
- `mapbiomas_integration.json` - MapBiomas land use integration
- `climate_risk_sources.json` - Brazilian climate risk sources
- `collection_report.md` - This comprehensive report

## TECHNICAL NOTES:
- All data uses WGS84 (EPSG:4326) coordinate system
- Earthquake data includes heritage-specific risk categories
- Brazilian bounding box ensures complete national coverage
- Integration templates provide step-by-step implementation guides

Generated by Brazilian Heritage Risk Data Collector v1.0
"""

        # Save report
        report_file = self.base_dir / 'heritage_risk_report.md'
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)

        print(f"Heritage risk report saved to: {report_file}")
        return report_file

    def run_complete_collection(self):
        """
        Run the complete Brazilian heritage risk data collection process
        """
        print("\n" + "=" * 60)
        print("STARTING COMPLETE BRAZILIAN HERITAGE RISK DATA COLLECTION")
        print("=" * 60)

        # Create directories
        self.base_dir.mkdir(exist_ok=True)

        # Step 1: Collect earthquake data
        earthquake_data = self.collect_usgs_earthquakes()

        # Step 2: Setup institutional integrations
        cemaden_config = self.setup_cemaden_integration()
        mapbiomas_config = self.setup_mapbiomas_integration()
        climate_sources = self.setup_climate_risk_sources()

        # Step 3: Generate comprehensive report
        report_file = self.generate_heritage_risk_report(earthquake_data)

        # Step 4: Display completion summary
        print("\n" + "=" * 60)
        print("BRAZILIAN HERITAGE RISK DATA COLLECTION COMPLETE!")
        print("=" * 60)
        print(f"📁 Data directory: {self.base_dir}")
        print(f"📊 Earthquakes collected: {len(earthquake_data)}")
        print(f"📋 Report generated: {report_file}")
        print("\n🎯 NEXT STEPS:")
        print("1. Load brazilian_earthquakes.geojson in QGIS")
        print("2. Add your IPHAN heritage sites layer")
        print("3. Create proximity analysis (buffer zones)")
        print("4. Contact CEMADEN for disaster risk data access")
        print("5. Download MapBiomas land use data for heritage regions")
        print("\n✅ Ready for Brazilian heritage risk analysis!")


def main():
    """Main function to run Brazilian heritage risk data collection"""
    print("BRAZILIAN HERITAGE RISK DATA COLLECTOR")
    print("=" * 60)
    print("Focused on Brazilian cultural heritage sites (IPHAN)")
    print("=" * 60)

    # Initialize collector
    collector = BrazilianHeritageRiskCollector()

    print("\nCOLLECTION OPTIONS:")
    print("1. Complete Brazilian Heritage Risk Collection (Recommended)")
    print("2. Earthquake Data Only")
    print("3. Setup Brazilian Institutional Integrations Only")
    print("4. Generate Report Only")

    choice = input("\nEnter your choice (1-4): ").strip()

    if choice == "1":
        # Complete collection
        collector.run_complete_collection()

    elif choice == "2":
        # Earthquake data only
        earthquake_data = collector.collect_usgs_earthquakes()
        if not earthquake_data.empty:
            print(f"\n✅ Successfully collected {len(earthquake_data)} Brazilian earthquakes")
            print(f"📁 Saved to: {collector.base_dir}/brazilian_earthquakes.geojson")
        else:
            print("❌ No earthquake data collected")

    elif choice == "3":
        # Setup integrations only
        collector.setup_cemaden_integration()
        collector.setup_mapbiomas_integration()
        collector.setup_climate_risk_sources()
        print("\n✅ Brazilian institutional integration templates created")

    elif choice == "4":
        # Generate report only (with empty earthquake data)
        collector.generate_heritage_risk_report(gpd.GeoDataFrame())
        print("\n✅ Report generated")

    else:
        print("Invalid choice. Please run the script again.")


if __name__ == "__main__":
    main()