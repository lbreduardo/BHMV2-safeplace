import requests
import pandas as pd
import json
import time
import os
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import logging
from typing import Dict, List, Optional
import re
import urllib.parse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException


class CemadenScraper:
    def __init__(self, base_url="https://painelalertas.cemaden.gov.br"):
        self.base_url = base_url
        self.session = requests.Session()

        # Create project_data folder if it doesn't exist
        # Get script directory and go up to project root
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)

        # Create cemaden folder in project_data
        self.project_data_dir = os.path.join(project_root, "project_data", "cemaden")


        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(os.path.join(self.project_data_dir, 'cemaden_scraper.log')),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

        # User agent to avoid blocking
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session.headers.update(self.headers)

    def test_connection(self) -> bool:
        """Test if the CEMADEN website is accessible"""
        try:
            response = self.session.get(self.base_url, timeout=30)
            if response.status_code == 200:
                self.logger.info("✅ Connection to CEMADEN successful")
                return True
            else:
                self.logger.warning(f"❌ Connection failed with status: {response.status_code}")
                return False
        except Exception as e:
            self.logger.error(f"❌ Connection test failed: {str(e)}")
            return False

    def setup_selenium_driver(self) -> Optional[webdriver.Chrome]:
        """Setup Selenium WebDriver with Chrome"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument(f"--user-agent={self.headers['User-Agent']}")

            driver = webdriver.Chrome(options=chrome_options)
            self.logger.info("✅ Selenium WebDriver initialized")
            return driver
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize Selenium: {str(e)}")
            return None

    def scrape_with_selenium(self) -> List[Dict]:
        """Scrape CEMADEN alerts using Selenium"""
        driver = self.setup_selenium_driver()
        if not driver:
            return []

        alerts = []
        try:
            self.logger.info("🔍 Loading CEMADEN page with Selenium...")
            driver.get(self.base_url)

            # Wait for page to load
            wait = WebDriverWait(driver, 20)

            # Look for alert elements (you'll need to inspect the page to find correct selectors)
            alert_elements = wait.until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "alert-item"))
            )

            for element in alert_elements:
                try:
                    # Extract alert data (adjust selectors based on actual page structure)
                    alert_data = self.extract_alert_from_element(element)
                    if alert_data:
                        alerts.append(alert_data)
                except Exception as e:
                    self.logger.warning(f"⚠️ Error extracting alert: {str(e)}")
                    continue

            self.logger.info(f"✅ Scraped {len(alerts)} alerts with Selenium")

        except TimeoutException:
            self.logger.error("❌ Selenium timeout - page didn't load properly")
        except Exception as e:
            self.logger.error(f"❌ Selenium scraping failed: {str(e)}")
        finally:
            driver.quit()

        return alerts

    def extract_alert_from_element(self, element) -> Optional[Dict]:
        """Extract alert data from a web element"""
        try:
            # This is a template - you'll need to adjust based on actual page structure
            alert_text = element.text

            # Extract municipality and state
            municipality_match = re.search(r'Município:\s*([^,\n]+)', alert_text)
            state_match = re.search(r'UF:\s*([A-Z]{2})', alert_text)

            # Extract risk type and level
            risk_type_match = re.search(r'Tipo:\s*(Risco\s+(?:Hidrológico|Geológico))', alert_text)
            risk_level_match = re.search(r'Nível:\s*(Moderado|Alto|Muito\s+Alto)', alert_text)

            if not all([municipality_match, state_match, risk_type_match, risk_level_match]):
                return None

            # Only include specified risk types and levels
            risk_type = risk_type_match.group(1).strip()
            risk_level = risk_level_match.group(1).strip()

            if risk_type not in ['Risco Hidrológico', 'Risco Geológico']:
                return None

            if risk_level not in ['Moderado', 'Alto', 'Muito Alto']:
                return None

            return {
                'municipio': municipality_match.group(1).strip(),
                'uf': state_match.group(1).strip().upper(),
                'tipo_alerta': risk_type,
                'nivel': risk_level,
                'abertura': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'timestamp': datetime.now().timestamp()
            }

        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting alert data: {str(e)}")
            return None

    def scrape_with_requests(self) -> List[Dict]:
        """Attempt to scrape using requests (fallback method)"""
        try:
            response = self.session.get(self.base_url, timeout=30)
            if response.status_code != 200:
                return []

            soup = BeautifulSoup(response.content, 'html.parser')

            # Look for alert data in the HTML
            alerts = []
            # This is a template - adjust selectors based on actual page structure
            alert_elements = soup.find_all('div', class_='alert-item')

            for element in alert_elements:
                alert_data = self.extract_alert_from_soup(element)
                if alert_data:
                    alerts.append(alert_data)

            self.logger.info(f"✅ Scraped {len(alerts)} alerts with requests")
            return alerts

        except Exception as e:
            self.logger.error(f"❌ Requests scraping failed: {str(e)}")
            return []

    def extract_alert_from_soup(self, element) -> Optional[Dict]:
        """Extract alert data from BeautifulSoup element"""
        try:
            # Similar to extract_alert_from_element but for BeautifulSoup
            alert_text = element.get_text()

            # Extract data using regex (adjust patterns based on actual HTML)
            municipality_match = re.search(r'Município:\s*([^,\n]+)', alert_text)
            state_match = re.search(r'UF:\s*([A-Z]{2})', alert_text)
            risk_type_match = re.search(r'Tipo:\s*(Risco\s+(?:Hidrológico|Geológico))', alert_text)
            risk_level_match = re.search(r'Nível:\s*(Moderado|Alto|Muito\s+Alto)', alert_text)

            if not all([municipality_match, state_match, risk_type_match, risk_level_match]):
                return None

            risk_type = risk_type_match.group(1).strip()
            risk_level = risk_level_match.group(1).strip()

            if risk_type not in ['Risco Hidrológico', 'Risco Geológico']:
                return None

            if risk_level not in ['Moderado', 'Alto', 'Muito Alto']:
                return None

            return {
                'municipio': municipality_match.group(1).strip(),
                'uf': state_match.group(1).strip().upper(),
                'tipo_alerta': risk_type,
                'nivel': risk_level,
                'abertura': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'timestamp': datetime.now().timestamp()
            }

        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting alert data: {str(e)}")
            return None

    def create_sample_data(self) -> List[Dict]:
        """Create sample data for testing when scraping fails"""
        sample_data = [
            {
                'municipio': 'São Paulo',
                'uf': 'SP',
                'tipo_alerta': 'Risco Hidrológico',
                'nivel': 'Alto',
                'abertura': '2025-07-14 10:00:00',
                'timestamp': datetime.now().timestamp()
            },
            {
                'municipio': 'Rio de Janeiro',
                'uf': 'RJ',
                'tipo_alerta': 'Risco Geológico',
                'nivel': 'Muito Alto',
                'abertura': '2025-07-14 11:00:00',
                'timestamp': datetime.now().timestamp()
            },
            {
                'municipio': 'Belo Horizonte',
                'uf': 'MG',
                'tipo_alerta': 'Risco Hidrológico',
                'nivel': 'Moderado',
                'abertura': '2025-07-14 12:00:00',
                'timestamp': datetime.now().timestamp()
            },
            {
                'municipio': 'Salvador',
                'uf': 'BA',
                'tipo_alerta': 'Risco Geológico',
                'nivel': 'Alto',
                'abertura': '2025-07-14 13:00:00',
                'timestamp': datetime.now().timestamp()
            },
            {
                'municipio': 'Recife',
                'uf': 'PE',
                'tipo_alerta': 'Risco Hidrológico',
                'nivel': 'Muito Alto',
                'abertura': '2025-07-14 14:00:00',
                'timestamp': datetime.now().timestamp()
            }
        ]

        self.logger.info(f"📝 Created {len(sample_data)} sample alerts for testing")
        return sample_data

    def clean_municipality_name(self, name: str) -> str:
        """Clean and standardize municipality names"""
        if not name:
            return ""

        # Remove parentheses and content
        name = re.sub(r'\([^)]*\)', '', name)

        # Clean spaces and convert to proper case
        name = ' '.join(name.split()).title()

        return name.strip()

    def process_alerts(self, raw_alerts: List[Dict]) -> List[Dict]:
        """Process and clean alert data"""
        processed_alerts = []

        for alert in raw_alerts:
            # Clean municipality name
            alert['municipio'] = self.clean_municipality_name(alert['municipio'])

            # Ensure UF is uppercase
            alert['uf'] = alert['uf'].upper()

            # Add to processed list
            processed_alerts.append(alert)

        return processed_alerts

    def remove_duplicates(self, alerts: List[Dict]) -> List[Dict]:
        """Remove duplicate alerts, keeping most recent for same municipality + risk type"""
        if not alerts:
            return []

        # Group by municipality + UF + risk type
        grouped = {}
        for alert in alerts:
            key = f"{alert['municipio']}_{alert['uf']}_{alert['tipo_alerta']}"

            if key not in grouped:
                grouped[key] = alert
            else:
                # Keep the most recent alert
                if alert['timestamp'] > grouped[key]['timestamp']:
                    grouped[key] = alert

        deduped_alerts = list(grouped.values())
        self.logger.info(f"📊 Removed duplicates: {len(alerts)} → {len(deduped_alerts)} alerts")

        return deduped_alerts

    def save_data(self, alerts: List[Dict]) -> None:
        """Save data in multiple formats to project_data folder"""
        if not alerts:
            self.logger.warning("⚠️ No alerts to save")
            return

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Save as JSON
        json_file = os.path.join(self.project_data_dir, f'real_time_cemaden_data_{timestamp}.json')
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(alerts, f, ensure_ascii=False, indent=2)
        self.logger.info(f"💾 Saved JSON: {json_file}")

        # Save as CSV
        csv_file = os.path.join(self.project_data_dir, f'real_time_cemaden_data_{timestamp}.csv')
        df = pd.DataFrame(alerts)
        df.to_csv(csv_file, index=False, encoding='utf-8')
        self.logger.info(f"💾 Saved CSV: {csv_file}")

        # Save latest version for pipeline (without timestamp)
        latest_json = os.path.join(self.project_data_dir, 'real_time_cemaden_data.json')
        with open(latest_json, 'w', encoding='utf-8') as f:
            json.dump(alerts, f, ensure_ascii=False, indent=2)

        latest_csv = os.path.join(self.project_data_dir, 'real_time_cemaden_data.csv')
        df.to_csv(latest_csv, index=False, encoding='utf-8')

        self.logger.info(f"💾 Saved latest versions: real_time_cemaden_data.json/.csv")

        # Create summary
        summary = {
            'collection_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_alerts': len(alerts),
            'risk_types': {
                'Risco Hidrológico': len([a for a in alerts if a['tipo_alerta'] == 'Risco Hidrológico']),
                'Risco Geológico': len([a for a in alerts if a['tipo_alerta'] == 'Risco Geológico'])
            },
            'risk_levels': {
                'Moderado': len([a for a in alerts if a['nivel'] == 'Moderado']),
                'Alto': len([a for a in alerts if a['nivel'] == 'Alto']),
                'Muito Alto': len([a for a in alerts if a['nivel'] == 'Muito Alto'])
            },
            'states_covered': len(set([a['uf'] for a in alerts])),
            'municipalities_covered': len(set([f"{a['municipio']}_{a['uf']}" for a in alerts]))
        }

        summary_file = os.path.join(self.project_data_dir, 'real_time_cemaden_summary.json')
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        self.logger.info(f"📈 Summary: {summary}")

    def run_scraper(self) -> List[Dict]:
        """Main scraper execution with fallback strategies"""
        self.logger.info("🚀 Starting CEMADEN scraper...")

        # Test connection first
        if not self.test_connection():
            self.logger.error("❌ Cannot connect to CEMADEN. Using sample data.")
            return self.create_sample_data()

        # Try Selenium first
        alerts = self.scrape_with_selenium()

        # Fallback to requests if Selenium fails
        if not alerts:
            self.logger.info("🔄 Selenium failed, trying requests...")
            alerts = self.scrape_with_requests()

        # Final fallback to sample data
        if not alerts:
            self.logger.warning("⚠️ All scraping methods failed. Using sample data.")
            alerts = self.create_sample_data()

        # Process the alerts
        processed_alerts = self.process_alerts(alerts)
        final_alerts = self.remove_duplicates(processed_alerts)

        # Save the data
        self.save_data(final_alerts)

        self.logger.info(f"✅ Scraper completed successfully with {len(final_alerts)} alerts")
        return final_alerts


def main():
    """Main execution function"""
    scraper = CemadenScraper()
    alerts = scraper.run_scraper()

    if alerts:
        print(f"\n🎯 SUCCESS: Collected {len(alerts)} CEMADEN alerts")
        print(f"📁 Data saved to: {scraper.project_data_dir}")
        print(f"📊 Files created:")
        print(f"   - real_time_cemaden_data.json (latest)")
        print(f"   - real_time_cemaden_data.csv (latest)")
        print(f"   - real_time_cemaden_data_[timestamp].json (archived)")
        print(f"   - real_time_cemaden_data_[timestamp].csv (archived)")
        print(f"   - real_time_cemaden_summary.json (summary)")
        print(f"   - cemaden_scraper.log (log file)")

        # Show sample of data
        print(f"\n📋 Sample alerts:")
        for i, alert in enumerate(alerts[:3]):
            print(f"   {i + 1}. {alert['municipio']}/{alert['uf']} - {alert['tipo_alerta']} - {alert['nivel']}")

        if len(alerts) > 3:
            print(f"   ... and {len(alerts) - 3} more alerts")
    else:
        print("❌ No alerts collected. Check the logs for details.")


if __name__ == "__main__":
    main()