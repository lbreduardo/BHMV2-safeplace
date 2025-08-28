import subprocess
import sys
import os
import time
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

print("🚀 MASTER HERITAGE RISK PIPELINE - COMPLETE AUTOMATION")
print("=" * 80)
print("📊 This script will:")
print("   1. Update CEMADEN disaster alerts (cemaden_collector.py)")
print("   2. Update INPE fire risk data (generate_map.py)")
print("   3. Run comprehensive heritage risk assessment")
print("   4. Generate client-ready deliverables")
print("=" * 80)


class MasterHeritageRiskPipeline:
    def __init__(self):
        # Get script directory
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.project_root = os.path.dirname(self.script_dir)

        # Define script paths - YOUR EXISTING SCRIPTS
        self.scripts = {
            'cemaden_collector': os.path.join(self.script_dir, 'cemaden_collector.py'),
            'generate_map': os.path.join(self.script_dir, 'generate_map.py'),
            'heritage_assessment': os.path.join(self.script_dir, 'heritage_risk_assessment.py')
        }

        # Results directory
        self.results_dir = os.path.join(self.project_root, "project_data", "comprehensive_assessment")

        logger.info("🏛️ Master Heritage Risk Pipeline initialized")
        logger.info(f"📂 Project root: {self.project_root}")
        logger.info(f"📊 Results will be saved to: {self.results_dir}")

    def check_scripts_exist(self) -> bool:
        """Check if all required scripts exist"""
        print("\n📋 Step 1: Checking if all scripts exist...")

        missing_scripts = []
        for script_name, script_path in self.scripts.items():
            if os.path.exists(script_path):
                print(f"   ✅ Found {script_name}: {os.path.basename(script_path)}")
                logger.info(f"✅ Script found: {script_path}")
            else:
                print(f"   ❌ Missing {script_name}: {script_path}")
                logger.error(f"❌ Script missing: {script_path}")
                missing_scripts.append(script_name)

        if missing_scripts:
            print(f"❌ Missing scripts: {', '.join(missing_scripts)}")
            print("💡 Make sure all scripts are in the same directory as this master script!")
            return False

        print("✅ All scripts found!")
        return True

    def run_script(self, script_name: str, script_path: str, description: str) -> bool:
        """Run a single script and return success status"""
        print(f"\n📋 {description}")
        print(f"   🔄 Running: {os.path.basename(script_path)}")

        start_time = time.time()
        logger.info(f"🚀 Starting {script_name}: {script_path}")

        try:
            # Run the script using Python
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                cwd=self.script_dir,
                timeout=1800  # 30 minute timeout
            )

            duration = time.time() - start_time

            if result.returncode == 0:
                print(f"   ✅ {script_name} completed successfully! ({duration:.1f}s)")
                logger.info(f"✅ {script_name} completed successfully in {duration:.1f}s")

                # Log last few lines of output for confirmation
                if result.stdout:
                    output_lines = result.stdout.strip().split('\n')
                    for line in output_lines[-3:]:  # Last 3 lines
                        if line.strip():
                            print(f"   📝 {line.strip()}")

                return True
            else:
                print(f"   ❌ {script_name} failed! (Exit code: {result.returncode})")
                logger.error(f"❌ {script_name} failed with exit code {result.returncode}")

                # Print error output
                if result.stderr:
                    print(f"   📝 Error: {result.stderr.strip()}")
                    logger.error(f"Error output: {result.stderr.strip()}")

                return False

        except subprocess.TimeoutExpired:
            print(f"   ⏰ {script_name} timed out after 30 minutes!")
            logger.error(f"⏰ {script_name} timed out")
            return False

        except Exception as e:
            print(f"   ❌ Error running {script_name}: {str(e)}")
            logger.error(f"❌ Error running {script_name}: {str(e)}")
            return False

    def check_results(self) -> None:
        """Check and report on generated results"""
        print("\n📋 Step 5: Checking generated results...")

        expected_files = [
            "comprehensive_heritage_risk.geojson",
            "comprehensive_heritage_risk.csv",
            "comprehensive_analysis.json"
        ]

        results_found = []
        for file_name in expected_files:
            file_path = os.path.join(self.results_dir, file_name)
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path) / (1024 * 1024)  # Size in MB
                print(f"   ✅ {file_name} ({file_size:.1f} MB)")
                results_found.append(file_name)
            else:
                print(f"   ❌ {file_name} - Not found!")

        if len(results_found) == len(expected_files):
            print("✅ All expected results generated successfully!")
            logger.info("✅ All expected result files found")
        else:
            print(f"⚠️ {len(results_found)}/{len(expected_files)} expected files found")
            logger.warning(f"Only {len(results_found)}/{len(expected_files)} expected files found")

    def print_summary(self, success: bool, total_time: float) -> None:
        """Print execution summary"""
        print("\n" + "=" * 80)
        if success:
            print("🎉 MASTER PIPELINE COMPLETED SUCCESSFULLY!")
            print("=" * 80)
            print(f"⏱️ Total execution time: {total_time:.1f} seconds ({total_time / 60:.1f} minutes)")
            print(f"📂 Results available in: {self.results_dir}")
            print("\n🎯 DELIVERABLES READY:")
            print("   ✅ Fresh CEMADEN disaster alerts integrated")
            print("   ✅ Latest INPE fire risk data included")
            print("   ✅ Comprehensive heritage risk assessment updated")
            print("   ✅ Client-ready files generated")
            print("\n💡 NEXT STEPS:")
            print("   • Open comprehensive_heritage_risk.geojson in QGIS")
            print("   • Export to interactive web map using QGIS2Web")
            print("   • Deploy updated map for client")
            print("   • Share updated analysis with stakeholders")
        else:
            print("❌ MASTER PIPELINE FAILED!")
            print("=" * 80)
            print("💡 Check the error messages above and:")
            print("   • Ensure all scripts are working individually")
            print("   • Check internet connection for data collection")
            print("   • Verify all data directories exist")
            print("   • Review log messages for specific errors")

        print("=" * 80)

    def run_complete_pipeline(self) -> bool:
        """Run the complete heritage risk assessment pipeline"""
        pipeline_start = time.time()
        logger.info("🚀 Starting Master Heritage Risk Pipeline")

        try:
            # Step 1: Check all scripts exist
            if not self.check_scripts_exist():
                return False

            # Step 2: Update CEMADEN disaster data
            if not self.run_script(
                    'cemaden_collector',
                    self.scripts['cemaden_collector'],
                    "Step 2: Updating CEMADEN disaster alerts..."
            ):
                print("❌ CEMADEN data collection failed! Stopping pipeline.")
                return False

            # Step 3: Update INPE fire risk data
            if not self.run_script(
                    'generate_map',
                    self.scripts['generate_map'],
                    "Step 3: Updating INPE fire risk data..."
            ):
                print("❌ INPE data collection failed! Stopping pipeline.")
                return False

            # Step 4: Run comprehensive heritage risk assessment
            if not self.run_script(
                    'heritage_assessment',
                    self.scripts['heritage_assessment'],
                    "Step 4: Running comprehensive heritage risk assessment..."
            ):
                print("❌ Heritage risk assessment failed! Stopping pipeline.")
                return False

            # Step 5: Check results
            self.check_results()

            total_time = time.time() - pipeline_start
            self.print_summary(True, total_time)
            logger.info(f"✅ Master pipeline completed successfully in {total_time:.1f}s")
            return True

        except KeyboardInterrupt:
            print("\n⚠️ Pipeline interrupted by user!")
            logger.warning("Pipeline interrupted by user")
            return False

        except Exception as e:
            total_time = time.time() - pipeline_start
            print(f"\n❌ Unexpected error in pipeline: {str(e)}")
            logger.error(f"Unexpected pipeline error: {str(e)}")
            self.print_summary(False, total_time)
            return False


def main():
    """Main execution function"""
    print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 Goal: Generate complete heritage risk assessment with latest data")
    print()

    pipeline = MasterHeritageRiskPipeline()
    success = pipeline.run_complete_pipeline()

    print(f"🕐 Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if success:
        print("\n🚀 Ready for QGIS2Web export and client delivery!")
        sys.exit(0)
    else:
        print("\n💡 Fix the errors above and try again.")
        sys.exit(1)


if __name__ == "__main__":
    main()