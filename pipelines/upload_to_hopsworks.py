"""
Upload local data to Hopsworks Feature Store
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from pathlib import Path
from features.feature_groups import FeatureStoreManager, LOCAL_DATA_DIR
from config.settings import ELECTRICITY_FG_NAME, WEATHER_FG_NAME
import logging

# If old table exists and causes conflicts, modify here to temporarily use a new version number
# After modification, re-uploading will create a new version of the FG
OVERRIDE_FG_VERSION = 3  # If needed, change to a larger number

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def upload_all_data():
    """Upload all locally saved data to Hopsworks"""
    
    # Check local data directory
    if not LOCAL_DATA_DIR.exists():
        logger.error(f"❌ Local data directory not found: {LOCAL_DATA_DIR}")
        return
    
    # Find all parquet files
    electricity_files = sorted(LOCAL_DATA_DIR.glob("electricity_*.parquet"))
    weather_files = sorted(LOCAL_DATA_DIR.glob("weather_*.parquet"))
    
    if not electricity_files or not weather_files:
        logger.error("❌ No local data files found, please run 1_backfill_features.py first")
        return
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Found {len(electricity_files)} electricity data files")
    logger.info(f"Found {len(weather_files)} weather data files")
    logger.info(f"{'='*70}\n")
    
    # User confirmation
    confirm = input("Start uploading to Hopsworks? (y/n): ")
    if confirm.lower() != 'y':
        logger.info("User cancelled upload")
        return
    
    # Connect to Hopsworks
    logger.info("\n🔗 Connecting to Hopsworks...")
    try:
        fsm = FeatureStoreManager(local_only=False)
    except Exception as e:
        logger.error(f"❌ Connection failed: {e}")
        logger.error("Please check HOPSWORKS_API_KEY and HOPSWORKS_PROJECT_NAME in .env file")
        return
    
    # Upload data
    success_count = 0
    fail_count = 0
    
    for elec_file, weather_file in zip(electricity_files, weather_files):
        month = elec_file.stem.replace("electricity_", "")
        
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Uploading month: {month}")
            logger.info(f"{'='*60}")
            
            # Read local files
            logger.info(f"📂 Reading local files...")
            electricity_df = pd.read_parquet(elec_file)
            weather_df = pd.read_parquet(weather_file)
            
            logger.info(f"   Electricity data: {len(electricity_df)} rows")
            logger.info(f"   Weather data: {len(weather_df)} rows")
            
            # Upload to Hopsworks
            fsm.create_electricity_feature_group(electricity_df)
            fsm.create_weather_feature_group(weather_df)
            
            logger.info(f"✅ Month {month} uploaded successfully!")
            success_count += 1
            
        except Exception as e:
            logger.error(f"❌ Month {month} upload failed: {e}")
            fail_count += 1
            
            # Ask whether to continue
            if fail_count > 0:
                retry = input(f"\nUpload failed, continue to next month? (y/n): ")
                if retry.lower() != 'y':
                    logger.info("User aborted upload")
                    break
    
    # Summary
    logger.info(f"\n{'='*70}")
    logger.info(f"Upload complete!")
    logger.info(f"  Success: {success_count} months")
    logger.info(f"  Failed: {fail_count} months")
    logger.info(f"{'='*70}")


def upload_specific_month(month: str):
    """
    Upload data for a specific month
    
    Args:
        month: Month string, e.g. '2024-01'
    """
    elec_file = LOCAL_DATA_DIR / f"electricity_{month}.parquet"
    weather_file = LOCAL_DATA_DIR / f"weather_{month}.parquet"
    
    if not elec_file.exists() or not weather_file.exists():
        logger.error(f"❌ Data files for month {month} do not exist")
        return
    
    # Connect to Hopsworks
    logger.info("🔗 Connecting to Hopsworks...")
    try:
        fsm = FeatureStoreManager(local_only=False)
    except Exception as e:
        logger.error(f"❌ Connection failed: {e}")
        return
    
    try:
        # Read and upload
        logger.info(f"📂 Reading data for month {month}...")
        electricity_df = pd.read_parquet(elec_file)
        weather_df = pd.read_parquet(weather_file)
        
        fsm.create_electricity_feature_group(electricity_df)
        fsm.create_weather_feature_group(weather_df)
        
        logger.info(f"✅ Month {month} uploaded successfully!")
        
    except Exception as e:
        logger.error(f"❌ Upload failed: {e}")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Upload local data to Hopsworks")
    parser.add_argument('--month', type=str, help='Specify month, e.g., 2024-01')
    parser.add_argument('--all', action='store_true', help='Upload all months')
    
    args = parser.parse_args()
    
    if args.month:
        upload_specific_month(args.month)
    elif args.all:
        upload_all_data()
    else:
        # Default: upload all
        upload_all_data()


if __name__ == "__main__":
    main()

