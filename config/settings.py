"""
Global configuration file
"""
import os
from dotenv import load_dotenv
import pytz

# Load environment variables
load_dotenv()

# API Keys
ENTSOE_API_KEY = os.getenv('ENTSOE_API_KEY')
HOPSWORKS_API_KEY = os.getenv('HOPSWORKS_API_KEY')
HOPSWORKS_PROJECT_NAME = os.getenv('HOPSWORKS_PROJECT_NAME', 'electricity_price_prediction')

# Region configuration
SE3_REGION = 'SE_3'
BIDDING_ZONE = '10Y1001A1001A47J'  # SE3 ENTSO-E code

# Timezone
TIMEZONE = pytz.timezone('Europe/Stockholm')

# SE3 region key locations (for weather data aggregation)
SE3_LOCATIONS = [
    {"name": "Stockholm", "lat": 59.33, "lon": 18.07, "weight": 0.4},
    {"name": "Uppsala", "lat": 59.86, "lon": 17.64, "weight": 0.2},
    {"name": "Västerås", "lat": 59.62, "lon": 16.55, "weight": 0.2},
    {"name": "Norrköping", "lat": 58.59, "lon": 16.19, "weight": 0.2}
]

# Data configuration
BACKFILL_START_DATE = "2024-01-01"
TRAINING_WINDOW_MONTHS = 6
VALIDATION_WINDOW_MONTHS = 3

# Feature group configuration
FEATURE_GROUP_VERSION = 4  # Increase version number to create new Feature Group with float type fields
ELECTRICITY_FG_NAME = "electricity_market"
WEATHER_FG_NAME = "weather"
ENGINEERED_FG_NAME = "electricity_features_engineered"  # Engineered features Feature Group
ENGINEERED_FG_VERSION = 2  # Increase version number to avoid Table already exists error

# Model configuration
MODEL_NAME = "se3_price_predictor"
MODEL_VERSION = 1

# Pipeline execution time (CET)
DAILY_UPDATE_HOUR = 13  # 13:30 CET, ensures next day's price is published
DAILY_UPDATE_MINUTE = 30

# Alert threshold
MAX_MISSING_HOURS = 6  # Maximum allowed consecutive missing hours
MAE_THRESHOLD = 8.0  # Alert when MAE exceeds this value (EUR/MWh)

