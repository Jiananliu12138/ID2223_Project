"""
全局配置文件
"""
import os
from dotenv import load_dotenv
import pytz

# 加载环境变量
load_dotenv()

# API Keys
ENTSOE_API_KEY = os.getenv('ENTSOE_API_KEY')
HOPSWORKS_API_KEY = os.getenv('HOPSWORKS_API_KEY')
HOPSWORKS_PROJECT_NAME = os.getenv('HOPSWORKS_PROJECT_NAME', 'electricity_price_prediction')

# 区域配置
SE3_REGION = 'SE_3'
BIDDING_ZONE = '10Y1001A1001A47J'  # SE3的ENTSO-E代码

# 时区
TIMEZONE = pytz.timezone('Europe/Stockholm')

# SE3区域关键位置(用于天气数据聚合)
SE3_LOCATIONS = [
    {"name": "Stockholm", "lat": 59.33, "lon": 18.07, "weight": 0.4},
    {"name": "Uppsala", "lat": 59.86, "lon": 17.64, "weight": 0.2},
    {"name": "Västerås", "lat": 59.62, "lon": 16.55, "weight": 0.2},
    {"name": "Norrköping", "lat": 58.59, "lon": 16.19, "weight": 0.2}
]

# 数据配置
BACKFILL_START_DATE = "2024-01-01"
TRAINING_WINDOW_MONTHS = 6
VALIDATION_WINDOW_MONTHS = 3

# 特征组配置
FEATURE_GROUP_VERSION = 4  # 提升版本号以创建新 Feature Group，使用 float 类型字段
ELECTRICITY_FG_NAME = "electricity_market"
WEATHER_FG_NAME = "weather"
ENGINEERED_FG_NAME = "electricity_features_engineered"  # 工程特征 Feature Group
ENGINEERED_FG_VERSION = 1

# 模型配置
MODEL_NAME = "se3_price_predictor"
MODEL_VERSION = 1

# 管道执行时间(CET)
DAILY_UPDATE_HOUR = 13  # 13:30 CET,确保次日价格已公布
DAILY_UPDATE_MINUTE = 30

# 告警阈值
MAX_MISSING_HOURS = 6  # 最多允许连续缺失小时数
MAE_THRESHOLD = 8.0  # MAE超过此值时告警(EUR/MWh)

