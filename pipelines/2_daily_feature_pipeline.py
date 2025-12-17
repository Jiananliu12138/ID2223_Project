"""
每日特征更新管道
在每天13:30 CET运行,获取最新发布的次日价格和预测数据
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime, timedelta
from data.entsoe_client import ENTSOEClient
from data.weather_client import WeatherClient
from data.data_cleaner import DataCleaner
from features.feature_groups import FeatureStoreManager
from config.settings import TIMEZONE
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def daily_update():
    """
    每日更新流程
    获取昨天和今天的数据(确保不遗漏)
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"每日特征更新 - {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*60}\n")
    
    # 计算时间范围(获取昨天到明天的数据)
    today = datetime.now(TIMEZONE)
    start_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = (today + timedelta(days=1)).strftime('%Y-%m-%d')
    
    logger.info(f"获取数据范围: {start_date} 到 {end_date}")
    
    try:
        # 1. 初始化客户端
        logger.info("步骤 1/6: 初始化客户端...")
        entsoe_client = ENTSOEClient()
        weather_client = WeatherClient()
        fsm = FeatureStoreManager()
        
        # 2. 获取市场数据
        logger.info("步骤 2/6: 获取ENTSO-E市场数据...")
        market_df = entsoe_client.fetch_all_market_data(start_date, end_date)
        logger.info(f"  - 获取了 {len(market_df)} 条市场数据")
        
        # 3. 获取天气预报
        logger.info("步骤 3/6: 获取天气预报...")
        weather_df = weather_client.fetch_forecast(start_date, end_date)
        logger.info(f"  - 获取了 {len(weather_df)} 条天气数据")
        
        # 4. 合并数据
        logger.info("步骤 4/6: 合并数据...")
        merged_df = market_df.merge(weather_df, on='timestamp', how='left')
        logger.info(f"  - 合并后共 {len(merged_df)} 条记录")
        
        # 5. 数据清洗
        logger.info("步骤 5/6: 数据清洗...")
        cleaned_df = DataCleaner.clean_pipeline(merged_df)
        
        # 6. 上传到Feature Store
        logger.info("步骤 6/6: 上传到Feature Store...")
        
        # 分离数据
        electricity_cols = ['timestamp', 'price', 'load_forecast', 
                          'wind_forecast', 'solar_forecast']
        weather_cols = ['timestamp', 'temperature_avg', 'wind_speed_10m_avg',
                       'wind_speed_80m_avg', 'irradiance_avg']
        
        electricity_df = cleaned_df[electricity_cols]
        weather_df = cleaned_df[weather_cols]
        
        # 上传(使用upsert避免重复)
        fsm.create_electricity_feature_group(electricity_df, online=False)
        fsm.create_weather_feature_group(weather_df, online=False)
        
        logger.info(f"\n{'='*60}")
        logger.info("✅ 每日更新成功!")
        logger.info(f"  - 电力数据: {len(electricity_df)} 条")
        logger.info(f"  - 天气数据: {len(weather_df)} 条")
        logger.info(f"  - 更新时间: {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}\n")
        
        return True
        
    except Exception as e:
        logger.error(f"\n{'='*60}")
        logger.error(f"❌ 每日更新失败!")
        logger.error(f"错误信息: {e}")
        logger.error(f"{'='*60}\n")
        
        # 这里可以添加告警通知逻辑
        # send_alert_email(f"每日管道失败: {e}")
        
        return False


def main():
    """主函数"""
    success = daily_update()
    
    if success:
        logger.info("管道执行成功,退出...")
        exit(0)
    else:
        logger.error("管道执行失败,退出码1")
        exit(1)


if __name__ == "__main__":
    main()

