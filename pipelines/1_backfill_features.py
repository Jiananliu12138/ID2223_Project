"""
历史数据回填管道
用于初始化Feature Store,获取历史2年的数据
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime, timedelta
from data.entsoe_client import ENTSOEClient
from data.weather_client import WeatherClient
from data.data_cleaner import DataCleaner
from features.feature_engineering import FeatureEngineer
from features.feature_groups import FeatureStoreManager
from config.settings import BACKFILL_START_DATE, TIMEZONE
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def backfill_monthly(start_date: str, end_date: str, fsm: FeatureStoreManager):
    """
    按月回填数据(避免API超时)
    
    Args:
        start_date: 开始日期 'YYYY-MM-DD'
        end_date: 结束日期 'YYYY-MM-DD'
        fsm: Feature Store管理器
    """
    # 初始化客户端
    entsoe_client = ENTSOEClient()
    weather_client = WeatherClient()
    
    # 生成月度时间范围
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    
    current = start
    while current < end:
        month_start = current
        month_end = min(current + pd.DateOffset(months=1), end)
        
        month_start_str = month_start.strftime('%Y-%m-%d')
        month_end_str = month_end.strftime('%Y-%m-%d')
        
        logger.info(f"\n{'='*60}")
        logger.info(f"回填月份: {month_start_str} 到 {month_end_str}")
        logger.info(f"{'='*60}")
        
        try:
            # 1. 获取ENTSO-E市场数据
            logger.info("步骤 1/5: 获取ENTSO-E市场数据...")
            market_df = entsoe_client.fetch_all_market_data(
                month_start_str, 
                month_end_str
            )
            
            # 2. 获取天气数据
            logger.info("步骤 2/5: 获取天气数据...")
            weather_df = weather_client.fetch_historical(
                month_start_str,
                month_end_str
            )
            
            # 3. 合并数据
            logger.info("步骤 3/5: 合并数据...")
            
            # 统一时区：将天气数据的时区设置为与市场数据一致
            if market_df['timestamp'].dt.tz is not None and weather_df['timestamp'].dt.tz is None:
                # 市场数据有时区，天气数据无时区 → 给天气数据添加时区
                weather_df['timestamp'] = weather_df['timestamp'].dt.tz_localize(TIMEZONE)
                logger.info("已将天气数据时区设置为 Europe/Stockholm")
            elif market_df['timestamp'].dt.tz is None and weather_df['timestamp'].dt.tz is not None:
                # 天气数据有时区，市场数据无时区 → 给市场数据添加时区
                market_df['timestamp'] = market_df['timestamp'].dt.tz_localize(TIMEZONE)
                logger.info("已将市场数据时区设置为 Europe/Stockholm")
            
            merged_df = market_df.merge(weather_df, on='timestamp', how='left')
            
            # 4. 数据清洗
            logger.info("步骤 4/5: 数据清洗...")
            cleaned_df = DataCleaner.clean_pipeline(merged_df)
            
            # 5. 上传到Hopsworks
            logger.info("步骤 5/5: 上传到Feature Store...")
            
            # 分离电力和天气数据
            electricity_cols = ['timestamp', 'price', 'load_forecast', 
                              'wind_forecast', 'solar_forecast']
            weather_cols = ['timestamp', 'temperature_avg', 'wind_speed_10m_avg',
                          'wind_speed_80m_avg', 'irradiance_avg']
            
            electricity_df = cleaned_df[electricity_cols]
            weather_df = cleaned_df[weather_cols]
            
            # 保存到本地 (不上传到 Hopsworks)
            month_str = month_start.strftime('%Y-%m')
            fsm.save_electricity_data_local(electricity_df, month_str)
            fsm.save_weather_data_local(weather_df, month_str)
            
            logger.info(f"✅ 月份 {month_start.strftime('%Y-%m')} 数据已保存到本地!")
            
        except Exception as e:
            logger.error(f"❌ 月份 {month_start.strftime('%Y-%m')} 回填失败: {e}")
            logger.error(f"跳过此月份,继续下一个...")
        
        current = month_end


def main():
    """主函数"""
    logger.info(f"\n{'='*70}")
    logger.info("开始历史数据回填流程")
    logger.info(f"{'='*70}\n")
    
    # 计算回填时间范围
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = BACKFILL_START_DATE
    
    logger.info(f"回填范围: {start_date} 到 {end_date}")
    logger.info(f"预计耗时: 约 {pd.to_datetime(end_date).year - pd.to_datetime(start_date).year} 年数据")
    
    # 用户确认
    confirm = input("\n开始回填? 这可能需要较长时间 (y/n): ")
    if confirm.lower() != 'y':
        logger.info("用户取消回填")
        return
    
    # 初始化Feature Store (仅本地模式，不连接Hopsworks)
    logger.info("\n初始化本地数据管理器...")
    fsm = FeatureStoreManager(local_only=True)
    
    # 执行回填
    backfill_monthly(start_date, end_date, fsm)
    
    logger.info(f"\n{'='*70}")
    logger.info("✅ 历史数据回填完成!")
    logger.info(f"{'='*70}")


if __name__ == "__main__":
    main()

