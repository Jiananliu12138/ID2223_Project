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


def safe_tz_localize_series(series: pd.Series, tz: str):
    """Safely tz_localize a datetime Series with fallbacks to avoid DST errors."""
    try:
        return series.dt.tz_localize(tz, ambiguous='infer', nonexistent='shift_forward')
    except Exception:
        try:
            return series.dt.tz_localize(tz, ambiguous='NaT', nonexistent='shift_forward')
        except Exception as e:
            logger.warning("safe_tz_localize_series: fallback failed (%s). Leaving naive timestamps.", e)
            return series


def normalize_dataframe_timestamps(df: pd.DataFrame, time_col: str = 'timestamp', tz: str = TIMEZONE, name: str = 'data') -> pd.DataFrame:
    """Ensure time column is datetime, localized to tz (if naive), converted to UTC and deduplicated.

    - Converts to datetime
    - Safely localizes naive datetimes to `tz`
    - Converts to UTC if tz-aware
    - Removes duplicate timestamps (keeps first) and logs examples
    """
    if time_col not in df.columns:
        logger.warning("normalize_dataframe_timestamps: '%s' not in dataframe columns for %s", time_col, name)
        return df

    df = df.copy()
    df[time_col] = pd.to_datetime(df[time_col])

    # Localize naive timestamps
    if df[time_col].dt.tz is None:
        df[time_col] = safe_tz_localize_series(df[time_col], tz)
        logger.info("已将 %s 的时区设置为 %s", name, tz)

    # Convert to UTC for consistent merging
    if df[time_col].dt.tz is not None:
        try:
            df[time_col] = df[time_col].dt.tz_convert('UTC')
        except Exception as e:
            logger.warning("normalize_dataframe_timestamps: tz_convert to UTC failed for %s: %s", name, e)

    # Remove duplicate timestamps
    if df[time_col].duplicated().any():
        dup_count = int(df[time_col].duplicated().sum())
        dup_vals = df[time_col][df[time_col].duplicated()].unique()[:5]
        logger.warning("%s: Found %d duplicate timestamps, examples: %s. Keeping first occurrence.", name, dup_count, dup_vals)
        df = df[~df[time_col].duplicated(keep='first')]

    return df


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

        # 统一并标准化时间列：安全地本地化到配置时区，然后转换为 UTC，去重
        market_df = normalize_dataframe_timestamps(market_df, time_col='timestamp', tz=TIMEZONE, name='市场数据')
        weather_df = normalize_dataframe_timestamps(weather_df, time_col='timestamp', tz=TIMEZONE, name='天气数据')

        merged_df = market_df.merge(weather_df, on='timestamp', how='left')
        logger.info(f"  - 合并后共 {len(merged_df)} 条记录")
        
        # 5. 数据清洗
        logger.info("步骤 5/6: 数据清洗...")
        cleaned_df = DataCleaner.clean_pipeline(merged_df)
        
        # 将内部统一的 UTC timestamp 转回展示用的本地时区 (Europe/Stockholm)
        try:
            # 如果为 tz-aware（例如 UTC），直接转换；否则先 localize 到 UTC 再转换
            if cleaned_df['timestamp'].dt.tz is None:
                cleaned_df['timestamp'] = pd.to_datetime(cleaned_df['timestamp']).dt.tz_localize('UTC').dt.tz_convert(TIMEZONE)
            else:
                cleaned_df['timestamp'] = pd.to_datetime(cleaned_df['timestamp']).dt.tz_convert(TIMEZONE)
            logger.info("已将合并后时间戳转换为 %s 时区以便展示", TIMEZONE)
        except Exception as e:
            logger.warning("转换合并后时间戳到 %s 时区失败: %s。保留原始时间戳。", TIMEZONE, e)
        
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
        fsm.create_electricity_feature_group(electricity_df)
        fsm.create_weather_feature_group(weather_df)
        
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

