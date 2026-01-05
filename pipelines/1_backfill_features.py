"""
Historical backfill pipeline
Used to initialize the Feature Store and fetch historical data (2 years)
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


def safe_tz_localize_series(series: pd.Series, tz: str):
    """Attempt to tz_localize a datetime Series with fallbacks to avoid DST errors."""
    try:
        return series.dt.tz_localize(tz, ambiguous='infer', nonexistent='shift_forward')
    except Exception:
        try:
            return series.dt.tz_localize(tz, ambiguous='NaT', nonexistent='shift_forward')
        except Exception as e:
            logger.warning("safe_tz_localize_series: fallback failed (%s). Leaving naive timestamps.", e)
            return series


def normalize_dataframe_timestamps(df: pd.DataFrame, time_col: str = 'timestamp', tz: str = TIMEZONE, name: str = 'data') -> pd.DataFrame:
    """Ensure the time column is datetime, timezone-aware (converted to UTC), and has no duplicate timestamps.

    - Converts to datetime
    - Safely localizes naive datetimes to `tz` with fallbacks
    - Converts all timestamps to UTC for consistent merging
    - Removes duplicate timestamps (keeps first) and logs them
    """
    if time_col not in df.columns:
        logger.warning("normalize_dataframe_timestamps: '%s' not in dataframe columns for %s", time_col, name)
        return df

    df = df.copy()
    df[time_col] = pd.to_datetime(df[time_col])

    # If naive, try to localize safely
    if df[time_col].dt.tz is None:
        df[time_col] = safe_tz_localize_series(df[time_col], tz)
        logger.info("已将 %s 的时区设置为 %s", name, tz)

    # If still naive after attempts, leave as-is; otherwise convert to UTC for merging
    if df[time_col].dt.tz is not None:
        try:
            df[time_col] = df[time_col].dt.tz_convert('UTC')
        except Exception as e:
            logger.warning("normalize_dataframe_timestamps: tz_convert to UTC failed for %s: %s", name, e)

    # Normalize duplicates by timestamp
    if df[time_col].duplicated().any():
        dup_count = df[time_col].duplicated().sum()
        dup_vals = df[time_col][df[time_col].duplicated()].unique()[:5]
        logger.warning("%s: Found %d duplicate timestamps, examples: %s. Keeping first occurrence.", name, dup_count, dup_vals)
        df = df[~df[time_col].duplicated(keep='first')]

    return df


def backfill_monthly(start_date: str, end_date: str, fsm: FeatureStoreManager):
    """
    Backfill data monthly (avoids API timeouts)
    
    Args:
        start_date: start date 'YYYY-MM-DD'
        end_date: end date 'YYYY-MM-DD'
        fsm: Feature Store manager
    """
    # Initialize clients
    entsoe_client = ENTSOEClient()
    weather_client = WeatherClient()
    
    # Generate monthly date ranges
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    counter = 0
    
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
            # 1. Fetch ENTSO-E market data
            logger.info("步骤 1/5: 获取ENTSO-E市场数据...")
            market_df = entsoe_client.fetch_all_market_data(
                month_start_str, 
                month_end_str
            )
            
            # 2. Fetch weather data
            logger.info("步骤 2/5: 获取天气数据...")
            weather_df = weather_client.fetch_historical(
                month_start_str,
                month_end_str
            )
            
            # 3. Merge data
            logger.info("步骤 3/5: 合并数据...")

            # Normalize and standardize time columns: safely localize to configured timezone, convert to UTC, deduplicate
            market_df = normalize_dataframe_timestamps(market_df, time_col='timestamp', tz=TIMEZONE, name='市场数据')
            weather_df = normalize_dataframe_timestamps(weather_df, time_col='timestamp', tz=TIMEZONE, name='天气数据')

            # Ensure both tables have timestamp columns of the same type (UTC or naive) before merging
            merged_df = market_df.merge(weather_df, on='timestamp', how='left')
            
            # 4. Data cleaning
            logger.info("步骤 4/5: 数据清洗...")
            cleaned_df = DataCleaner.clean_pipeline(merged_df)
            
            try:
                # If tz-aware (e.g., UTC), convert directly; otherwise localize to UTC then convert
                if cleaned_df['timestamp'].dt.tz is None:
                    cleaned_df['timestamp'] = pd.to_datetime(cleaned_df['timestamp']).dt.tz_localize('UTC').dt.tz_convert(TIMEZONE)
                else:
                    cleaned_df['timestamp'] = pd.to_datetime(cleaned_df['timestamp']).dt.tz_convert(TIMEZONE)
                logger.info("已将合并后时间戳转换为 %s 时区以便展示", TIMEZONE)
            except Exception as e:
                logger.warning("转换合并后时间戳到 %s 时区失败: %s。保留原始时间戳。", TIMEZONE, e)
        
            
            # 5. Upload to Hopsworks
            logger.info("步骤 5/5: 上传到Feature Store...")
            
            # Split electricity and weather data
            electricity_cols = ['timestamp', 'price', 'load_forecast', 
                              'wind_forecast', 'solar_forecast']
            weather_cols = ['timestamp', 'temperature_avg', 'wind_speed_10m_avg',
                          'wind_speed_80m_avg', 'irradiance_avg']
            
            electricity_df = cleaned_df[electricity_cols]
            weather_df = cleaned_df[weather_cols]
            
            # Save locally (do not upload to Hopsworks)
            month_str = month_start.strftime('%Y-%m')
            fsm.save_electricity_data_local(electricity_df, month_str)
            fsm.save_weather_data_local(weather_df, month_str)
            
            logger.info(f"✅ 月份 {month_start.strftime('%Y-%m')} 数据已保存到本地!")
            
        except Exception as e:
            logger.error(f"❌ 月份 {month_start.strftime('%Y-%m')} 回填失败: {e}")
            logger.error(f"跳过此月份,继续下一个...")
            counter += 1
        current = month_end
    return counter

def main():
    """Main function"""
    logger.info(f"\n{'='*70}")
    logger.info("开始历史数据回填流程")
    logger.info(f"{'='*70}\n")
    
    # Compute backfill time range
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = BACKFILL_START_DATE
    
    logger.info(f"回填范围: {start_date} 到 {end_date}")
    logger.info(f"预计耗时: 约 {pd.to_datetime(end_date).year - pd.to_datetime(start_date).year} 年数据")
    
    # User confirmation
    confirm = input("\n开始回填? 这可能需要较长时间 (y/n): ")
    if confirm.lower() != 'y':
        logger.info("用户取消回填")
        return
    
    # Initialize Feature Store (local-only, do not connect to Hopsworks)
    logger.info("\n初始化本地数据管理器...")
    fsm = FeatureStoreManager(local_only=True)
    
    # Run backfill
    counter = backfill_monthly(start_date, end_date, fsm)
    
    logger.info(f"\n{'='*70}")
    logger.info("✅ 历史数据回填完成!")
    logger.info(f"总计跳过月份数: {counter}")
    logger.info(f"{'='*70}")


if __name__ == "__main__":
    main()

