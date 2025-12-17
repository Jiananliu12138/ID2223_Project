"""
特征工程模块
"""
import pandas as pd
import numpy as np
from datetime import datetime
from config.feature_config import SWEDISH_HOLIDAYS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FeatureEngineer:
    """特征工程类"""
    
    @staticmethod
    def create_time_features(df: pd.DataFrame, timestamp_col: str = 'timestamp') -> pd.DataFrame:
        """
        创建时间相关特征
        
        Args:
            df: 输入DataFrame
            timestamp_col: 时间戳列名
            
        Returns:
            添加时间特征后的DataFrame
        """
        df = df.copy()
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        
        # 基础时间特征
        df['hour'] = df[timestamp_col].dt.hour
        df['day_of_week'] = df[timestamp_col].dt.dayofweek  # 0=Monday
        df['month'] = df[timestamp_col].dt.month
        df['day_of_year'] = df[timestamp_col].dt.dayofyear
        df['week_of_year'] = df[timestamp_col].dt.isocalendar().week
        
        # 周末标识
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        
        # 假日标识
        df['is_holiday'] = df[timestamp_col].apply(
            lambda x: 1 if x.strftime('%m-%d') in SWEDISH_HOLIDAYS else 0
        )
        
        # 周期性编码(小时和月份)
        df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
        
        # 用电高峰时段(早7-9点,晚17-20点)
        df['is_peak_morning'] = df['hour'].apply(lambda x: 1 if 7 <= x <= 9 else 0)
        df['is_peak_evening'] = df['hour'].apply(lambda x: 1 if 17 <= x <= 20 else 0)
        
        logger.info(f"创建了 {len([c for c in df.columns if c not in [timestamp_col]])} 个时间特征")
        return df
    
    @staticmethod
    def create_market_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        创建市场相关特征
        
        Args:
            df: 输入DataFrame(需包含load_forecast, wind_forecast, solar_forecast)
            
        Returns:
            添加市场特征后的DataFrame
        """
        df = df.copy()
        
        # 残差负载(需要化石燃料/核电满足的负载)
        if all(col in df.columns for col in ['load_forecast', 'wind_forecast', 'solar_forecast']):
            df['residual_load'] = (
                df['load_forecast'] - df['wind_forecast'] - df['solar_forecast']
            )
            
            # 可再生能源占比
            df['renewable_ratio'] = (
                (df['wind_forecast'] + df['solar_forecast']) / df['load_forecast']
            ).clip(0, 1)  # 限制在0-1之间
            
            # 可再生能源过剩(负残差负载)
            df['renewable_surplus'] = np.where(df['residual_load'] < 0, 
                                               abs(df['residual_load']), 
                                               0)
            
            # 负载压力指标
            df['load_stress'] = df['residual_load'] / df['load_forecast']
            
            logger.info("创建了市场特征: residual_load, renewable_ratio等")
        else:
            logger.warning("缺少必要的列,无法创建市场特征")
        
        return df
    
    @staticmethod
    def create_lag_features(df: pd.DataFrame, 
                           target_col: str = 'price',
                           lags: list = [1, 24, 168]) -> pd.DataFrame:
        """
        创建滞后特征
        
        Args:
            df: 输入DataFrame
            target_col: 目标列名
            lags: 滞后时间列表(小时)
            
        Returns:
            添加滞后特征后的DataFrame
        """
        df = df.copy()
        
        if target_col not in df.columns:
            logger.warning(f"列 {target_col} 不存在,跳过滞后特征创建")
            return df
        
        # 滞后特征
        for lag in lags:
            df[f'{target_col}_lag_{lag}h'] = df[target_col].shift(lag)
        
        # 滚动统计特征
        windows = [24, 168]  # 24小时和7天
        for window in windows:
            df[f'{target_col}_rolling_mean_{window}h'] = (
                df[target_col].shift(1).rolling(window=window).mean()
            )
            df[f'{target_col}_rolling_std_{window}h'] = (
                df[target_col].shift(1).rolling(window=window).std()
            )
            df[f'{target_col}_rolling_min_{window}h'] = (
                df[target_col].shift(1).rolling(window=window).min()
            )
            df[f'{target_col}_rolling_max_{window}h'] = (
                df[target_col].shift(1).rolling(window=window).max()
            )
        
        # 价格变化率
        df[f'{target_col}_diff_1h'] = df[target_col].diff(1)
        df[f'{target_col}_diff_24h'] = df[target_col].diff(24)
        
        logger.info(f"创建了 {len(lags)} 个滞后特征和滚动统计特征")
        return df
    
    @staticmethod
    def create_interaction_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        创建交互特征
        
        Args:
            df: 输入DataFrame
            
        Returns:
            添加交互特征后的DataFrame
        """
        df = df.copy()
        
        # 温度和负载的交互(供暖/制冷需求)
        if 'temperature_avg' in df.columns and 'load_forecast' in df.columns:
            df['temp_load_interaction'] = df['temperature_avg'] * df['load_forecast']
        
        # 风速和风电的交互
        if 'wind_speed_80m_avg' in df.columns and 'wind_forecast' in df.columns:
            df['wind_efficiency'] = np.where(
                df['wind_speed_80m_avg'] > 0,
                df['wind_forecast'] / (df['wind_speed_80m_avg'] ** 3),  # 风力发电立方关系
                0
            )
        
        # 时段和负载的交互
        if 'hour' in df.columns and 'load_forecast' in df.columns:
            df['hour_load_interaction'] = df['hour'] * df['load_forecast']
        
        logger.info("创建了交互特征")
        return df
    
    @staticmethod
    def engineer_features_pipeline(df: pd.DataFrame, 
                                   include_lag: bool = True) -> pd.DataFrame:
        """
        完整的特征工程管道
        
        Args:
            df: 输入DataFrame
            include_lag: 是否包含滞后特征(推理时可能不需要)
            
        Returns:
            特征工程后的DataFrame
        """
        logger.info("开始特征工程流程...")
        
        # 1. 时间特征
        df = FeatureEngineer.create_time_features(df)
        
        # 2. 市场特征
        df = FeatureEngineer.create_market_features(df)
        
        # 3. 滞后特征(仅训练时)
        if include_lag and 'price' in df.columns:
            df = FeatureEngineer.create_lag_features(df, 'price')
        
        # 4. 交互特征
        df = FeatureEngineer.create_interaction_features(df)
        
        logger.info(f"特征工程完成,总共 {df.shape[1]} 列")
        return df


def main():
    """测试函数"""
    # 创建测试数据
    dates = pd.date_range('2024-01-01', periods=200, freq='H')
    df = pd.DataFrame({
        'timestamp': dates,
        'price': np.random.uniform(20, 80, 200),
        'load_forecast': np.random.uniform(5000, 10000, 200),
        'wind_forecast': np.random.uniform(1000, 3000, 200),
        'solar_forecast': np.random.uniform(0, 500, 200),
        'temperature_avg': np.random.uniform(-5, 25, 200),
        'wind_speed_80m_avg': np.random.uniform(5, 15, 200)
    })
    
    # 特征工程
    df_engineered = FeatureEngineer.engineer_features_pipeline(df)
    
    print(f"原始列数: {len(['timestamp', 'price', 'load_forecast', 'wind_forecast', 'solar_forecast'])}")
    print(f"特征工程后列数: {df_engineered.shape[1]}")
    print(f"\n新增特征列:")
    print(df_engineered.columns.tolist())


if __name__ == "__main__":
    main()

