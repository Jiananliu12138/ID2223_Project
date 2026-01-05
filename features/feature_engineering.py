"""
Feature engineering module
"""
import pandas as pd
import numpy as np
from datetime import datetime
from config.feature_config import SWEDISH_HOLIDAYS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FeatureEngineer:
    """Feature engineering class"""
    
    @staticmethod
    def create_time_features(df: pd.DataFrame, timestamp_col: str = 'timestamp') -> pd.DataFrame:
        """
        Create time-related features
        
        Args:
            df: Input DataFrame
            timestamp_col: Name of timestamp column
            
        Returns:
            DataFrame with added time features
        """
        df = df.copy()
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        
        # 基础时间特征
        df['hour'] = df[timestamp_col].dt.hour
        df['day_of_week'] = df[timestamp_col].dt.dayofweek  # 0=Monday
        df['month'] = df[timestamp_col].dt.month
        df['day_of_year'] = df[timestamp_col].dt.dayofyear
        df['week_of_year'] = df[timestamp_col].dt.isocalendar().week
        
        # Weekend identifier
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        
        # Holiday identifier
        df['is_holiday'] = df[timestamp_col].apply(
            lambda x: 1 if x.strftime('%m-%d') in SWEDISH_HOLIDAYS else 0
        )
        
        # Periodic encoding (hour and month)
        df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
        
        # Peak hours (morning 7-9am, evening 5-8pm)
        df['is_peak_morning'] = df['hour'].apply(lambda x: 1 if 7 <= x <= 9 else 0)
        df['is_peak_evening'] = df['hour'].apply(lambda x: 1 if 17 <= x <= 20 else 0)
        
        logger.info(f"Created {len([c for c in df.columns if c not in [timestamp_col]])} time features")
        return df
    
    @staticmethod
    def create_market_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        Create market-related features
        
        Args:
            df: Input DataFrame (must contain load_forecast, wind_forecast, solar_forecast)
            
        Returns:
            DataFrame with added market features
        """
        df = df.copy()
        
        # Residual load (load that needs to be met by fossil fuels/nuclear)
        if all(col in df.columns for col in ['load_forecast', 'wind_forecast', 'solar_forecast']):
            df['residual_load'] = (
                df['load_forecast'] - df['wind_forecast'] - df['solar_forecast']
            )
            
            # Renewable energy ratio
            df['renewable_ratio'] = (
                (df['wind_forecast'] + df['solar_forecast']) / df['load_forecast']
            ).clip(0, 1)  # Limit to 0-1
            
            # Renewable energy surplus (negative residual load)
            df['renewable_surplus'] = np.where(df['residual_load'] < 0,
                                               abs(df['residual_load']), 
                                               0)
            
            # Load stress indicator
            df['load_stress'] = df['residual_load'] / df['load_forecast']
            
            logger.info("Created market features: residual_load, renewable_ratio, etc.")
        else:
            logger.warning("Missing necessary columns, cannot create market features")
        
        return df
    
    @staticmethod
    def create_lag_features(df: pd.DataFrame,
                           target_col: str = 'price',
                           lags: list = [1, 24, 168]) -> pd.DataFrame:
        """
        Create lag features
        
        Args:
            df: Input DataFrame
            target_col: Target column name
            lags: List of lag times (hours)
            
        Returns:
            DataFrame with added lag features
        """
        df = df.copy()
        
        if target_col not in df.columns:
            logger.warning(f"Column {target_col} does not exist, skipping lag feature creation")
            return df
        
        # Lag features
        for lag in lags:
            df[f'{target_col}_lag_{lag}h'] = df[target_col].shift(lag)
        
        # Rolling statistics features
        windows = [24, 168]  # 24 hours and 7 days
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
        
        # Price change rate
        df[f'{target_col}_diff_1h'] = df[target_col].diff(1)
        df[f'{target_col}_diff_24h'] = df[target_col].diff(24)
        
        logger.info(f"Created {len(lags)} lag features and rolling statistics")
        return df
    
    @staticmethod
    def create_interaction_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        Create interaction features
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with added interaction features
        """
        df = df.copy()
        
        # Temperature and load interaction (heating/cooling demand)
        if 'temperature_avg' in df.columns and 'load_forecast' in df.columns:
            df['temp_load_interaction'] = df['temperature_avg'] * df['load_forecast']
        
        # Wind speed and wind power interaction
        if 'wind_speed_80m_avg' in df.columns and 'wind_forecast' in df.columns:
            df['wind_efficiency'] = np.where(
                df['wind_speed_80m_avg'] > 0,
                df['wind_forecast'] / (df['wind_speed_80m_avg'] ** 3),  # Cubic relationship for wind power
                0
            )
        
        # Hour and load interaction
        if 'hour' in df.columns and 'load_forecast' in df.columns:
            df['hour_load_interaction'] = df['hour'] * df['load_forecast']
        
        logger.info("Created interaction features")
        return df
    
    @staticmethod
    def engineer_features_pipeline(df: pd.DataFrame,
                                   include_lag: bool = True) -> pd.DataFrame:
        """
        Complete feature engineering pipeline
        
        Args:
            df: Input DataFrame
            include_lag: Whether to include lag features (may not be needed at inference time)
            
        Returns:
            DataFrame after feature engineering
        """
        logger.info("Starting feature engineering pipeline...")
        
        # 1. Time features
        df = FeatureEngineer.create_time_features(df)
        
        # 2. Market features
        df = FeatureEngineer.create_market_features(df)
        
        # 3. Lag features (training only)
        if include_lag and 'price' in df.columns:
            df = FeatureEngineer.create_lag_features(df, 'price')
        
        # 4. Interaction features
        df = FeatureEngineer.create_interaction_features(df)
        
        logger.info(f"Feature engineering complete, total {df.shape[1]} columns")
        return df


def main():
    """Test function"""
    # Create test data
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
    
    # Feature engineering
    df_engineered = FeatureEngineer.engineer_features_pipeline(df)
    
    print(f"原始列数: {len(['timestamp', 'price', 'load_forecast', 'wind_forecast', 'solar_forecast'])}")
    print(f"特征工程后列数: {df_engineered.shape[1]}")
    print(f"\n新增特征列:")
    print(df_engineered.columns.tolist())


if __name__ == "__main__":
    main()

