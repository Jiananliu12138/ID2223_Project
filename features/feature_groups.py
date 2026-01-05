"""
Hopsworks Feature Store management
"""
import hopsworks
import pandas as pd
import os
from pathlib import Path
from config.settings import (
    HOPSWORKS_API_KEY, 
    HOPSWORKS_PROJECT_NAME,
    ELECTRICITY_FG_NAME,
    WEATHER_FG_NAME,
    FEATURE_GROUP_VERSION,
    ENGINEERED_FG_NAME,
    ENGINEERED_FG_VERSION
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Local data directory
LOCAL_DATA_DIR = Path("data/local_cache")
LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)


class FeatureStoreManager:
    """Hopsworks Feature Store manager"""
    
    def __init__(self, api_key: str = None, project_name: str = None, local_only: bool = False):
        """
        Initialize Feature Store connection
        
        Args:
            api_key: Hopsworks API key
            project_name: Hopsworks project name
            local_only: Whether to use local-only mode (no Hopsworks connection)
        """
        self.local_only = local_only
        
        if not local_only:
            # Online mode: connect to Hopsworks
            self.api_key = api_key or HOPSWORKS_API_KEY
            self.project_name = project_name or HOPSWORKS_PROJECT_NAME
            
            if not self.api_key:
                raise ValueError("Hopsworks API key is not set, please configure it in .env file")
            
            logger.info(f"Connecting to Hopsworks project: {self.project_name}")
            self.project = hopsworks.login(
                api_key_value=self.api_key,
                project=self.project_name
            )
            self.fs = self.project.get_feature_store()
            logger.info("✅ Hopsworks connection successful")
        else:
            # Local mode: no connection
            logger.info("📁 Local-only mode: data will be saved locally")
    
    def save_electricity_data_local(self, df: pd.DataFrame, month_str: str) -> str:
        """
        Save electricity market data locally
        
        Args:
            df: Data DataFrame
            month_str: Month identifier, e.g., '2024-01'
            
        Returns:
            Path of saved file
        """
        filepath = LOCAL_DATA_DIR / f"electricity_{month_str}.parquet"
        df.to_parquet(filepath, index=False)
        logger.info(f"💾 Electricity data saved to: {filepath}")
        return str(filepath)
    
    def create_electricity_feature_group(self, df: pd.DataFrame) -> None:
        """Create or get electricity market feature group (minimal version)"""
        logger.info(f"\n🔄 Creating/updating Feature Group: {ELECTRICITY_FG_NAME}")
        
        # Create a copy to avoid SettingWithCopyWarning
        df = df.copy()
        
        # Ensure numeric columns are float to create FG with float types
        try:
            for col in ['price', 'load_forecast', 'wind_forecast', 'solar_forecast']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
        except Exception as e:
            logger.warning(f"Failed to cast electricity numeric columns to float: {e}")

        # Follow example code syntax exactly
        electricity_fg = self.fs.get_or_create_feature_group(
            name=ELECTRICITY_FG_NAME,
            version=FEATURE_GROUP_VERSION,
            description="Electricity market data: day-ahead price, load forecast, wind and solar forecast",
            primary_key=['timestamp'],
            event_time="timestamp"
        )

        logger.info(f"✅ Feature Group '{electricity_fg.name}' ready")
        logger.info(f"📤 Inserting {len(df)} rows of electricity data...")

        # Insert data
        electricity_fg.insert(df, wait=True)
        logger.info("✅ Electricity data inserted successfully!")
    
    def save_weather_data_local(self, df: pd.DataFrame, month_str: str) -> str:
        """
        Save weather data locally
        
        Args:
            df: Data DataFrame
            month_str: Month identifier, e.g., '2024-01'
            
        Returns:
            Path of saved file
        """
        filepath = LOCAL_DATA_DIR / f"weather_{month_str}.parquet"
        df.to_parquet(filepath, index=False)
        logger.info(f"💾 Weather data saved to: {filepath}")
        return str(filepath)
    
    def create_weather_feature_group(self, df: pd.DataFrame) -> None:
        """Create or get weather feature group (minimal version)"""
        logger.info(f"\n🔄 Creating/updating Feature Group: {WEATHER_FG_NAME}")
        
        # Create a copy to avoid SettingWithCopyWarning
        df = df.copy()
        
        # Ensure numeric weather columns are float so the new FG version uses float types
        try:
            for col in ['temperature_avg', 'wind_speed_10m_avg', 'wind_speed_80m_avg', 'irradiance_avg']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
        except Exception as e:
            logger.warning(f"Failed to cast weather numeric columns to float: {e}")
        
        # Follow example code syntax exactly
        weather_fg = self.fs.get_or_create_feature_group(
            name=WEATHER_FG_NAME,
            version=FEATURE_GROUP_VERSION,
            description="SE3 region weighted average weather data: temperature, wind speed, solar irradiance",
            primary_key=['timestamp'],
            event_time="timestamp"
        )
        
        logger.info(f"✅ Feature Group '{weather_fg.name}' ready")
        logger.info(f"📤 Inserting {len(df)} rows of weather data...")

        # Insert data
        weather_fg.insert(df, wait=True)
        logger.info("✅ Weather data inserted successfully!")

    def create_engineered_feature_group(self, df: pd.DataFrame) -> None:
        """Create or get engineered feature group (minimal version)"""
        logger.info(f"\n🔄 Creating/updating Feature Group: {ENGINEERED_FG_NAME}")
        
        # Create a copy to avoid SettingWithCopyWarning
        df = df.copy()
        
        # Ensure all numeric columns are float64 type
        try:
            numeric_cols = df.select_dtypes(include=['int64', 'float64', 'int32', 'float32']).columns
            for col in numeric_cols:
                if col != 'timestamp':  # Skip timestamp
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
        except Exception as e:
            logger.warning(f"Failed to cast numeric columns to float: {e}")
        
        # Follow example code syntax exactly
        engineered_fg = self.fs.get_or_create_feature_group(
            name=ENGINEERED_FG_NAME,
            version=ENGINEERED_FG_VERSION,
            description="Engineered features for electricity price prediction: time, supply-demand, lag, and interaction features",
            primary_key=['timestamp'],
            event_time="timestamp"
        )
        
        logger.info(f"✅ Feature Group '{engineered_fg.name}' ready")
        logger.info(f"📤 Inserting {len(df)} rows of engineered features...")
        
        # Insert data
        engineered_fg.insert(df, wait=True)
        logger.info("✅ Engineered features inserted successfully!")

    def get_feature_view(self, name: str = "electricity_price_fv", version: int = 1):
        """Get or create feature view (raw features: electricity + weather)"""
        logger.info(f"💾 Creating/Getting Feature View: {name} v{version}")
        
        # Get the two Feature Groups
        logger.info(f"  📋 Getting Feature Groups...")
        electricity_fg = self.fs.get_feature_group(ELECTRICITY_FG_NAME, FEATURE_GROUP_VERSION)
        weather_fg = self.fs.get_feature_group(WEATHER_FG_NAME, FEATURE_GROUP_VERSION)
        
        # Create union query
        logger.info("  🔍 Creating feature query (join electricity + weather)...")
        selected_features = electricity_fg.select_all().join(
            weather_fg.select_all(), 
            on=['timestamp']
        )
        
        # Use get_or_create_feature_view to create or get Feature View
        logger.info(f"  ✨ Creating Feature View with 'price' as label...\")
        feature_view = self.fs.get_or_create_feature_view(
            name=name,
            description="Electricity market and weather features with price as target",
            version=version,
            labels=['price'],
            query=selected_features
        )
        
        logger.info(f"✅ Feature View '{name}' ready!")
        return feature_view
    
    def get_engineered_feature_view(self, name: str = "electricity_engineered_fv", version: int = 1):
        """
        Get or create engineered feature view (for model training)
        
        Args:
            name: Feature View name
            version: Feature View version number
            
        Returns:
            Feature View object
        """
        logger.info(f"💾 Creating/Getting Feature View: {name} v{version}")
        
        # Get engineered feature Feature Group
        logger.info(f"  📋 Getting Feature Group: {ENGINEERED_FG_NAME} v{ENGINEERED_FG_VERSION}")
        engineered_fg = self.fs.get_feature_group(
            name=ENGINEERED_FG_NAME, 
            version=ENGINEERED_FG_VERSION
        )
        
        # Create query (select all features)
        logger.info("  🔍 Creating feature query (select all features)...")
        selected_features = engineered_fg.select_all()
        
        # Use get_or_create_feature_view to create or get Feature View
        logger.info(f"  ✨ Creating Feature View with 'price' as label...")
        feature_view = self.fs.get_or_create_feature_view(
            name=name,
            description="Engineered features for electricity price prediction with price as target",
            version=version,
            labels=['price'],  # price is the target variable
            query=selected_features
        )
        
        logger.info(f"✅ Feature View '{name}' ready!")
        return feature_view
    
    def read_raw_feature_groups(self, 
                                 start_time: str = None,
                                 end_time: str = None) -> pd.DataFrame:
        """
        Read raw data directly from Feature Groups and merge
        
        Args:
            start_time: Start time 'YYYY-MM-DD HH:MM:SS'
            end_time: End time 'YYYY-MM-DD HH:MM:SS'
            
        Returns:
            Merged DataFrame (electricity + weather)
        """
        logger.info("📖 Reading raw data from Feature Groups...")
        
        # 1. Get Feature Groups
        logger.info(f"  Get Feature Group: {ELECTRICITY_FG_NAME} v{FEATURE_GROUP_VERSION}")
        electricity_fg = self.fs.get_feature_group(ELECTRICITY_FG_NAME, FEATURE_GROUP_VERSION)
        
        logger.info(f"  Get Feature Group: {WEATHER_FG_NAME} v{FEATURE_GROUP_VERSION}")
        weather_fg = self.fs.get_feature_group(WEATHER_FG_NAME, FEATURE_GROUP_VERSION)
        
        # 2. Read data
        if start_time and end_time:
            logger.info(f"  Time range: {start_time} to {end_time}")
            # Use read() to read all data, then filter in pandas (more reliable)
            electricity_df = electricity_fg.read()
            weather_df = weather_fg.read()
            
            # Filter time range in pandas
            electricity_df['timestamp'] = pd.to_datetime(electricity_df['timestamp'])
            weather_df['timestamp'] = pd.to_datetime(weather_df['timestamp'])
            
            electricity_df = electricity_df[
                (electricity_df['timestamp'] >= start_time) & 
                (electricity_df['timestamp'] <= end_time)
            ]
            weather_df = weather_df[
                (weather_df['timestamp'] >= start_time) & 
                (weather_df['timestamp'] <= end_time)
            ]
        else:
            logger.info("  Read all data")
            electricity_df = electricity_fg.read()
            weather_df = weather_fg.read()
        
        logger.info(f"  ✅ Electricity data: {len(electricity_df)} rows")
        logger.info(f"  ✅ Weather data: {len(weather_df)} rows")
        
        # 3. Merge data
        logger.info("  Merge electricity and weather data...")
        df = pd.merge(electricity_df, weather_df, on='timestamp', how='inner')
        
        logger.info(f"  ✅ After merge: {len(df)} rows, {len(df.columns)} columns")
        
        return df
    
    def read_feature_data(self, 
                         start_time: str = None,
                         end_time: str = None) -> pd.DataFrame:
        """
        Read data from feature view (for cases where Feature View already exists)
        
        Args:
            start_time: Start time 'YYYY-MM-DD HH:MM:SS'
            end_time: End time 'YYYY-MM-DD HH:MM:SS'
            
        Returns:
            Merged DataFrame
        """
        fv = self.get_feature_view()
        
        if start_time and end_time:
            logger.info(f"Read feature data: {start_time} to {end_time}")
            df = fv.get_batch_data(start_time=start_time, end_time=end_time)
        else:
            logger.info("Read all feature data")
            df = fv.get_batch_data()
        
        logger.info(f"Read {len(df)} records")
        return df
    
    def get_training_data(self, 
                         test_size: float = 0.2) -> tuple:
        """
        Get training data (already split)
        
        Args:
            test_size: Test set ratio
            
        Returns:
            (X_train, X_test, y_train, y_test, feature_names)
        """
        fv = self.get_feature_view()
        
        # Get training data
        X_train, X_test, y_train, y_test = fv.train_test_split(test_size=test_size)
        
        logger.info(f"Training set size: {len(X_train)}, test set size: {len(X_test)}")
        
        return X_train, X_test, y_train, y_test
    
    def get_model_registry(self):
        """Get model registry"""
        return self.project.get_model_registry()


def main():
    """Test function"""
    # Test connection
    try:
        fsm = FeatureStoreManager()
        logger.info("Hopsworks connection test successful!")
        
        # Get feature group list
        feature_groups = fsm.fs.get_feature_groups()
        logger.info(f"Number of existing feature groups: {len(feature_groups)}")
        
    except Exception as e:
        logger.error(f"Connection failed: {e}")


if __name__ == "__main__":
    main()

