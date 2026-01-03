"""
Hopsworks特征组管理
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

# 本地数据目录
LOCAL_DATA_DIR = Path("data/local_cache")
LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)


class FeatureStoreManager:
    """Hopsworks Feature Store管理器"""
    
    def __init__(self, api_key: str = None, project_name: str = None, local_only: bool = False):
        """
        初始化Feature Store连接
        
        Args:
            api_key: Hopsworks API key
            project_name: Hopsworks项目名称
            local_only: 是否仅本地模式（不连接Hopsworks）
        """
        self.local_only = local_only
        
        if not local_only:
            # 在线模式：连接到 Hopsworks
            self.api_key = api_key or HOPSWORKS_API_KEY
            self.project_name = project_name or HOPSWORKS_PROJECT_NAME
            
            if not self.api_key:
                raise ValueError("Hopsworks API key未设置,请在.env文件中配置")
            
            logger.info(f"Connecting to Hopsworks project: {self.project_name}")
            self.project = hopsworks.login(
                api_key_value=self.api_key,
                project=self.project_name
            )
            self.fs = self.project.get_feature_store()
            logger.info("✅ Hopsworks connection successful")
        else:
            # 本地模式：不连接
            logger.info("📁 Local-only mode: data will be saved locally")
    
    def save_electricity_data_local(self, df: pd.DataFrame, month_str: str) -> str:
        """
        保存电力市场数据到本地
        
        Args:
            df: 数据DataFrame
            month_str: 月份标识，如 '2024-01'
            
        Returns:
            保存的文件路径
        """
        filepath = LOCAL_DATA_DIR / f"electricity_{month_str}.parquet"
        df.to_parquet(filepath, index=False)
        logger.info(f"💾 电力数据已保存到: {filepath}")
        return str(filepath)
    
    def create_electricity_feature_group(self, df: pd.DataFrame) -> None:
        """创建或获取电力市场特征组 (极简版)"""
        logger.info(f"\n🔄 Creating/updating Feature Group: {ELECTRICITY_FG_NAME}")
        # Ensure numeric columns are float to create FG with float types
        try:
            for col in ['price', 'load_forecast', 'wind_forecast', 'solar_forecast']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
        except Exception as e:
            logger.warning(f"Failed to cast electricity numeric columns to float: {e}")

        # 完全参考示例代码语法
        electricity_fg = self.fs.get_or_create_feature_group(
            name=ELECTRICITY_FG_NAME,
            version=FEATURE_GROUP_VERSION,
            description="Electricity market data: day-ahead price, load forecast, wind and solar forecast",
            primary_key=['timestamp'],
            event_time="timestamp"
        )

        logger.info(f"✅ Feature Group '{electricity_fg.name}' ready")
        logger.info(f"📤 Inserting {len(df)} rows of electricity data...")

        # 插入数据
        electricity_fg.insert(df, wait=True)
        logger.info("✅ Electricity data inserted successfully!")
    
    def save_weather_data_local(self, df: pd.DataFrame, month_str: str) -> str:
        """
        保存天气数据到本地
        
        Args:
            df: 数据DataFrame
            month_str: 月份标识，如 '2024-01'
            
        Returns:
            保存的文件路径
        """
        filepath = LOCAL_DATA_DIR / f"weather_{month_str}.parquet"
        df.to_parquet(filepath, index=False)
        logger.info(f"💾 天气数据已保存到: {filepath}")
        return str(filepath)
    
    def create_weather_feature_group(self, df: pd.DataFrame) -> None:
        """创建或获取天气特征组 (极简版)"""
        logger.info(f"\n🔄 Creating/updating Feature Group: {WEATHER_FG_NAME}")
        
        # 完全参考示例代码语法
        weather_fg = self.fs.get_or_create_feature_group(
            name=WEATHER_FG_NAME,
            version=FEATURE_GROUP_VERSION,
            description="SE3 region weighted average weather data: temperature, wind speed, solar irradiance",
            primary_key=['timestamp'],
            event_time="timestamp"
        )
        
        logger.info(f"✅ Feature Group '{weather_fg.name}' ready")
        logger.info(f"📤 Inserting {len(df)} rows of weather data...")

        # Ensure numeric weather columns are float so the new FG version uses float types
        try:
            for col in ['temperature_avg', 'wind_speed_10m_avg', 'wind_speed_80m_avg', 'irradiance_avg']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
        except Exception as e:
            logger.warning(f"Failed to cast weather numeric columns to float: {e}")

        # 插入数据
        weather_fg.insert(df, wait=True)
        logger.info("✅ Weather data inserted successfully!")

    def create_engineered_feature_group(self, df: pd.DataFrame) -> None:
        """创建或获取工程特征组 (极简版)"""
        logger.info(f"\n🔄 Creating/updating Feature Group: {ENGINEERED_FG_NAME}")
        
        # Ensure all numeric columns are float64 type
        try:
            numeric_cols = df.select_dtypes(include=['int64', 'float64', 'int32', 'float32']).columns
            for col in numeric_cols:
                if col != 'timestamp':  # Skip timestamp
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
        except Exception as e:
            logger.warning(f"Failed to cast numeric columns to float: {e}")
        
        # 完全参考示例代码语法
        engineered_fg = self.fs.get_or_create_feature_group(
            name=ENGINEERED_FG_NAME,
            version=ENGINEERED_FG_VERSION,
            description="Engineered features for electricity price prediction: time, supply-demand, lag, and interaction features",
            primary_key=['timestamp'],
            event_time="timestamp"
        )
        
        logger.info(f"✅ Feature Group '{engineered_fg.name}' ready")
        logger.info(f"📤 Inserting {len(df)} rows of engineered features...")
        
        # 插入数据
        engineered_fg.insert(df, wait=True)
        logger.info("✅ Engineered features inserted successfully!")

    def get_feature_view(self, name: str = "electricity_price_fv", version: int = 1):
        """获取或创建特征视图（原始特征：electricity + weather）"""
        logger.info(f"💾 Creating/Getting Feature View: {name} v{version}")
        
        # 获取两个 Feature Groups
        logger.info(f"  📋 Getting Feature Groups...")
        electricity_fg = self.fs.get_feature_group(ELECTRICITY_FG_NAME, FEATURE_GROUP_VERSION)
        weather_fg = self.fs.get_feature_group(WEATHER_FG_NAME, FEATURE_GROUP_VERSION)
        
        # 创建联合查询
        logger.info("  🔍 Creating feature query (join electricity + weather)...")
        selected_features = electricity_fg.select_all().join(
            weather_fg.select_all(), 
            on=['timestamp']
        )
        
        # 使用 get_or_create_feature_view 创建或获取 Feature View
        logger.info(f"  ✨ Creating Feature View with 'price' as label...")
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
        获取或创建工程特征视图（用于模型训练）
        
        Args:
            name: Feature View 名称
            version: Feature View 版本号
            
        Returns:
            Feature View 对象
        """
        logger.info(f"💾 Creating/Getting Feature View: {name} v{version}")
        
        # 获取工程特征 Feature Group
        logger.info(f"  📋 Getting Feature Group: {ENGINEERED_FG_NAME} v{ENGINEERED_FG_VERSION}")
        engineered_fg = self.fs.get_feature_group(
            name=ENGINEERED_FG_NAME, 
            version=ENGINEERED_FG_VERSION
        )
        
        # 创建查询（选择所有特征）
        logger.info("  🔍 Creating feature query (select all features)...")
        selected_features = engineered_fg.select_all()
        
        # 使用 get_or_create_feature_view 创建或获取 Feature View
        logger.info(f"  ✨ Creating Feature View with 'price' as label...")
        feature_view = self.fs.get_or_create_feature_view(
            name=name,
            description="Engineered features for electricity price prediction with price as target",
            version=version,
            labels=['price'],  # price 是目标变量
            query=selected_features
        )
        
        logger.info(f"✅ Feature View '{name}' ready!")
        return feature_view
    
    def read_raw_feature_groups(self, 
                                 start_time: str = None,
                                 end_time: str = None) -> pd.DataFrame:
        """
        直接从 Feature Groups 读取原始数据并合并
        
        Args:
            start_time: 开始时间 'YYYY-MM-DD HH:MM:SS'
            end_time: 结束时间 'YYYY-MM-DD HH:MM:SS'
            
        Returns:
            合并后的 DataFrame（electricity + weather）
        """
        logger.info("📖 从 Feature Groups 读取原始数据...")
        
        # 1. 获取 Feature Groups
        logger.info(f"  获取 Feature Group: {ELECTRICITY_FG_NAME} v{FEATURE_GROUP_VERSION}")
        electricity_fg = self.fs.get_feature_group(ELECTRICITY_FG_NAME, FEATURE_GROUP_VERSION)
        
        logger.info(f"  获取 Feature Group: {WEATHER_FG_NAME} v{FEATURE_GROUP_VERSION}")
        weather_fg = self.fs.get_feature_group(WEATHER_FG_NAME, FEATURE_GROUP_VERSION)
        
        # 2. 读取数据
        if start_time and end_time:
            logger.info(f"  时间范围: {start_time} 到 {end_time}")
            # 使用 read() 读取所有数据，然后在 pandas 中过滤（更可靠）
            electricity_df = electricity_fg.read()
            weather_df = weather_fg.read()
            
            # 在 pandas 中过滤时间范围
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
            logger.info("  读取所有数据")
            electricity_df = electricity_fg.read()
            weather_df = weather_fg.read()
        
        logger.info(f"  ✅ 电力数据: {len(electricity_df)} 行")
        logger.info(f"  ✅ 天气数据: {len(weather_df)} 行")
        
        # 3. 合并数据
        logger.info("  合并电力和天气数据...")
        df = pd.merge(electricity_df, weather_df, on='timestamp', how='inner')
        
        logger.info(f"  ✅ 合并后: {len(df)} 行, {len(df.columns)} 列")
        
        return df
    
    def read_feature_data(self, 
                         start_time: str = None,
                         end_time: str = None) -> pd.DataFrame:
        """
        从特征视图读取数据（用于已有 Feature View 的情况）
        
        Args:
            start_time: 开始时间 'YYYY-MM-DD HH:MM:SS'
            end_time: 结束时间 'YYYY-MM-DD HH:MM:SS'
            
        Returns:
            合并后的DataFrame
        """
        fv = self.get_feature_view()
        
        if start_time and end_time:
            logger.info(f"读取特征数据: {start_time} 到 {end_time}")
            df = fv.get_batch_data(start_time=start_time, end_time=end_time)
        else:
            logger.info("读取所有特征数据")
            df = fv.get_batch_data()
        
        logger.info(f"读取了 {len(df)} 条记录")
        return df
    
    def get_training_data(self, 
                         test_size: float = 0.2) -> tuple:
        """
        获取训练数据(已分割)
        
        Args:
            test_size: 测试集比例
            
        Returns:
            (X_train, X_test, y_train, y_test, feature_names)
        """
        fv = self.get_feature_view()
        
        # 获取训练数据
        X_train, X_test, y_train, y_test = fv.train_test_split(test_size=test_size)
        
        logger.info(f"训练集大小: {len(X_train)}, 测试集大小: {len(X_test)}")
        
        return X_train, X_test, y_train, y_test
    
    def get_model_registry(self):
        """获取模型注册表"""
        return self.project.get_model_registry()


def main():
    """测试函数"""
    # 测试连接
    try:
        fsm = FeatureStoreManager()
        logger.info("Hopsworks连接测试成功!")
        
        # 获取特征组列表
        feature_groups = fsm.fs.get_feature_groups()
        logger.info(f"现有特征组数量: {len(feature_groups)}")
        
    except Exception as e:
        logger.error(f"连接失败: {e}")


if __name__ == "__main__":
    main()

