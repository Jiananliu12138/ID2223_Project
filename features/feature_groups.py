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
    FEATURE_GROUP_VERSION
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

    def get_feature_view(self, name: str = "electricity_price_fv", version: int = 1):
        """获取或创建特征视图"""
        try:
            # 尝试获取现有特征视图
            fv = self.fs.get_feature_view(name=name, version=version)
            logger.info(f"获取现有特征视图: {name} v{version}")
            return fv
        except:
            logger.info(f"Creating new feature view: {name}")
            fg1 = self.fs.get_feature_group(ELECTRICITY_FG_NAME, FEATURE_GROUP_VERSION)
            fg2 = self.fs.get_feature_group(WEATHER_FG_NAME, FEATURE_GROUP_VERSION)
            query = fg1.select_all().join(fg2.select_all(), on=['timestamp'])
            return self.fs.create_feature_view(
                name=name,
                version=version,
                labels=['price'],
                query=query
            )
            
            logger.info(f"特征视图 {name} 创建成功")
            return fv
    
    def read_feature_data(self, 
                         start_time: str = None,
                         end_time: str = None) -> pd.DataFrame:
        """
        从特征视图读取数据
        
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

