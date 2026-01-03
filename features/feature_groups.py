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
        """
        创建或获取工程特征组（包含所有特征工程后的特征）
        
        Args:
            df: 经过特征工程处理的DataFrame，包含所有原始特征+工程特征
        """
        logger.info(f"\n🔄 Creating/updating Engineered Feature Group: {ENGINEERED_FG_NAME}")
        
        # 确保时间戳列存在
        if 'timestamp' not in df.columns:
            raise ValueError("DataFrame must contain 'timestamp' column")
        
        # 确保所有数值列都是 float64 类型
        numeric_cols = df.select_dtypes(include=['int64', 'float64', 'int32', 'float32']).columns
        for col in numeric_cols:
            if col != 'timestamp':  # 跳过时间戳
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
        
        logger.info(f"  特征数量: {len(df.columns)}")
        logger.info(f"  数据行数: {len(df)}")
        
        # 创建或获取 Feature Group
        engineered_fg = self.fs.get_or_create_feature_group(
            name=ENGINEERED_FG_NAME,
            version=ENGINEERED_FG_VERSION,
            description="完整特征工程后的电力价格预测特征集，包含时间特征、供需特征、滞后特征和交互特征",
            primary_key=['timestamp'],
            event_time="timestamp"
        )
        
        logger.info(f"✅ Feature Group '{engineered_fg.name}' ready")
        logger.info(f"📤 Inserting {len(df)} rows of engineered features...")
        
        # 插入数据
        engineered_fg.insert(df, wait=True)
        logger.info("✅ Engineered features inserted successfully!")

    def get_feature_view(self, name: str = "electricity_price_fv", version: int = 1):
        """获取或创建特征视图（原始特征）"""
        try:
            # 尝试获取现有特征视图
            fv = self.fs.get_feature_view(name=name, version=version)
            logger.info(f"✅ 获取现有特征视图: {name} v{version}")
            return fv
        except Exception as e:
            logger.info(f"🆕 Feature View 不存在，尝试创建: {name}")
            
            try:
                # 获取 Feature Groups
                logger.info(f"  获取 Feature Group: {ELECTRICITY_FG_NAME} v{FEATURE_GROUP_VERSION}")
                fg1 = self.fs.get_feature_group(ELECTRICITY_FG_NAME, FEATURE_GROUP_VERSION)
                
                logger.info(f"  获取 Feature Group: {WEATHER_FG_NAME} v{FEATURE_GROUP_VERSION}")
                fg2 = self.fs.get_feature_group(WEATHER_FG_NAME, FEATURE_GROUP_VERSION)
                
                if fg1 is None or fg2 is None:
                    raise ValueError(f"Feature Groups 不存在！请先上传数据。")
                
                # 创建联合查询
                logger.info("  创建联合查询...")
                query = fg1.select_all().join(fg2.select_all(), on=['timestamp'])
                
                # 创建 Feature View
                logger.info(f"  创建 Feature View: {name}")
                fv = self.fs.create_feature_view(
                    name=name,
                    version=version,
                    labels=['price'],
                    query=query
                )
                
                logger.info(f"✅ Feature View {name} 创建成功")
                return fv
                
            except Exception as create_error:
                logger.error(f"\n{'='*70}")
                logger.error(f"❌ 创建 Feature View 失败！")
                logger.error(f"错误信息: {create_error}")
                logger.error(f"{'='*70}")
                logger.error(f"\n⚠️  请先确保数据已上传到 Hopsworks：")
                logger.error(f"  1. 检查本地数据: ls data/local_features/")
                logger.error(f"  2. 上传数据: python pipelines/upload_to_hopsworks.py")
                logger.error(f"  3. 如果没有本地数据，先运行: python pipelines/1_backfill_features.py")
                logger.error(f"\n{'='*70}\n")
                raise RuntimeError(f"无法创建 Feature View，原因: {create_error}")
    
    def get_engineered_feature_view(self, name: str = "electricity_engineered_fv", version: int = 1):
        """
        获取或创建工程特征视图（用于模型训练）
        
        Args:
            name: Feature View 名称
            version: Feature View 版本号
            
        Returns:
            Feature View 对象
        """
        try:
            # 尝试获取现有特征视图
            fv = self.fs.get_feature_view(name=name, version=version)
            logger.info(f"✅ 获取现有工程特征视图: {name} v{version}")
            return fv
        except:
            logger.info(f"🆕 创建新的工程特征视图: {name} v{version}")
            
            # 获取工程特征 Feature Group
            engineered_fg = self.fs.get_feature_group(
                name=ENGINEERED_FG_NAME, 
                version=ENGINEERED_FG_VERSION
            )
            
            # 创建查询（选择所有特征）
            query = engineered_fg.select_all()
            
            # 创建 Feature View，price 作为标签
            fv = self.fs.create_feature_view(
                name=name,
                version=version,
                description="用于电力价格预测的完整工程特征视图",
                labels=['price'],  # price 是目标变量
                query=query
            )
            
            logger.info(f"✅ 工程特征视图 {name} 创建成功")
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

