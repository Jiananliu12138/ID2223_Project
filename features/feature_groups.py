"""
Hopsworks特征组管理
"""
import hopsworks
import pandas as pd
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


class FeatureStoreManager:
    """Hopsworks Feature Store管理器"""
    
    def __init__(self, api_key: str = None, project_name: str = None):
        """
        初始化Feature Store连接
        
        Args:
            api_key: Hopsworks API key
            project_name: Hopsworks项目名称
        """
        self.api_key = api_key or HOPSWORKS_API_KEY
        self.project_name = project_name or HOPSWORKS_PROJECT_NAME
        
        if not self.api_key:
            raise ValueError("Hopsworks API key未设置,请在.env文件中配置")
        
        logger.info(f"连接到Hopsworks项目: {self.project_name}")
        self.project = hopsworks.login(
            api_key_value=self.api_key,
            project=self.project_name
        )
        self.fs = self.project.get_feature_store()
        logger.info("Hopsworks连接成功")
    
    def create_electricity_feature_group(self, df: pd.DataFrame, 
                                        online: bool = False) -> None:
        """
        创建或获取电力市场特征组
        
        Args:
            df: 包含电力市场数据的DataFrame
            online: 是否启用在线特征存储
        """
        logger.info(f"创建/更新特征组: {ELECTRICITY_FG_NAME}")
        
        # 使用 get_or_create，如果表已存在的错误则忽略
        try:
            fg = self.fs.get_or_create_feature_group(
                name=ELECTRICITY_FG_NAME,
                version=FEATURE_GROUP_VERSION,
                description="电力市场数据:价格、负载、发电预测",
                primary_key=['timestamp'],
                event_time='timestamp',
                online_enabled=online,
                statistics_config={
                    "enabled": True,
                    "histograms": True,
                    "correlations": True
                }
            )
            logger.info(f"特征组 {ELECTRICITY_FG_NAME} 获取/创建成功")
        except Exception as e:
            # 如果是"表已存在"错误，尝试直接获取
            if "already exists" in str(e):
                logger.warning(f"表已存在错误，尝试直接获取: {e}")
                fg = self.fs.get_feature_group(
                    name=ELECTRICITY_FG_NAME,
                    version=FEATURE_GROUP_VERSION
                )
                if fg is None:
                    raise ValueError(f"无法获取特征组 {ELECTRICITY_FG_NAME}")
                logger.info(f"成功获取已存在的特征组 {ELECTRICITY_FG_NAME}")
            else:
                raise
        
        # 插入数据
        logger.info(f"插入 {len(df)} 条记录到 {ELECTRICITY_FG_NAME}")
        fg.insert(df, write_options={"wait_for_job": True})
        
        logger.info(f"特征组 {ELECTRICITY_FG_NAME} 更新成功")
    
    def create_weather_feature_group(self, df: pd.DataFrame,
                                     online: bool = False) -> None:
        """
        创建或获取天气特征组
        
        Args:
            df: 包含天气数据的DataFrame
            online: 是否启用在线特征存储
        """
        logger.info(f"创建/更新特征组: {WEATHER_FG_NAME}")
        
        # 使用 get_or_create，如果表已存在的错误则忽略
        try:
            fg = self.fs.get_or_create_feature_group(
                name=WEATHER_FG_NAME,
                version=FEATURE_GROUP_VERSION,
                description="SE3区域加权平均天气数据",
                primary_key=['timestamp'],
                event_time='timestamp',
                online_enabled=online,
                statistics_config={
                    "enabled": True,
                    "histograms": True,
                    "correlations": True
                }
            )
            logger.info(f"特征组 {WEATHER_FG_NAME} 获取/创建成功")
        except Exception as e:
            # 如果是"表已存在"错误，尝试直接获取
            if "already exists" in str(e):
                logger.warning(f"表已存在错误，尝试直接获取: {e}")
                fg = self.fs.get_feature_group(
                    name=WEATHER_FG_NAME,
                    version=FEATURE_GROUP_VERSION
                )
                if fg is None:
                    raise ValueError(f"无法获取特征组 {WEATHER_FG_NAME}")
                logger.info(f"成功获取已存在的特征组 {WEATHER_FG_NAME}")
            else:
                raise
        
        logger.info(f"插入 {len(df)} 条记录到 {WEATHER_FG_NAME}")
        fg.insert(df, write_options={"wait_for_job": True})
        
        logger.info(f"特征组 {WEATHER_FG_NAME} 更新成功")
    
    def get_feature_view(self, name: str = "electricity_price_fv",
                        version: int = 1) -> object:
        """
        获取或创建特征视图
        
        Args:
            name: 特征视图名称
            version: 版本号
            
        Returns:
            FeatureView对象
        """
        try:
            # 尝试获取现有特征视图
            fv = self.fs.get_feature_view(name=name, version=version)
            logger.info(f"获取现有特征视图: {name} v{version}")
            return fv
        except:
            logger.info(f"创建新特征视图: {name}")
            
            # 获取特征组
            electricity_fg = self.fs.get_feature_group(
                name=ELECTRICITY_FG_NAME,
                version=FEATURE_GROUP_VERSION
            )
            weather_fg = self.fs.get_feature_group(
                name=WEATHER_FG_NAME,
                version=FEATURE_GROUP_VERSION
            )
            
            # 创建查询(JOIN两个特征组)
            query = electricity_fg.select_all().join(
                weather_fg.select_all(),
                on=['timestamp']
            )
            
            # 创建特征视图
            fv = self.fs.create_feature_view(
                name=name,
                version=version,
                description="电力价格预测完整特征视图",
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

