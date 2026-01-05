"""
ENTSO-E数据获取客户端
"""
from entsoe import EntsoePandasClient
import pandas as pd
from datetime import datetime, timedelta
from config.settings import ENTSOE_API_KEY, BIDDING_ZONE, TIMEZONE
import time
from tenacity import retry, wait_exponential, stop_after_attempt
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ENTSOEClient:
    """ENTSO-E Transparency Platform数据客户端"""
    
    def __init__(self, api_key: str = None):
        """
        初始化ENTSO-E客户端
        
        Args:
            api_key: ENTSO-E API密钥,如果未提供则从环境变量读取
        """
        self.api_key = api_key or ENTSOE_API_KEY
        if not self.api_key:
            raise ValueError("ENTSO-E API key未设置,请在.env文件中配置ENTSOE_API_KEY")
        
        self.client = EntsoePandasClient(api_key=self.api_key)
        self.bidding_zone = BIDDING_ZONE
        
    @retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(3))
    def fetch_day_ahead_prices(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        获取日前市场价格
        
        Args:
            start: 开始时间(aware timezone)
            end: 结束时间(aware timezone)
            
        Returns:
            DataFrame with columns: timestamp, price
        """
        try:
            logger.info(f"获取日前价格: {start} 到 {end}")
            prices = self.client.query_day_ahead_prices(
                self.bidding_zone, 
                start=start, 
                end=end
            )
            
            # 转换为DataFrame - 最安全的方式
            if isinstance(prices, (pd.Series, pd.DataFrame)):
                # 先重置索引，将时间戳变为一列
                df = prices.reset_index()
                # 无论原来叫什么（'index', 'MTU', 'timestamp'），统一重命名
                df.columns = ['timestamp', 'price'] if df.shape[1] == 2 else ['timestamp'] + [f'price_{i}' for i in range(df.shape[1]-1)]
                # 如果有多列价格，取第一列
                if df.shape[1] > 2:
                    df = df[['timestamp', df.columns[1]]].rename(columns={df.columns[1]: 'price'})
            else:
                raise ValueError(f"返回了意外的数据类型: {type(prices)}")
            
            # 确保时间戳是 datetime 类型
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            logger.info(f"成功获取 {len(df)} 条价格数据")
            return df
            
        except Exception as e:
            logger.error(f"获取日前价格失败: {e}")
            raise
    
    @retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(3))
    def fetch_load_forecast(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        获取总负载预测
        
        Args:
            start: 开始时间
            end: 结束时间
            
        Returns:
            DataFrame with columns: timestamp, load_forecast
        """
        try:
            logger.info(f"获取负载预测: {start} 到 {end}")
            load = self.client.query_load_forecast(
                self.bidding_zone,
                start=start,
                end=end
            )
            
            # 处理DataFrame和Series两种情况
            if isinstance(load, pd.DataFrame):
                # 如果是多列（如不同的预测版本），取平均值
                if load.shape[1] > 1:
                    load = load.mean(axis=1)
                    logger.info("负载预测有多个版本，已取平均值")
            
            # 使用 reset_index 转换为 DataFrame
            df = load.reset_index()
            df.columns = ['timestamp', 'load_forecast']
            
            # 确保时间戳是 datetime 类型
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            logger.info(f"成功获取 {len(df)} 条负载预测数据")
            return df
            
        except Exception as e:
            logger.error(f"获取负载预测失败: {e}")
            raise
    
    @retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(3))
    def fetch_wind_solar_forecast(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        获取风电和光伏发电预测
        
        Args:
            start: 开始时间
            end: 结束时间
            
        Returns:
            DataFrame with columns: timestamp, wind_forecast, solar_forecast
        """
        try:
            logger.info(f"获取风光预测: {start} 到 {end}")
            
            # 获取风电和光伏预测
            data = self.client.query_wind_and_solar_forecast(
                self.bidding_zone,
                start=start,
                end=end,
                psr_type=None  # 获取所有类型
            )
            
            # 初始化结果DataFrame
            result_df = pd.DataFrame(index=data.index)
            
            # 提取风电数据（可能有多种类型）
            wind_total = 0
            wind_columns = []
            
            for col in data.columns:
                if 'wind' in col.lower():
                    wind_columns.append(col)
                    if isinstance(data[col], pd.Series):
                        wind_total = wind_total + data[col] if isinstance(wind_total, pd.Series) else data[col]
                    
            if isinstance(wind_total, pd.Series) and len(wind_total) > 0:
                result_df['wind_forecast'] = wind_total
                logger.info(f"风电数据来源: {wind_columns}")
            else:
                result_df['wind_forecast'] = 0
                logger.warning("未找到风电数据，填充为0")
            
            # 提取光伏数据
            if 'Solar' in data.columns:
                result_df['solar_forecast'] = data['Solar']
                logger.info("光伏数据来源: ['Solar']")
            elif 'solar' in [c.lower() for c in data.columns]:
                # 查找小写solar列
                solar_col = [c for c in data.columns if 'solar' in c.lower()][0]
                result_df['solar_forecast'] = data[solar_col]
                logger.info(f"光伏数据来源: ['{solar_col}']")
            else:
                result_df['solar_forecast'] = 0
                logger.warning("未找到光伏数据，填充为0")
            
            # 重置索引并选择需要的列
            result_df = result_df.reset_index()
            
            # 自动识别时间列名
            if 'index' in result_df.columns:
                result_df = result_df.rename(columns={'index': 'timestamp'})
            elif result_df.columns[0] != 'timestamp':
                # 第一列就是时间戳
                result_df = result_df.rename(columns={result_df.columns[0]: 'timestamp'})
            
            # 确保时间戳是 datetime 类型
            result_df['timestamp'] = pd.to_datetime(result_df['timestamp'])
            result_df = result_df[['timestamp', 'wind_forecast', 'solar_forecast']]
            
            logger.info(f"成功获取 {len(result_df)} 条风光预测数据")
            logger.info(f"  风电范围: {result_df['wind_forecast'].min():.1f} - {result_df['wind_forecast'].max():.1f} MW")
            logger.info(f"  光伏范围: {result_df['solar_forecast'].min():.1f} - {result_df['solar_forecast'].max():.1f} MW")
            return result_df
            
        except Exception as e:
            logger.error(f"获取风光预测失败: {e}")
            # 返回空DataFrame避免管道中断
            logger.warning("返回空风光预测数据")
            return pd.DataFrame(columns=['timestamp', 'wind_forecast', 'solar_forecast'])
    
    def fetch_all_market_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取所有市场数据并合并
        
        Args:
            start_date: 开始日期字符串 'YYYY-MM-DD'
            end_date: 结束日期字符串 'YYYY-MM-DD'
            
        Returns:
            合并后的完整DataFrame
        """
        # 转换为timezone-aware timestamps
        start = pd.Timestamp(start_date, tz=TIMEZONE)
        end = pd.Timestamp(end_date, tz=TIMEZONE)
        
        # 获取各类数据
        prices_df = self.fetch_day_ahead_prices(start, end)
        load_df = self.fetch_load_forecast(start, end)
        wind_solar_df = self.fetch_wind_solar_forecast(start, end)
        
        # 记录数据形状
        logger.info(f"数据形状: 价格={len(prices_df)}, 负载={len(load_df)}, 风光={len(wind_solar_df)}")
        
        # 合并数据
        df = prices_df.merge(load_df, on='timestamp', how='left')
        logger.info(f"价格+负载合并后: {len(df)} 条记录")
        
        df = df.merge(wind_solar_df, on='timestamp', how='left')
        logger.info(f"最终合并后: {len(df)} 条记录")
        
        # 填充缺失值（使用新版pandas语法）
        df = df.ffill().bfill()
        
        logger.info(f"✅ 合并完成，共 {len(df)} 条记录")
        return df


def main():
    """测试函数"""
    client = ENTSOEClient()
    
    # 测试获取最近3天的数据
    end = pd.Timestamp.now(tz=TIMEZONE)
    start = end - pd.Timedelta(days=3)
    
    df = client.fetch_all_market_data(
        start.strftime('%Y-%m-%d'),
        end.strftime('%Y-%m-%d')
    )
    
    print(df.head())
    print(f"\n数据形状: {df.shape}")
    print(f"\n列名: {df.columns.tolist()}")


if __name__ == "__main__":
    main()

