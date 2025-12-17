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
            
            # 转换为DataFrame
            df = pd.DataFrame({
                'timestamp': prices.index,
                'price': prices.values
            })
            
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
            
            df = pd.DataFrame({
                'timestamp': load.index,
                'load_forecast': load.values
            })
            
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
            
            # 获取风电预测
            wind = self.client.query_wind_and_solar_forecast(
                self.bidding_zone,
                start=start,
                end=end,
                psr_type=None  # 获取所有类型
            )
            
            df = pd.DataFrame(index=wind.index)
            
            # 提取风电和光伏数据
            if 'Wind Onshore' in wind.columns:
                df['wind_onshore'] = wind['Wind Onshore']
            if 'Wind Offshore' in wind.columns:
                df['wind_offshore'] = wind['Wind Offshore']
            if 'Solar' in wind.columns:
                df['solar_forecast'] = wind['Solar']
            
            # 计算总风电
            wind_cols = [col for col in df.columns if 'wind' in col.lower()]
            if wind_cols:
                df['wind_forecast'] = df[wind_cols].sum(axis=1)
            else:
                df['wind_forecast'] = 0
            
            # 如果没有光伏数据,填充为0
            if 'solar_forecast' not in df.columns:
                df['solar_forecast'] = 0
            
            # 重置索引
            df = df.reset_index().rename(columns={'index': 'timestamp'})
            df = df[['timestamp', 'wind_forecast', 'solar_forecast']]
            
            logger.info(f"成功获取 {len(df)} 条风光预测数据")
            return df
            
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
        
        # 合并数据
        df = prices_df.merge(load_df, on='timestamp', how='left')
        df = df.merge(wind_solar_df, on='timestamp', how='left')
        
        # 填充缺失值
        df = df.fillna(method='ffill').fillna(method='bfill')
        
        logger.info(f"合并后共 {len(df)} 条记录")
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

