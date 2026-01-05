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
        获取日前市场价格 - 采用分段获取策略以绕过 entsoe-py 内部长度不匹配错误
        """
        try:
            logger.info(f"获取日前价格: {start} 到 {end}")
            
            # 如果跨度超过1天，采用逐日获取策略
            all_dfs = []
            current_start = start
            
            while current_start < end:
                # 每次取 24 小时
                current_end = min(current_start + pd.Timedelta(days=1), end)
                
                try:
                    prices = self.client.query_day_ahead_prices(
                        self.bidding_zone, 
                        start=current_start, 
                        end=current_end
                    )
                    
                    if isinstance(prices, (pd.Series, pd.DataFrame)):
                        temp_df = prices.reset_index()
                        temp_df.columns = ['timestamp', 'price'] if temp_df.shape[1] == 2 else ['timestamp'] + [f'price_{i}' for i in range(temp_df.shape[1]-1)]
                        if temp_df.shape[1] > 2:
                            temp_df = temp_df[['timestamp', temp_df.columns[1]]].rename(columns={temp_df.columns[1]: 'price'})
                        all_dfs.append(temp_df)
                except Exception as day_e:
                    logger.warning(f"  ⚠️ 获取时段 {current_start} 数据失败: {day_e}，跳过该时段")
                
                current_start = current_end

            if not all_dfs:
                raise ValueError("未能获取到任何价格数据")
                
            # 合并所有片段并去重
            df = pd.concat(all_dfs, ignore_index=True)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # 核心步骤：彻底去重并按时间排序
            df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
            
            logger.info(f"成功获取 {len(df)} 条价格数据 (已去重)")
            return df
            
        except Exception as e:
            logger.error(f"获取日前价格失败: {e}")
            raise
    
    @retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(3))
    def fetch_load_forecast(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        获取总负载预测 - 采用分段获取策略
        """
        try:
            logger.info(f"获取负载预测: {start} 到 {end}")
            
            all_dfs = []
            current_start = start
            
            while current_start < end:
                current_end = min(current_start + pd.Timedelta(days=1), end)
                
                try:
                    load = self.client.query_load_forecast(
                        self.bidding_zone,
                        start=current_start,
                        end=current_end
                    )
                    
                    if isinstance(load, pd.DataFrame):
                        if load.shape[1] > 1:
                            load = load.mean(axis=1)
                    
                    temp_df = load.reset_index()
                    temp_df.columns = ['timestamp', 'load_forecast']
                    all_dfs.append(temp_df)
                except Exception as day_e:
                    logger.warning(f"  ⚠️ 获取时段 {current_start} 负载失败: {day_e}")
                
                current_start = current_end

            if not all_dfs:
                raise ValueError("未能获取到任何负载预测数据")

            df = pd.concat(all_dfs, ignore_index=True)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
            
            logger.info(f"成功获取 {len(df)} 条负载预测数据 (已去重)")
            return df
    
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

