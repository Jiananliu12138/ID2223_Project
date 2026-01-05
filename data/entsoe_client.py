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
        获取日前市场价格（增强版：带详细调试信息和容错处理）
        """
        logger.info(f"获取日前价格: {start} 到 {end}")
        
        try:
            prices = self.client.query_day_ahead_prices(
                self.bidding_zone, 
                start=start, 
                end=end
            )
            
            # 🔍 详细调试信息
            logger.info(f"  📊 原始数据类型: {type(prices)}")
            
            if isinstance(prices, pd.Series):
                logger.info(f"  📊 Series 长度: {len(prices)}")
                logger.info(f"  📊 Index 长度: {len(prices.index)}")
                logger.info(f"  📊 Values 长度: {len(prices.values)}")
                logger.info(f"  📊 Index 类型: {type(prices.index)}")
                logger.info(f"  📊 前3个时间戳: {list(prices.index[:3])}")
                logger.info(f"  📊 后3个时间戳: {list(prices.index[-3:])}")
                
                # 检查是否有重复的时间戳
                duplicates = prices.index.duplicated()
                if duplicates.any():
                    logger.warning(f"  ⚠️  发现 {duplicates.sum()} 个重复时间戳！")
                    # 去重：保留第一个
                    prices = prices[~duplicates]
                    logger.info(f"  ✅ 去重后长度: {len(prices)}")
            
            elif isinstance(prices, pd.DataFrame):
                logger.info(f"  📊 DataFrame 形状: {prices.shape}")
                logger.info(f"  📊 列名: {list(prices.columns)}")
                logger.info(f"  📊 Index 长度: {len(prices.index)}")
            
        except Exception as query_error:
            logger.error(f"❌ API 查询失败: {query_error}")
            logger.error(f"   错误类型: {type(query_error).__name__}")
            import traceback
            logger.error(f"   详细堆栈:\n{traceback.format_exc()}")
            raise
        
        # 尝试转换为 DataFrame（多种方法）
        try:
            if isinstance(prices, pd.Series):
                # 方法1: to_frame()
                df = prices.to_frame(name='price').reset_index()
                df.columns = ['timestamp', 'price']
            else:
                # DataFrame
                df = prices.reset_index()
                if len(df.columns) == 2:
                    df.columns = ['timestamp', 'price']
                else:
                    df = df.iloc[:, [0, 1]]
                    df.columns = ['timestamp', 'price']
            
            logger.info(f"✅ 成功获取 {len(df)} 条价格数据")
            return df
            
        except Exception as convert_error:
            logger.error(f"❌ DataFrame 转换失败: {convert_error}")
            logger.error(f"   尝试备用方法...")
            
            # 🔧 备用方法：手动构造，但先确保长度一致
            try:
                if isinstance(prices, pd.Series):
                    timestamps = list(prices.index)
                    values = list(prices.values)
                    
                    logger.info(f"  备用方法: timestamps={len(timestamps)}, values={len(values)}")
                    
                    # 强制对齐长度
                    min_len = min(len(timestamps), len(values))
                    df = pd.DataFrame({
                        'timestamp': timestamps[:min_len],
                        'price': values[:min_len]
                    })
                    
                    logger.warning(f"  ⚠️  使用备用方法成功，数据长度: {len(df)}")
                    return df
                else:
                    raise ValueError("备用方法仅支持 Series 类型")
                    
            except Exception as backup_error:
                logger.error(f"❌ 备用方法也失败: {backup_error}")
                raise
    
    @retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(3))
    def fetch_load_forecast(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        获取总负载预测（增强版：带调试信息）
        """
        logger.info(f"获取负载预测: {start} 到 {end}")
        
        try:
            load = self.client.query_load_forecast(
                self.bidding_zone,
                start=start,
                end=end
            )
            
            logger.info(f"  📊 负载数据类型: {type(load)}")
            
            # 处理DataFrame和Series两种情况
            if isinstance(load, pd.DataFrame):
                logger.info(f"  📊 DataFrame 形状: {load.shape}")
                logger.info(f"  📊 列名: {list(load.columns)}")
                
                # 检查重复索引
                if load.index.duplicated().any():
                    logger.warning(f"  ⚠️  发现重复索引，正在去重...")
                    load = load[~load.index.duplicated()]
                
                if load.shape[1] == 1:
                    load_values = load.iloc[:, 0]
                else:
                    load_values = load.mean(axis=1)
                    logger.info(f"  使用 {load.shape[1]} 列的平均值")
                
                df = load_values.to_frame(name='load_forecast').reset_index()
                df.columns = ['timestamp', 'load_forecast']
            else:
                logger.info(f"  📊 Series 长度: {len(load)}")
                
                # 检查重复索引
                if load.index.duplicated().any():
                    logger.warning(f"  ⚠️  发现重复索引，正在去重...")
                    load = load[~load.index.duplicated()]
                
                df = load.to_frame(name='load_forecast').reset_index()
                df.columns = ['timestamp', 'load_forecast']
            
            logger.info(f"✅ 成功获取 {len(df)} 条负载预测数据")
            return df
            
        except Exception as e:
            logger.error(f"❌ 获取负载预测失败: {e}")
            import traceback
            logger.error(f"   详细堆栈:\n{traceback.format_exc()}")
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
            # 确保第一列是时间戳
            if result_df.columns[0] != 'timestamp':
                result_df = result_df.rename(columns={result_df.columns[0]: 'timestamp'})
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
        
        # 合并数据
        df = prices_df.merge(load_df, on='timestamp', how='left')
        df = df.merge(wind_solar_df, on='timestamp', how='left')
        
        # 填充缺失值（使用新版pandas语法）
        df = df.ffill().bfill()
        
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

