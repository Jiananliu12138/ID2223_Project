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
        
    def _fetch_prices_raw_api(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        直接调用 ENTSO-E REST API，绕过 entsoe-py 的解析 bug
        """
        import requests
        from xml.etree import ElementTree as ET
        
        # ENTSO-E API 端点
        url = "https://web-api.tp.entsoe.eu/api"
        
        # API 参数
        params = {
            'securityToken': self.api_key,
            'documentType': 'A44',  # Price document
            'in_Domain': self.bidding_zone,
            'out_Domain': self.bidding_zone,
            'periodStart': start.strftime('%Y%m%d%H%M'),
            'periodEnd': end.strftime('%Y%m%d%H%M')
        }
        
        logger.info(f"  直接调用 ENTSO-E REST API...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        # 解析 XML
        root = ET.fromstring(response.content)
        
        # 提取时间序列数据
        ns = {'ns': 'urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3'}
        
        timestamps = []
        prices = []
        
        for timeseries in root.findall('.//ns:TimeSeries', ns):
            for period in timeseries.findall('.//ns:Period', ns):
                # 获取起始时间
                start_time_str = period.find('ns:timeInterval/ns:start', ns).text
                # 解析时间（格式：2026-01-04T23:00Z）
                period_start = pd.to_datetime(start_time_str).tz_convert(TIMEZONE)
                
                # 获取分辨率（通常是 PT60M = 60分钟）
                resolution = period.find('ns:resolution', ns).text
                if resolution == 'PT60M':
                    freq = pd.Timedelta(hours=1)
                elif resolution == 'PT15M':
                    freq = pd.Timedelta(minutes=15)
                else:
                    freq = pd.Timedelta(hours=1)
                
                # 提取所有数据点
                for point in period.findall('ns:Point', ns):
                    position = int(point.find('ns:position', ns).text)
                    price = float(point.find('ns:price.amount', ns).text)
                    
                    # 计算时间戳
                    timestamp = period_start + (position - 1) * freq
                    
                    timestamps.append(timestamp)
                    prices.append(price)
        
        # 创建 DataFrame 并去重
        df = pd.DataFrame({'timestamp': timestamps, 'price': prices})
        df = df.drop_duplicates(subset=['timestamp'], keep='first').sort_values('timestamp')
        
        logger.info(f"  ✅ 原始 API 返回 {len(timestamps)} 个数据点，去重后 {len(df)} 个")
        return df
    
    @retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(3))
    def fetch_day_ahead_prices(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        获取日前市场价格（增强版：优先使用原始 API，避免 entsoe-py 的解析 bug）
        """
        logger.info(f"获取日前价格: {start} 到 {end}")
        
        # 🔧 优先尝试直接调用 REST API（绕过 entsoe-py bug）
        try:
            df = self._fetch_prices_raw_api(start, end)
            logger.info(f"✅ 成功获取 {len(df)} 条价格数据（使用原始 API）")
            return df
        except Exception as raw_api_error:
            logger.warning(f"⚠️  原始 API 调用失败: {raw_api_error}")
            logger.info(f"  尝试使用 entsoe-py 库...")
        
        # 备用方案：使用 entsoe-py 库
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
    
    def _fetch_load_raw_api(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        直接调用 ENTSO-E REST API 获取负载预测
        """
        import requests
        from xml.etree import ElementTree as ET
        
        url = "https://web-api.tp.entsoe.eu/api"
        params = {
            'securityToken': self.api_key,
            'documentType': 'A65',  # System total load forecast
            'processType': 'A01',   # Day ahead
            'outBiddingZone_Domain': self.bidding_zone,
            'periodStart': start.strftime('%Y%m%d%H%M'),
            'periodEnd': end.strftime('%Y%m%d%H%M')
        }
        
        logger.info(f"  直接调用 ENTSO-E REST API (负载预测)...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        # 🔍 调试：保存原始 XML
        logger.debug(f"  Response status: {response.status_code}")
        logger.debug(f"  Response length: {len(response.content)} bytes")
        
        # 解析 XML
        root = ET.fromstring(response.content)
        
        # 🔍 尝试多种可能的命名空间
        possible_namespaces = [
            {'ns': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'},  # Generation/Load Document（正确的！）
            {'ns': 'urn:iec62325.351:tc57wg16:451-6:loaddocument:3:0'},
            {'ns': 'urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3'},
            {},  # 无命名空间
        ]
        
        timestamps = []
        loads = []
        timeseries_list = []
        used_ns = {}
        
        for ns in possible_namespaces:
            timeseries_list = root.findall('.//ns:TimeSeries', ns) if ns else root.findall('.//TimeSeries')
            if timeseries_list:
                used_ns = ns
                logger.info(f"  ✅ 找到 {len(timeseries_list)} 个 TimeSeries（命名空间: {ns.get('ns', 'none')}）")
                break
        
        if not timeseries_list:
            logger.warning(f"  ⚠️  未找到 TimeSeries，尝试查看 XML 根节点...")
            logger.warning(f"  根节点: {root.tag}")
            logger.warning(f"  子节点: {[child.tag for child in root][:5]}")
            # 尝试无命名空间
            timeseries_list = root.findall('.//TimeSeries')
        
        for timeseries in timeseries_list:
            # 尝试有命名空间和无命名空间两种方式
            periods = timeseries.findall('.//ns:Period', used_ns) if used_ns else timeseries.findall('.//Period')
            
            for period in periods:
                # 获取起始时间
                start_elem = period.find('ns:timeInterval/ns:start', used_ns) if used_ns else period.find('.//start')
                if start_elem is None:
                    continue
                start_time_str = start_elem.text
                period_start = pd.to_datetime(start_time_str).tz_convert(TIMEZONE)
                
                # 获取分辨率
                res_elem = period.find('ns:resolution', used_ns) if used_ns else period.find('.//resolution')
                resolution = res_elem.text if res_elem is not None else 'PT60M'
                freq = pd.Timedelta(hours=1) if resolution == 'PT60M' else pd.Timedelta(minutes=15)
                
                # 获取数据点
                points = period.findall('ns:Point', used_ns) if used_ns else period.findall('.//Point')
                for point in points:
                    pos_elem = point.find('ns:position', used_ns) if used_ns else point.find('.//position')
                    qty_elem = point.find('ns:quantity', used_ns) if used_ns else point.find('.//quantity')
                    
                    if pos_elem is None or qty_elem is None:
                        continue
                    
                    position = int(pos_elem.text)
                    load = float(qty_elem.text)
                    
                    timestamp = period_start + (position - 1) * freq
                    timestamps.append(timestamp)
                    loads.append(load)
        
        # 创建 DataFrame
        if not timestamps:
            logger.warning("  ⚠️  原始 API 未返回任何数据，将尝试 entsoe-py 库")
            raise ValueError("No load forecast data from raw API")
        
        df = pd.DataFrame({'timestamp': timestamps, 'load_forecast': loads})
        df = df.drop_duplicates(subset=['timestamp'], keep='first').sort_values('timestamp')
        
        logger.info(f"  ✅ 原始 API 返回 {len(timestamps)} 个数据点，去重后 {len(df)} 个")
        return df
    
    @retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(3))
    def fetch_load_forecast(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        获取总负载预测（增强版：优先使用原始 API）
        """
        logger.info(f"获取负载预测: {start} 到 {end}")
        
        # 优先尝试直接调用 REST API
        try:
            df = self._fetch_load_raw_api(start, end)
            logger.info(f"✅ 成功获取 {len(df)} 条负载预测数据（使用原始 API）")
            return df
        except Exception as raw_api_error:
            logger.warning(f"⚠️  原始 API 调用失败: {raw_api_error}")
            logger.info(f"  尝试使用 entsoe-py 库...")
        
        # 备用方案：使用 entsoe-py 库
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
            
            # 🛡️ 最终容错：返回空 DataFrame，让管道继续运行
            logger.warning("⚠️  所有方法都失败了，返回空负载预测数据")
            logger.warning("⚠️  后续的数据清洗步骤会使用前向填充或默认值")
            return pd.DataFrame(columns=['timestamp', 'load_forecast'])
    
    def _fetch_wind_solar_raw_api(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        直接调用 ENTSO-E REST API 获取风电和光伏预测
        """
        import requests
        from xml.etree import ElementTree as ET
        
        url = "https://web-api.tp.entsoe.eu/api"
        params = {
            'securityToken': self.api_key,
            'documentType': 'A69',  # Wind and solar forecast
            'processType': 'A01',   # Day ahead
            'in_Domain': self.bidding_zone,
            'periodStart': start.strftime('%Y%m%d%H%M'),
            'periodEnd': end.strftime('%Y%m%d%H%M')
        }
        
        logger.info(f"  直接调用 ENTSO-E REST API (风光预测)...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        # 解析 XML
        root = ET.fromstring(response.content)
        ns = {'ns': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'}
        
        wind_data = {}
        solar_data = {}
        
        for timeseries in root.findall('.//ns:TimeSeries', ns):
            # 获取发电类型
            psr_type_elem = timeseries.find('.//ns:MktPSRType/ns:psrType', ns)
            if psr_type_elem is None:
                continue
            psr_type = psr_type_elem.text
            
            for period in timeseries.findall('.//ns:Period', ns):
                start_time_str = period.find('ns:timeInterval/ns:start', ns).text
                period_start = pd.to_datetime(start_time_str).tz_convert(TIMEZONE)
                
                resolution = period.find('ns:resolution', ns).text
                freq = pd.Timedelta(hours=1) if resolution == 'PT60M' else pd.Timedelta(minutes=15)
                
                for point in period.findall('ns:Point', ns):
                    position = int(point.find('ns:position', ns).text)
                    quantity = float(point.find('ns:quantity', ns).text)
                    
                    timestamp = period_start + (position - 1) * freq
                    
                    # B19 = Solar, B18 = Wind Offshore, B19 = Wind Onshore
                    if psr_type == 'B16':  # Solar
                        solar_data[timestamp] = solar_data.get(timestamp, 0) + quantity
                    elif psr_type in ['B18', 'B19']:  # Wind (Offshore + Onshore)
                        wind_data[timestamp] = wind_data.get(timestamp, 0) + quantity
        
        # 创建 DataFrame
        all_timestamps = sorted(set(list(wind_data.keys()) + list(solar_data.keys())))
        
        df = pd.DataFrame({
            'timestamp': all_timestamps,
            'wind_forecast': [wind_data.get(ts, 0) for ts in all_timestamps],
            'solar_forecast': [solar_data.get(ts, 0) for ts in all_timestamps]
        })
        
        logger.info(f"  ✅ 风光预测获取成功: {len(df)} 个时间点")
        return df
    
    @retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(3))
    def fetch_wind_solar_forecast(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        获取风电和光伏发电预测（优先使用原始 API）
        """
        logger.info(f"获取风光预测: {start} 到 {end}")
        
        # 优先尝试直接调用 REST API
        try:
            df = self._fetch_wind_solar_raw_api(start, end)
            logger.info(f"✅ 成功获取 {len(df)} 条风光预测数据（使用原始 API）")
            return df
        except Exception as raw_api_error:
            logger.warning(f"⚠️  原始 API 调用失败: {raw_api_error}")
            logger.info(f"  尝试使用 entsoe-py 库...")
        
        # 备用方案：使用 entsoe-py 库
        try:
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