"""
Open-Meteo天气数据获取客户端
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
from config.settings import SE3_LOCATIONS, TIMEZONE
import logging
from typing import List, Dict
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WeatherClient:
    """Open-Meteo天气API客户端"""
    
    BASE_URL = "https://api.open-meteo.com/v1/forecast"
    ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
    
    def __init__(self, locations: List[Dict] = None):
        """
        初始化天气客户端
        
        Args:
            locations: 位置列表,每个位置包含name, lat, lon, weight
        """
        self.locations = locations or SE3_LOCATIONS
        
        # 验证权重总和为1
        total_weight = sum(loc['weight'] for loc in self.locations)
        if not np.isclose(total_weight, 1.0):
            logger.warning(f"位置权重总和为 {total_weight},将自动归一化")
            for loc in self.locations:
                loc['weight'] /= total_weight
    
    def fetch_forecast(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取天气预报数据(最多16天)
        
        Args:
            start_date: 开始日期 'YYYY-MM-DD'
            end_date: 结束日期 'YYYY-MM-DD'
            
        Returns:
            加权平均后的天气DataFrame
        """
        logger.info(f"获取天气预报: {start_date} 到 {end_date}")
        
        all_location_data = []
        
        for location in self.locations:
            params = {
                'latitude': location['lat'],
                'longitude': location['lon'],
                'hourly': [
                    'temperature_2m',
                    'wind_speed_10m',
                    'wind_speed_80m',
                    'direct_normal_irradiance'
                ],
                'start_date': start_date,
                'end_date': end_date,
                'timezone': 'Europe/Stockholm'
            }
            
            try:
                response = requests.get(self.BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                # 转换为DataFrame
                df = pd.DataFrame({
                    'timestamp': pd.to_datetime(data['hourly']['time']),
                    'temperature_2m': data['hourly']['temperature_2m'],
                    'wind_speed_10m': data['hourly']['wind_speed_10m'],
                    'wind_speed_80m': data['hourly']['wind_speed_80m'],
                    'irradiance': data['hourly']['direct_normal_irradiance']
                })
                
                # 应用权重
                for col in ['temperature_2m', 'wind_speed_10m', 'wind_speed_80m', 'irradiance']:
                    df[col] = df[col] * location['weight']
                
                df['location'] = location['name']
                all_location_data.append(df)
                
                logger.info(f"成功获取 {location['name']} 的天气数据")
                
            except Exception as e:
                logger.error(f"获取 {location['name']} 天气数据失败: {e}")
                continue
        
        if not all_location_data:
            raise ValueError("未能获取任何位置的天气数据")
        
        # 合并并加权平均
        combined_df = pd.concat(all_location_data, ignore_index=True)
        
        # 按时间戳分组求和(因为已经加权)
        result_df = combined_df.groupby('timestamp').agg({
            'temperature_2m': 'sum',
            'wind_speed_10m': 'sum',
            'wind_speed_80m': 'sum',
            'irradiance': 'sum'
        }).reset_index()
        
        # 重命名列
        result_df = result_df.rename(columns={
            'temperature_2m': 'temperature_avg',
            'wind_speed_10m': 'wind_speed_10m_avg',
            'wind_speed_80m': 'wind_speed_80m_avg',
            'irradiance': 'irradiance_avg'
        })
        
        logger.info(f"天气数据加权平均完成,共 {len(result_df)} 条记录")
        return result_df
    
    def fetch_historical(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取历史天气数据(用于回填)
        
        Args:
            start_date: 开始日期 'YYYY-MM-DD'
            end_date: 结束日期 'YYYY-MM-DD'
            
        Returns:
            加权平均后的历史天气DataFrame
        """
        logger.info(f"获取历史天气数据: {start_date} 到 {end_date}")
        
        all_location_data = []
        
        for location in self.locations:
            params = {
                'latitude': location['lat'],
                'longitude': location['lon'],
                'start_date': start_date,
                'end_date': end_date,
                'hourly': [
                    'temperature_2m',
                    'wind_speed_10m',
                    'wind_speed_80m',
                    'direct_normal_irradiance'
                ],
                'timezone': 'Europe/Stockholm'
            }
            
            try:
                response = requests.get(self.ARCHIVE_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                df = pd.DataFrame({
                    'timestamp': pd.to_datetime(data['hourly']['time']),
                    'temperature_2m': data['hourly']['temperature_2m'],
                    'wind_speed_10m': data['hourly']['wind_speed_10m'],
                    'wind_speed_80m': data['hourly']['wind_speed_80m'],
                    'irradiance': data['hourly']['direct_normal_irradiance']
                })
                
                # 应用权重
                for col in ['temperature_2m', 'wind_speed_10m', 'wind_speed_80m', 'irradiance']:
                    df[col] = df[col] * location['weight']
                
                all_location_data.append(df)
                logger.info(f"成功获取 {location['name']} 的历史天气数据")
                
            except Exception as e:
                logger.error(f"获取 {location['name']} 历史天气数据失败: {e}")
                continue
        
        if not all_location_data:
            raise ValueError("未能获取任何位置的历史天气数据")
        
        # 合并并加权平均
        combined_df = pd.concat(all_location_data, ignore_index=True)
        result_df = combined_df.groupby('timestamp').agg({
            'temperature_2m': 'sum',
            'wind_speed_10m': 'sum',
            'wind_speed_80m': 'sum',
            'irradiance': 'sum'
        }).reset_index()
        
        result_df = result_df.rename(columns={
            'temperature_2m': 'temperature_avg',
            'wind_speed_10m': 'wind_speed_10m_avg',
            'wind_speed_80m': 'wind_speed_80m_avg',
            'irradiance': 'irradiance_avg'
        })
        
        logger.info(f"历史天气数据加权平均完成,共 {len(result_df)} 条记录")
        return result_df


def main():
    """测试函数"""
    client = WeatherClient()
    
    # 测试获取最近3天的预报
    today = datetime.now().strftime('%Y-%m-%d')
    future = (datetime.now() + timedelta(days=3)).strftime('%Y-%m-%d')
    
    df = client.fetch_forecast(today, future)
    
    print(df.head())
    print(f"\n数据形状: {df.shape}")
    print(f"\n统计信息:\n{df.describe()}")


if __name__ == "__main__":
    main()

