"""
Open-Meteo weather data client
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
    """Open-Meteo weather API client"""
    
    BASE_URL = "https://api.open-meteo.com/v1/forecast"
    ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
    
    def __init__(self, locations: List[Dict] = None):
        """
        Initialize weather client
        
        Args:
            locations: list of locations, each with name, lat, lon, weight
        """
        self.locations = locations or SE3_LOCATIONS
        
        # Verify weights sum to 1
        total_weight = sum(loc['weight'] for loc in self.locations)
        if not np.isclose(total_weight, 1.0):
            logger.warning(f"位置权重总和为 {total_weight},将自动归一化")
            for loc in self.locations:
                loc['weight'] /= total_weight
    
    def fetch_forecast(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch weather forecast data (up to 16 days)
        
        Args:
            start_date: start date 'YYYY-MM-DD'
            end_date: end date 'YYYY-MM-DD'
            
        Returns:
            Weighted average weather DataFrame
        """
        logger.info(f"Fetching weather forecast: {start_date} to {end_date}")
        
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
                
                # Convert to DataFrame
                df = pd.DataFrame({
                    'timestamp': pd.to_datetime(data['hourly']['time']),
                    'temperature_2m': data['hourly']['temperature_2m'],
                    'wind_speed_10m': data['hourly']['wind_speed_10m'],
                    'wind_speed_80m': data['hourly']['wind_speed_80m'],
                    'irradiance': data['hourly']['direct_normal_irradiance']
                })
                
                # Apply weights
                for col in ['temperature_2m', 'wind_speed_10m', 'wind_speed_80m', 'irradiance']:
                    df[col] = df[col] * location['weight']
                
                df['location'] = location['name']
                all_location_data.append(df)
                
                logger.info(f"Successfully fetched weather for {location['name']}")
                
            except Exception as e:
                logger.error(f"Failed to fetch {location['name']} weather data: {e}")
                continue
        
        if not all_location_data:
            raise ValueError("false to fetch weather data for any location")
        
        # Combine and weight average
        combined_df = pd.concat(all_location_data, ignore_index=True)
        
        # group by timestamp and sum weighted values
        result_df = combined_df.groupby('timestamp').agg({
            'temperature_2m': 'sum',
            'wind_speed_10m': 'sum',
            'wind_speed_80m': 'sum',
            'irradiance': 'sum'
        }).reset_index()
        
        # rename columns
        result_df = result_df.rename(columns={
            'temperature_2m': 'temperature_avg',
            'wind_speed_10m': 'wind_speed_10m_avg',
            'wind_speed_80m': 'wind_speed_80m_avg',
            'irradiance': 'irradiance_avg'
        })

        logger.info(f"weighted average completed, {len(result_df)} records")
        return result_df
    
    def fetch_historical(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        acure historical weather data
        
        Args:
            start_date: 'YYYY-MM-DD'
            end_date: 'YYYY-MM-DD'

        Returns:
            weighted average historical weather DataFrame
        """
        logger.info(f"Fetching historical weather data: {start_date} to {end_date}")
        
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
                logger.info(f"successfully fetched historical weather data for {location['name']}")
                
            except Exception as e:
                logger.error(f"Failed to fetch {location['name']} historical weather data: {e}")
                continue
        
        if not all_location_data:
            raise ValueError("not able to fetch historical weather data for any location")
        
        # merge and weight average
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
        
        logger.info(f"historical weather data weighted average completed, {len(result_df)} records")
        return result_df


def main():
    """test weather client"""
    client = WeatherClient()
    
    # Test fetch for the last 3 days forecast
    today = datetime.now().strftime('%Y-%m-%d')
    future = (datetime.now() + timedelta(days=3)).strftime('%Y-%m-%d')
    
    df = client.fetch_forecast(today, future)
    
    print(df.head())
    print(f"\n数据形状: {df.shape}")
    print(f"\n统计信息:\n{df.describe()}")


if __name__ == "__main__":
    main()

