"""
Feature definition configuration
"""

# Time features
TIME_FEATURES = [
    'hour',
    'day_of_week',
    'month',
    'is_weekend',
    'is_holiday'
]

# Market features
MARKET_FEATURES = [
    'load_forecast',           # Total load forecast (MW)
    'wind_forecast',          # Wind power forecast (MW)
    'solar_forecast',         # Solar forecast (MW)
    'residual_load',          # Residual load = load - (wind + solar)
    'renewable_ratio'         # Renewable energy ratio
]

# Weather features
WEATHER_FEATURES = [
    'temperature_avg',        # Average temperature (°C)
    'wind_speed_10m_avg',    # 10m average wind speed (m/s)
    'wind_speed_80m_avg',    # 80m average wind speed (m/s)
    'irradiance_avg'         # Average irradiance (W/m²)
]

# Lag features
LAG_FEATURES = [
    'price_lag_1h',          # Price 1 hour ago
    'price_lag_24h',         # Price 24 hours ago
    'price_lag_168h',        # Price 168 hours ago (same time last week)
    'price_rolling_mean_24h', # Average price over past 24 hours
    'price_rolling_std_24h',  # Price standard deviation over past 24 hours
    'price_rolling_min_24h',  # Lowest price over past 24 hours
    'price_rolling_max_24h'   # Highest price over past 24 hours
]

# All features list
ALL_FEATURES = TIME_FEATURES + MARKET_FEATURES + WEATHER_FEATURES + LAG_FEATURES

# Target variable
TARGET = 'price'  # EUR/MWh

# Swedish public holidays (simplified version, should use holidays library)
SWEDISH_HOLIDAYS = [
    '01-01',  # New Year
    '01-06',  # Epiphany
    '05-01',  # Labour Day
    '06-06',  # National Day
    '12-24',  # Christmas Eve
    '12-25',  # Christmas
    '12-26',  # Boxing Day
    '12-31',  # New Year's Eve
]

