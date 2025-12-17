"""
特征定义配置
"""

# 时间特征
TIME_FEATURES = [
    'hour',
    'day_of_week',
    'month',
    'is_weekend',
    'is_holiday'
]

# 市场特征
MARKET_FEATURES = [
    'load_forecast',           # 总负载预测 (MW)
    'wind_forecast',          # 风电预测 (MW)
    'solar_forecast',         # 光伏预测 (MW)
    'residual_load',          # 残差负载 = load - (wind + solar)
    'renewable_ratio'         # 可再生能源占比
]

# 天气特征
WEATHER_FEATURES = [
    'temperature_avg',        # 平均温度 (°C)
    'wind_speed_10m_avg',    # 10m风速平均 (m/s)
    'wind_speed_80m_avg',    # 80m风速平均 (m/s)
    'irradiance_avg'         # 辐照度平均 (W/m²)
]

# 滞后特征
LAG_FEATURES = [
    'price_lag_1h',          # 1小时前价格
    'price_lag_24h',         # 24小时前价格
    'price_lag_168h',        # 168小时前价格(上周同时)
    'price_rolling_mean_24h', # 过去24小时均价
    'price_rolling_std_24h',  # 过去24小时价格标准差
    'price_rolling_min_24h',  # 过去24小时最低价
    'price_rolling_max_24h'   # 过去24小时最高价
]

# 所有特征列表
ALL_FEATURES = TIME_FEATURES + MARKET_FEATURES + WEATHER_FEATURES + LAG_FEATURES

# 目标变量
TARGET = 'price'  # EUR/MWh

# 瑞典公共假日(简化版,应使用holidays库)
SWEDISH_HOLIDAYS = [
    '01-01',  # 新年
    '01-06',  # 主显节
    '05-01',  # 劳动节
    '06-06',  # 国庆日
    '12-24',  # 平安夜
    '12-25',  # 圣诞节
    '12-26',  # 节礼日
    '12-31',  # 除夕
]

