# 电力价格预测项目实施指南

## 📁 推荐的项目目录结构

```
ID2223_Project/
│
├── .env                          # 环境变量(API keys)
├── .gitignore                    # Git忽略文件
├── requirements.txt              # Python依赖
├── README.md                     # 项目说明
│
├── config/
│   ├── __init__.py
│   ├── settings.py              # 全局配置(区域、时区等)
│   └── feature_config.py        # 特征定义
│
├── data/
│   ├── __init__.py
│   ├── entsoe_client.py         # ENTSO-E数据获取
│   ├── weather_client.py        # Open-Meteo天气数据
│   └── data_cleaner.py          # 数据清洗与插值
│
├── features/
│   ├── __init__.py
│   ├── feature_engineering.py   # 特征构建(残差负载等)
│   └── feature_groups.py        # Hopsworks特征组定义
│
├── pipelines/
│   ├── __init__.py
│   ├── 1_backfill_features.py   # 历史数据回填
│   ├── 2_daily_feature_pipeline.py  # 每日特征更新
│   ├── 3_training_pipeline.py   # 模型训练
│   └── 4_inference_pipeline.py  # 批量推理
│
├── models/
│   ├── __init__.py
│   ├── trainer.py               # 模型训练逻辑
│   └── evaluator.py             # 模型评估
│
├── ui/
│   ├── app.py                   # Streamlit主界面
│   ├── components/
│   │   ├── price_chart.py       # 价格对比图表
│   │   └── laundry_ticker.py    # 最佳用电时段提示
│   └── utils.py
│
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   ├── 02_feature_analysis.ipynb
│   └── 03_model_experiments.ipynb
│
└── tests/
    ├── test_data_sources.py
    ├── test_features.py
    └── test_pipelines.py
```

## 🚀 详细实施步骤

### 阶段 1:环境准备(第 1-2 天)

#### 1.1 创建虚拟环境

```bash
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

#### 1.2 配置环境变量

创建 `.env` 文件:

```env
# ENTSO-E API
ENTSOE_API_KEY=your_key_here

# Hopsworks
HOPSWORKS_API_KEY=your_key_here
HOPSWORKS_PROJECT_NAME=electricity_price_prediction

# 其他配置
SE3_REGION=SE_3
TIMEZONE=Europe/Stockholm
```

#### 1.3 注册必要的 API

- ENTSO-E: https://transparency.entsoe.eu/ (注册并获取 API key)
- Hopsworks: https://app.hopsworks.ai/ (创建免费账户)

### 阶段 2:数据获取模块(第 3-4 天)

#### 2.1 实现 ENTSO-E 客户端

需要获取的数据:

- Day-Ahead Prices (Document Type: A44)
- Total Load Forecast (Document Type: A65)
- Wind Generation Forecast (Process Type: A01, PsrType: B19)
- Solar Generation Forecast (Process Type: A01, PsrType: B16)

#### 2.2 实现 Open-Meteo 客户端

SE3 区域关键坐标点(建议):

```python
SE3_LOCATIONS = [
    {"name": "Stockholm", "lat": 59.33, "lon": 18.07, "weight": 0.4},
    {"name": "Uppsala", "lat": 59.86, "lon": 17.64, "weight": 0.2},
    {"name": "Västerås", "lat": 59.62, "lon": 16.55, "weight": 0.2},
    {"name": "Offshore_Wind", "lat": 59.00, "lon": 19.50, "weight": 0.2}
]
```

天气参数:

- `temperature_2m` (影响负载需求)
- `wind_speed_10m`, `wind_speed_80m` (影响风电产量)
- `direct_normal_irradiance` (影响光伏产量)

### 阶段 3:特征工程(第 5-6 天)

#### 3.1 核心特征设计

```python
# 时间特征
- hour_of_day (0-23)
- day_of_week (0-6)
- month (1-12)
- is_weekend
- is_holiday (瑞典节假日)

# 市场特征
- load_forecast
- wind_forecast
- solar_forecast
- residual_load = load_forecast - (wind_forecast + solar_forecast)

# 天气特征
- temperature_avg (加权平均)
- wind_speed_avg (加权平均)
- irradiance_avg (加权平均)

# 滞后特征
- price_lag_24h (昨天同一时刻)
- price_lag_168h (上周同一时刻)
- price_rolling_mean_24h
- price_rolling_std_24h
```

#### 3.2 Hopsworks 特征组

创建两个 Feature Groups:

1. **electricity_market_fg**: 价格、负载、发电预测
2. **weather_fg**: 天气数据

### 阶段 4:管道开发(第 7-10 天)

#### 4.1 回填管道 (backfill)

```python
# 获取历史数据:建议至少2年
start_date = "2022-01-01"
end_date = "2024-12-17"

# 逐月获取避免超时
for month in date_range:
    fetch_and_insert_to_hopsworks(month)
```

#### 4.2 每日特征管道

- 设定执行时间: 13:30 CET (确保次日价格已公布)
- 使用 GitHub Actions 或 Modal 定时任务
- 实现增量插入,避免重复

#### 4.3 训练管道

```python
# 训练集: 最近18个月
# 验证集: 最近3个月
# 测试集: 最近1个月

# 超参数调优
xgb_params = {
    'objective': 'reg:squarederror',
    'max_depth': 6,
    'learning_rate': 0.05,
    'n_estimators': 300,
    'subsample': 0.8,
    'colsample_bytree': 0.8
}

# 模型评估
metrics = {
    'MAE': mean_absolute_error,
    'RMSE': root_mean_squared_error,
    'R2': r2_score
}
```

#### 4.4 推理管道

- 获取最新特征
- 预测未来 24 小时价格
- 保存预测结果到 Hopsworks

### 阶段 5:UI 开发(第 11-12 天)

#### 5.1 Streamlit 应用功能

```python
# 主要组件:
1. 标题与项目介绍
2. 实时价格预测展示(未来24小时)
3. 历史预测准确度图表(过去7天)
4. "洗衣计时器" - 最便宜的4小时时段
5. 模型性能指标展示(MAE, RMSE)
6. 最后更新时间
```

#### 5.2 部署到 Hugging Face Spaces

```yaml
# 创建 README.md 在 UI 文件夹
---
title: SE3 Electricity Price Predictor
emoji: ⚡
colorFrom: blue
colorTo: green
sdk: streamlit
sdk_version: 1.29.0
app_file: app.py
pinned: false
---
```

### 阶段 6:测试与优化(第 13-14 天)

#### 6.1 单元测试

- 测试数据获取函数
- 测试特征计算逻辑
- 测试管道端到端流程

#### 6.2 监控与告警

- 设置数据质量检查(缺失值比例)
- 模型性能监控(MAE 超过阈值时告警)
- 管道执行失败通知

## ⚠️ 关键注意事项

### 时间正确性(Point-in-Time Correctness)

```python
# ❌ 错误:使用实际天气数据训练
weather_actual = fetch_weather_historical(date)

# ✅ 正确:使用预报数据训练
weather_forecast = fetch_weather_forecast(date - timedelta(days=1))
```

### 处理负价格

```python
# XGBoost可以直接处理负值,无需转换
# 避免使用MAPE指标(除零问题)
```

### 缺失数据处理

```python
# 优先级:
# 1. 线性插值(小于3小时的缺口)
# 2. 前向填充(1小时内)
# 3. 使用历史平均值(特定时段)
# 4. 删除样本(超过6小时连续缺失)
```

### API 速率限制

```python
# ENTSO-E: 每秒最多400请求
# 实现重试机制和指数退避
import time
from tenacity import retry, wait_exponential

@retry(wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_with_retry(client, params):
    return client.query(**params)
```

## 📊 成功标准

- [ ] 管道每天自动运行无错误
- [ ] MAE < 5 EUR/MWh (基准性能)
- [ ] UI 实时展示最新预测
- [ ] 完整的 7 天历史对比图表
- [ ] 代码已推送到 GitHub
- [ ] 项目文档完整(README + notebooks)

## 🔗 有用的资源

1. **ENTSO-E 文档**: https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
2. **Open-Meteo API**: https://open-meteo.com/en/docs
3. **Hopsworks 文档**: https://docs.hopsworks.ai/
4. **瑞典电力市场**: https://www.nordpoolgroup.com/

## 📅 推荐的两周开发时间表

| 天数  | 任务                      |
| ----- | ------------------------- |
| 1-2   | 环境配置 + API 注册       |
| 3-4   | 数据获取模块开发          |
| 5-6   | 特征工程 + Hopsworks 集成 |
| 7-8   | 回填管道 + 每日管道       |
| 9-10  | 训练管道 + 推理管道       |
| 11-12 | UI 开发 + Streamlit 部署  |
| 13-14 | 测试 + 文档 + 演示准备    |

祝您项目顺利! ⚡
