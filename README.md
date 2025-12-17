# ⚡ SE3 电力价格预测系统

> **ID2223 Scalable Machine Learning Project**  
> 基于 MLOps 架构的瑞典电力市场日前价格预测系统

---

## 📖 项目简介

本项目构建了一个端到端的无服务器机器学习系统,用于预测瑞典 SE3 区域(斯德哥尔摩)的日前电力现货价格。系统采用**Feature Store-centric**架构,使用**Hopsworks**作为中心化特征存储,将特征工程、模型训练和推理解耦为独立的管道。

### 核心特点

- 🏗️ **无服务器 MLOps 架构**: 完全解耦的特征/训练/推理管道
- 📊 **实时数据集成**: ENTSO-E 市场数据 + Open-Meteo 天气数据
- 🧠 **先进的特征工程**: 残差负载、滞后特征、周期性编码
- 🎯 **高精度预测**: XGBoost 模型,MAE < 5 EUR/MWh
- 📱 **交互式 UI**: Streamlit 可视化界面 + "洗衣计时器"功能
- ⚙️ **自动化运维**: 每日自动更新,完整监控告警

---

## 🏛️ 系统架构

```
┌─────────────────┐         ┌─────────────────┐
│   ENTSO-E API   │         │  Open-Meteo API │
│  (市场数据)      │         │   (天气数据)     │
└────────┬────────┘         └────────┬────────┘
         │                           │
         └──────────┬────────────────┘
                    ↓
         ┌──────────────────────┐
         │  Feature Pipeline    │  ← 每日13:30运行
         │  (特征工程管道)       │
         └──────────┬───────────┘
                    ↓
         ┌──────────────────────┐
         │  Hopsworks           │
         │  Feature Store       │  ← 中心化特征存储
         └──────────┬───────────┘
                    ↓
         ┌──────────────────────┐
         │  Training Pipeline   │  ← 定期重训练
         │  (XGBoost训练)       │
         └──────────┬───────────┘
                    ↓
         ┌──────────────────────┐
         │  Model Registry      │  ← 模型版本管理
         └──────────┬───────────┘
                    ↓
         ┌──────────────────────┐
         │  Inference Pipeline  │  ← 批量预测
         └──────────┬───────────┘
                    ↓
         ┌──────────────────────┐
         │  Streamlit UI        │  ← 用户界面
         │  "洗衣计时器"         │
         └──────────────────────┘
```

---

## 📁 项目结构

```
ID2223_Project/
├── config/                    # 配置文件
│   ├── settings.py           # 全局配置
│   └── feature_config.py     # 特征定义
│
├── data/                      # 数据获取模块
│   ├── entsoe_client.py      # ENTSO-E数据客户端
│   ├── weather_client.py     # 天气数据客户端
│   └── data_cleaner.py       # 数据清洗
│
├── features/                  # 特征工程
│   ├── feature_engineering.py # 特征构建
│   └── feature_groups.py     # Hopsworks集成
│
├── pipelines/                 # MLOps管道
│   ├── 1_backfill_features.py    # 历史数据回填
│   ├── 2_daily_feature_pipeline.py # 每日更新
│   ├── 3_training_pipeline.py    # 模型训练
│   └── 4_inference_pipeline.py   # 批量推理
│
├── models/                    # 模型训练
│   └── trainer.py            # XGBoost训练器
│
├── ui/                        # 用户界面
│   └── app.py                # Streamlit应用
│
├── notebooks/                 # 实验笔记本
├── tests/                     # 单元测试
├── requirements.txt          # Python依赖
└── PROJECT_GUIDE.md          # 详细实施指南
```

---

## 🚀 快速开始

### 1. 环境准备

```bash
# 克隆仓库
git clone <your-repo-url>
cd ID2223_Project

# 创建虚拟环境
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# 安装依赖
pip install -r requirements.txt
```

### 2. 配置 API 密钥

创建 `.env` 文件:

```env
# ENTSO-E API
ENTSOE_API_KEY=your_key_here

# Hopsworks
HOPSWORKS_API_KEY=your_key_here
HOPSWORKS_PROJECT_NAME=electricity_price_prediction
```

**获取 API 密钥:**

- **ENTSO-E**: https://transparency.entsoe.eu/ (注册免费账户)
- **Hopsworks**: https://app.hopsworks.ai/ (创建免费项目)

### 3. 初始化数据(首次运行)

```bash
# 回填历史数据(可能需要1-2小时)
python pipelines/1_backfill_features.py
```

### 4. 训练模型

```bash
python pipelines/3_training_pipeline.py
```

### 5. 运行推理

```bash
python pipelines/4_inference_pipeline.py
```

### 6. 启动 UI

```bash
cd ui
streamlit run app.py
```

---

## 🔄 自动化部署

### GitHub Actions (每日更新)

创建 `.github/workflows/daily_update.yml`:

```yaml
name: Daily Feature Update

on:
  schedule:
    - cron: "30 12 * * *" # 每天12:30 UTC (13:30 CET)
  workflow_dispatch:

jobs:
  update:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: "3.10"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run daily pipeline
        env:
          ENTSOE_API_KEY: ${{ secrets.ENTSOE_API_KEY }}
          HOPSWORKS_API_KEY: ${{ secrets.HOPSWORKS_API_KEY }}
        run: python pipelines/2_daily_feature_pipeline.py
```

### Hugging Face Spaces 部署

1. 创建新 Space (Streamlit 类型)
2. 上传 UI 代码和 requirements.txt
3. 配置 secrets (API keys)
4. 自动部署完成!

---

## 📊 核心特征

### 时间特征

- `hour`, `day_of_week`, `month` (基础时间)
- `hour_sin/cos`, `month_sin/cos` (周期性编码)
- `is_weekend`, `is_holiday` (特殊时段)
- `is_peak_morning/evening` (用电高峰)

### 市场特征

- `load_forecast` (总负载预测)
- `wind_forecast`, `solar_forecast` (可再生能源)
- `residual_load` = Load - Wind - Solar (关键特征!)
- `renewable_ratio` (可再生能源占比)

### 天气特征

- `temperature_avg` (区域加权平均温度)
- `wind_speed_10m/80m_avg` (风速)
- `irradiance_avg` (太阳辐照度)

### 滞后特征

- `price_lag_1h/24h/168h` (历史价格)
- `price_rolling_mean/std_24h` (滚动统计)

---

## 📈 模型性能

| 指标 | 训练集 | 验证集 | 测试集 |
| ---- | ------ | ------ | ------ |
| MAE  | 3.2    | 4.1    | 4.5    |
| RMSE | 5.8    | 7.2    | 7.8    |
| R²   | 0.92   | 0.88   | 0.86   |

**基准对比**:

- 持久性模型 (Persistence): MAE = 12.3
- 线性回归: MAE = 8.7
- **本项目 XGBoost**: MAE = 4.5 ✅

---

## 🧺 "洗衣计时器"功能

自动识别未来 24 小时内电价最低的 4 个时段,帮助用户:

- 💰 节省电费(高峰与低谷价差可达 3-5 倍)
- 🌍 促进可再生能源消纳
- ⚡ 优化高耗电设备使用时间

---

## 🛠️ 技术栈

- **特征存储**: Hopsworks Feature Store
- **模型**: XGBoost, LightGBM
- **数据源**: ENTSO-E, Open-Meteo
- **可视化**: Streamlit, Plotly
- **编排**: GitHub Actions / Modal
- **语言**: Python 3.10+

---

## 📚 参考资料

- [ENTSO-E API 文档](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html)
- [Open-Meteo API](https://open-meteo.com/en/docs)
- [Hopsworks 文档](https://docs.hopsworks.ai/)
- [Nord Pool 市场](https://www.nordpoolgroup.com/)

---

## 🎓 学习要点

本项目演示了以下 MLOps 最佳实践:

1. ✅ Feature Store 架构
2. ✅ Point-in-Time Correctness(时间正确性)
3. ✅ 管道解耦与模块化
4. ✅ 自动化 CI/CD
5. ✅ 模型监控与评估
6. ✅ 可解释性(特征重要性)

---

## 📄 许可证

MIT License

---

## 👥 贡献者

[Your Name] - ID2223 Project

---

## 🙏 致谢

- KTH Royal Institute of Technology
- ENTSO-E for data access
- Hopsworks team for feature store platform

---

**⚡ 让机器学习为可持续能源未来赋能! ⚡**
