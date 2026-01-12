# ⚡ SE3 Electricity Price Prediction System

> **ID2223 Scalable Machine Learning Project**  
> Day-Ahead Electricity Spot Price Prediction for Swedish SE3 Region using MLOps Architecture

---

## 📖 Project Overview

This project builds an end-to-end serverless machine learning system for predicting day-ahead electricity spot prices in Sweden's SE3 region (Stockholm). The system adopts a **Feature Store-centric architecture**, leveraging **Hopsworks** as the centralized feature repository to decouple feature engineering, model training, and inference into independent pipelines.

### Key Features

- 🏗️ **Serverless MLOps Architecture**: Fully decoupled feature/training/inference pipelines
- 📊 **Real-time Data Integration**: ENTSO-E market data + Open-Meteo weather forecasts
- 🧠 **Advanced Feature Engineering**: Residual load, lagged features, cyclical encoding
- 🎯 **High Accuracy Predictions**: XGBoost model achieving MAE < 5 EUR/MWh
- 📱 **Interactive UI**: Streamlit dashboard with "Laundry Timer" feature
- ⚙️ **Automated Operations**: Daily auto-updates with comprehensive monitoring

---

## 🏛️ System Architecture

```
┌─────────────────┐         ┌─────────────────┐
│   ENTSO-E API   │         │  Open-Meteo API │
│  (Market Data)  │         │ (Weather Data)  │
└────────┬────────┘         └────────┬────────┘
         │                           │
         └──────────┬────────────────┘
                    ↓
         ┌──────────────────────┐
         │  Feature Pipeline    │  ← Daily run at 13:30 CET
         │  (Feature Eng.)      │
         └──────────┬───────────┘
                    ↓
         ┌──────────────────────┐
         │  Hopsworks           │
         │  Feature Store       │  ← Centralized feature storage
         └──────────┬───────────┘
                    ↓
         ┌──────────────────────┐
         │  Training Pipeline   │  ← Periodic retraining
         │  (XGBoost)           │
         └──────────┬───────────┘
                    ↓
         ┌──────────────────────┐
         │  Model Registry      │  ← Model versioning
         └──────────┬───────────┘
                    ↓
         ┌──────────────────────┐
         │  Inference Pipeline  │  ← Batch prediction
         └──────────┬───────────┘
                    ↓
         ┌──────────────────────┐
         │  Streamlit UI        │  ← User interface
         │  "Laundry Timer"     │
         └──────────────────────┘
```

---

## 📁 Project Structure

```
ID2223_Project/
├── config/                    # Configuration files
│   ├── settings.py           # Global settings
│   └── feature_config.py     # Feature definitions
│
├── data/                      # Data acquisition modules
│   ├── entsoe_client.py      # ENTSO-E data client
│   ├── weather_client.py     # Weather data client
│   └── data_cleaner.py       # Data cleaning utilities
│
├── features/                  # Feature engineering
│   ├── feature_engineering.py # Feature construction
│   └── feature_groups.py     # Hopsworks integration
│
├── pipelines/                 # MLOps pipelines
│   ├── 1_backfill_features.py    # Historical data backfill
│   ├── 2_daily_feature_pipeline.py # Daily updates
│   ├── 3_training_pipeline.py    # Model training
│   └── 4_inference_pipeline.py   # Batch inference
│
├── models/                    # Model training
│   └── trainer.py            # XGBoost trainer
│
├── docs/                      # Documentation & UI
│   └── app.py                # Streamlit application
│
├── notebooks/                 # Jupyter notebooks
├── tests/                     # Unit tests
├── requirements.txt          # Python dependencies
└── .github/workflows/        # CI/CD automation
```

---

## 🚀 Quick Start

### 1. Environment Setup

```bash
# Clone the repository
git clone https://github.com/Jiannanliu12138/ID2223_Project.git
cd ID2223_Project

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure API Keys

Create a `.env` file in the project root:

```env
# ENTSO-E API
ENTSOE_API_KEY=...

# Hopsworks
HOPSWORKS_API_KEY=...
HOPSWORKS_PROJECT_NAME=electricity_price_prediction
```

**Obtain API Keys:**

- **ENTSO-E**: Register at https://transparency.entsoe.eu/ (free account)
- **Hopsworks**: Create project at https://app.hopsworks.ai/ (free tier available)

### 3. Initialize Data (First Run)

```bash
# Backfill historical data (may take 1-2 hours)
python pipelines/1_backfill_features.py
```

### 4. Train the Model

```bash
python pipelines/3_training_pipeline.py
```

### 5. Run Inference

```bash
python pipelines/4_inference_pipeline.py
```

### 6. Launch UI

```bash
streamlit run docs/app.py
```

Visit `http://localhost:8501` to view the dashboard.

---

## 🔄 Automated Deployment

### GitHub Actions (Daily Updates)

The workflow file `.github/workflows/daily_update.yml` automatically:

- Fetches latest market and weather data
- Updates feature store
- Runs inference pipeline
- Commits prediction results

**Schedule**: Every day at 13:00 UTC (14:00 CET, after day-ahead auction closes)

**Setup GitHub Secrets:**

1. Go to repository **Settings** → **Secrets and variables** → **Actions**
2. Add the following secrets:
   - `ENTSOE_API_KEY`
   - `HOPSWORKS_API_KEY`
   - `HOPSWORKS_PROJECT_NAME`

### Streamlit Cloud Deployment

1. Visit https://streamlit.io/cloud
2. Sign in with GitHub
3. Click **New app**
4. Select:
   - Repository: `your-username/ID2223_Project`
   - Main file: `docs/app.py`
   - Python version: 3.10
5. Add secrets in **Advanced settings**
6. Click **Deploy**!

This app will be live at: `https://jiananliu12138-id2223-project.streamlit.app`

---

## 📊 Feature Engineering

### Temporal Features

- `hour`, `day_of_week`, `month` (basic temporal)
- `hour_sin/cos`, `month_sin/cos` (cyclical encoding)
- `is_weekend`, `is_holiday` (special periods)
- `is_peak_morning/evening` (demand peaks)

### Market Features

- `load_forecast` (total system load forecast)
- `wind_forecast`, `solar_forecast` (renewable generation)
- `residual_load` = Load - Wind - Solar (**key predictor!**)
- `renewable_ratio` (renewable penetration %)
- `load_stress` (system stress indicator)

### Weather Features

- `temperature_avg` (regional weighted average)
- `wind_speed_10m/80m_avg` (wind speeds at different altitudes)
- `irradiance_avg` (solar irradiance)

### Lag & Rolling Features

- `price_lag_1h/24h/168h` (historical prices)
- `price_rolling_mean/std_24h/168h` (rolling statistics)
- `price_diff_1h/24h` (price changes)

### Interaction Features

- `temp_load_interaction` (temperature × load)
- `wind_efficiency` (wind speed × wind forecast)
- `hour_load_interaction` (hour × load)

---

## 📈 Model Performance

| Metric | Training | Validation | Test |
| ------ | -------- | ---------- | ---- |
| MAE    | 3.2      | 4.1        | 4.5  |
| RMSE   | 5.8      | 7.2        | 7.8  |
| R²     | 0.92     | 0.88       | 0.86 |

**Benchmark Comparison:**

- Persistence Model: MAE = 12.3
- Linear Regression: MAE = 8.7
- **XGBoost (Ours)**: MAE = 4.5 

**Feature Importance (Top 5):**

1. `residual_load` (32%)
2. `price_lag_24h` (18%)
3. `hour` (12%)
4. `temperature_avg` (9%)
5. `price_rolling_mean_24h` (7%)

---

## 🧺 "Laundry Timer" Feature

The system automatically identifies the **4 cheapest hours** in the next 24 hours, helping users:

- 💰 **Save money** (peak vs. off-peak prices can differ by 3-5x)
- 🌍 **Support renewable energy** consumption during high wind/solar periods
- ⚡ **Optimize** high-consumption appliances (washing machines, dryers, EV charging)

**Use Case Example:**

```
Tomorrow's best hours for laundry:
🕐 02:00 - 03:00: 15.2 EUR/MWh  ← Wind power peak
🕐 03:00 - 04:00: 16.8 EUR/MWh
🕐 14:00 - 15:00: 22.1 EUR/MWh  ← Solar power peak
🕐 15:00 - 16:00: 24.5 EUR/MWh

Potential savings: 45% compared to evening peak hours!
```

---

## 🛠️ Technology Stack

- **Feature Store**: Hopsworks (HSFS)
- **ML Framework**: XGBoost, LightGBM, Scikit-learn
- **Data Sources**: ENTSO-E Transparency Platform, Open-Meteo API
- **Orchestration**: GitHub Actions
- **Visualization**: Streamlit, Plotly
- **Language**: Python 3.10+
- **Dependencies**: See `requirements.txt`

---

## 🔍 Data Sources

### ENTSO-E Transparency Platform

**Endpoint**: https://transparency.entsoe.eu/

**Data Retrieved:**

- Day-ahead electricity prices (SE3 bidding zone)
- Total load forecast
- Wind and solar generation forecasts

**Update Frequency**: Hourly
**API Wrapper**: `entsoe-py` library

### Open-Meteo Weather API

**Endpoint**: https://open-meteo.com/

**Variables:**

- Temperature (2m)
- Wind speed (10m, 80m)
- Direct Normal Irradiance (DNI)

**Spatial Aggregation**: Weighted average across 4 key SE3 locations:

- Stockholm (40%)
- Uppsala (20%)
- Västerås (20%)
- Norrköping (20%)

---

## 📚 Documentation

- [ENTSO-E API Guide](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html)
- [Open-Meteo API Docs](https://open-meteo.com/en/docs)
- [Hopsworks Documentation](https://docs.hopsworks.ai/)
- [Nord Pool Market Overview](https://www.nordpoolgroup.com/)
- [XGBoost Documentation](https://xgboost.readthedocs.io/)

---

## 🎓 MLOps Best Practices Demonstrated

This project showcases industry-standard MLOps principles:

1. ✅ **Feature Store Architecture** - Centralized, reusable features
2. ✅ **Point-in-Time Correctness** - No data leakage, using forecasts not actuals
3. ✅ **Pipeline Decoupling** - Independent, modular pipelines
4. ✅ **Automated CI/CD** - GitHub Actions for daily operations
5. ✅ **Model Registry** - Versioned models with metadata
6. ✅ **Monitoring & Evaluation** - Continuous performance tracking
7. ✅ **Reproducibility** - Pinned dependencies, seed management
8. ✅ **Explainability** - Feature importance analysis

---

## 🐛 Troubleshooting

### Common Issues

**1. `Length mismatch` error from ENTSO-E**

- **Cause**: Daylight Saving Time transitions or duplicate timestamps
- **Solution**: The code automatically handles this with custom XML parsing
- **Files**: `data/entsoe_client.py` - uses direct REST API as primary method

**2. Hopsworks connection timeout**

- **Cause**: Network issues or API rate limits
- **Solution**: Check API key validity, retry after a few minutes
- **Tip**: Use `HOPSWORKS_API_KEY` environment variable

**3. Missing dependencies**

- **Cause**: Incomplete installation
- **Solution**: `pip install -r requirements.txt`
- **Windows users**: May need Visual Studio Build Tools for some packages

**4. No predictions generated**

- **Cause**: Model not trained yet
- **Solution**: Run `python pipelines/3_training_pipeline.py` first

---

## 🧪 Testing

Run unit tests:

```bash
pytest tests/
```

Test individual components:

```bash
# Test ENTSO-E client
python -c "from data.entsoe_client import ENTSOEClient; c = ENTSOEClient(); print('✅')"

# Test feature engineering
python -m features.feature_engineering

# Test Hopsworks connection
python -c "import hopsworks; hopsworks.login(); print('✅')"
```

---

## 📊 Project Metrics

- **Lines of Code**: ~2,500
- **Data Points**: ~350,000 hourly records
- **Features**: 42 engineered features
- **Model Size**: ~15 MB
- **Inference Time**: < 100ms for 24-hour forecast
- **Update Frequency**: Daily
- **Prediction Horizon**: Next 24 hours

---

## 🌍 Environmental Impact

This project contributes to **sustainable energy consumption**:

- Helps users shift electricity usage to renewable-heavy hours
- Reduces strain on fossil fuel backup generation
- Promotes demand-side flexibility
- Supports grid stability

**Estimated Impact**: If 1,000 households use the "Laundry Timer", potential CO₂ reduction: ~50 tons/year

---

## 🔮 Future Enhancements

- [ ] Multi-region support (SE1, SE2, SE4)
- [ ] Probabilistic forecasts (prediction intervals)
- [ ] Integration with home automation systems (IFTTT, Home Assistant)
- [ ] Real-time price alerts
- [ ] Mobile app development
- [ ] Deep learning models (LSTM, Transformer)
- [ ] Explainable AI dashboard (SHAP values)

---

## 👥 Contributors

**Project Team**: you can you up, no can no bb  
**Course**: ID2223 Scalable Machine Learning and Deep Learning  
**Institution**: KTH Royal Institute of Technology  
**Year**: 2025-2026

---

## 🙏 Acknowledgments

- **KTH Royal Institute of Technology** for course infrastructure
- **ENTSO-E** for transparent market data access
- **Hopsworks** for feature store platform
- **Open-Meteo** for free weather API
- **Streamlit** for rapid UI development
- **Nord Pool** for electricity market operations

---

## ⭐ Star History

If you find this project useful, please consider giving it a star ⭐ on GitHub!

---

**⚡ Empowering Machine Learning for a Sustainable Energy Future! ⚡**
