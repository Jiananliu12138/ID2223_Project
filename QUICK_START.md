# 🚀 快速开始指南

## 5 分钟运行演示

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置 API 密钥

创建 `.env` 文件(复制自`.env.example`):

```env
ENTSOE_API_KEY=your_entsoe_key
HOPSWORKS_API_KEY=your_hopsworks_key
HOPSWORKS_PROJECT_NAME=electricity_price_prediction
```

### 3. 测试数据获取

```bash
# 测试ENTSO-E连接
python -c "from data.entsoe_client import ENTSOEClient; client = ENTSOEClient(); print('✅ ENTSO-E连接成功')"

# 测试天气API
python -c "from data.weather_client import WeatherClient; client = WeatherClient(); print('✅ 天气API连接成功')"

# 测试Hopsworks连接
python -c "from features.feature_groups import FeatureStoreManager; fsm = FeatureStoreManager(); print('✅ Hopsworks连接成功')"
```

### 4. 运行管道(按顺序)

```bash
# 步骤1: 回填历史数据(首次运行,约1-2小时)
python pipelines/1_backfill_features.py

# 步骤2: 训练模型(约5-10分钟)
python pipelines/3_training_pipeline.py

# 步骤3: 运行推理(约1分钟)
python pipelines/4_inference_pipeline.py

# 步骤4: 启动UI
cd ui
streamlit run app.py
```

---

## 常见问题排查

### 问题 1: ENTSO-E API 限流

**症状**: `429 Too Many Requests`  
**解决**:

```python
# 在config/settings.py中增加重试延迟
from tenacity import retry, wait_exponential
```

### 问题 2: Hopsworks 连接超时

**症状**: `Connection timeout`  
**解决**:

```bash
# 检查网络连接
ping app.hopsworks.ai

# 确认API key正确
echo $HOPSWORKS_API_KEY
```

### 问题 3: 缺少历史数据

**症状**: 特征工程失败  
**解决**:

```bash
# 重新运行回填管道
python pipelines/1_backfill_features.py
```

---

## 开发模式

### 使用 Jupyter Notebook 探索

```bash
jupyter notebook notebooks/01_data_exploration.ipynb
```

### 运行测试

```bash
pytest tests/ -v
```

### 查看日志

```bash
# 查看管道日志
tail -f logs/daily_pipeline.log

# 查看训练日志
tail -f logs/training.log
```

---

## 下一步

1. ✅ 阅读 [PROJECT_GUIDE.md](PROJECT_GUIDE.md) 了解详细架构
2. ✅ 调整超参数 (`models/trainer.py`)
3. ✅ 添加更多特征 (`features/feature_engineering.py`)
4. ✅ 部署到 Hugging Face Spaces
5. ✅ 设置 GitHub Actions 定时任务

---

**祝您项目顺利! 🎉**
