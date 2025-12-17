# 🎯 项目启动指南

## 📋 运行前检查清单

### 1. 安装 Python 环境

确保您已安装 **Python 3.10 或更高版本**:

```bash
python --version
# 应显示: Python 3.10.x 或更高
```

### 2. 创建虚拟环境并安装依赖

```bash
# 在项目根目录下执行
python -m venv venv

# 激活虚拟环境
venv\Scripts\activate  # Windows
# 或
source venv/bin/activate  # Linux/Mac

# 安装所有依赖
pip install -r requirements.txt
```

---

## 🔑 配置 API 密钥(重要!)

### 步骤 1: 创建 `.env` 文件

在项目根目录下创建 `.env` 文件:

```bash
# Windows
copy .env.example .env

# Linux/Mac
cp .env.example .env
```

### 步骤 2: 获取并填写 API 密钥

打开 `.env` 文件,填入您的密钥:

#### A. ENTSO-E API Key (电力市场数据)

1. **访问**: https://transparency.entsoe.eu/
2. **注册账户** (免费)
3. **登录后**:
   - 点击右上角您的用户名
   - 选择 "Account Settings"
   - 进入 "Web API" 标签
   - 点击 "Generate" 生成 API key
4. **复制密钥**,粘贴到 `.env` 文件:
   ```env
   ENTSOE_API_KEY=你的密钥
   ```

#### B. Hopsworks API Key (Feature Store)

1. **访问**: https://app.hopsworks.ai/
2. **注册账户** (免费,提供 Serverless 层)
3. **登录后**:
   - 点击 "Create New Project"
   - 项目名称填写: `electricity_price_prediction`
   - 进入项目后,点击左侧 "Settings"
   - 选择 "API Keys" 标签
   - 点击 "Generate new key"
4. **复制密钥**,粘贴到 `.env` 文件:
   ```env
   HOPSWORKS_API_KEY=你的密钥
   HOPSWORKS_PROJECT_NAME=electricity_price_prediction
   ```

### 步骤 3: 验证配置

测试 API 连接是否成功:

```bash
# 测试 ENTSO-E
python -c "from data.entsoe_client import ENTSOEClient; client = ENTSOEClient(); print('✅ ENTSO-E 连接成功')"

# 测试 Hopsworks
python -c "from features.feature_groups import FeatureStoreManager; fsm = FeatureStoreManager(); print('✅ Hopsworks 连接成功')"
```

---

## 🏃 运行项目

### 首次运行(完整流程)

#### 1️⃣ 回填历史数据(约 1-2 小时)

```bash
python pipelines/1_backfill_features.py
```

**说明**: 此步骤会获取过去 2 年的历史数据并存入 Hopsworks Feature Store

#### 2️⃣ 训练模型(约 5-10 分钟)

```bash
python pipelines/3_training_pipeline.py
```

**输出**: 模型将保存在 `models/` 目录

#### 3️⃣ 运行推理(约 1 分钟)

```bash
python pipelines/4_inference_pipeline.py
```

**输出**: 预测结果保存在 `predictions/latest_predictions.json`

#### 4️⃣ 启动可视化界面

```bash
cd ui
streamlit run app.py
```

**访问**: 浏览器会自动打开 http://localhost:8501

---

## 🔄 日常运行(每天更新)

如果您已经完成首次运行,每天只需:

```bash
# 1. 更新最新数据
python pipelines/2_daily_feature_pipeline.py

# 2. 运行推理(可选:如需重新训练,运行 3_training_pipeline.py)
python pipelines/4_inference_pipeline.py

# 3. 查看结果
cd ui
streamlit run app.py
```

---

## ❓ 常见问题

### Q1: 提示 "ENTSO-E API key 未设置"

**解决**: 确保 `.env` 文件在项目根目录,且密钥已正确填写

### Q2: Hopsworks 连接超时

**解决**:

1. 检查网络连接
2. 确认项目名称与 `.env` 中一致
3. 重新生成 API key

### Q3: 回填管道运行很慢

**正常现象**: 2 年历史数据约需 1-2 小时,您可以先修改 `config/settings.py` 中的 `BACKFILL_START_DATE` 为最近 3 个月来快速测试

### Q4: 缺少某个 Python 包

**解决**:

```bash
pip install --upgrade -r requirements.txt
```

---

## 📊 验证运行成功

运行成功后,您应该看到:

```
✅ predictions/latest_predictions.json 文件已生成
✅ models/se3_price_predictor.pkl 模型已保存
✅ Streamlit UI 在浏览器中正常显示
✅ UI 中显示未来 24 小时价格预测
✅ "洗衣计时器"显示最便宜的 4 个时段
```

---

## 📖 下一步

- 阅读 [README.md](README.md) 了解项目详情
- 查看 [PROJECT_GUIDE.md](PROJECT_GUIDE.md) 学习架构设计
- 参考 [DEPLOYMENT.md](DEPLOYMENT.md) 部署到云端

---

## 🆘 需要帮助?

如遇到问题,请检查:

1. Python 版本 >= 3.10
2. `.env` 文件是否在根目录
3. API 密钥是否有效
4. 网络连接是否正常

**祝您运行顺利! ⚡**
