# 🚀 部署指南

## 本地部署

### 开发环境

```bash
# 1. 克隆项目
git clone <your-repo-url>
cd ID2223_Project

# 2. 创建虚拟环境
python -m venv venv
venv\Scripts\activate  # Windows

# 3. 安装依赖
pip install -r requirements.txt

# 4. 配置环境变量
copy .env.example .env
# 编辑.env文件,填入你的API密钥
```

---

## GitHub Actions 自动化

### 配置 Secrets

在 GitHub 仓库设置中添加以下 Secrets:

1. `ENTSOE_API_KEY` - ENTSO-E API 密钥
2. `HOPSWORKS_API_KEY` - Hopsworks API 密钥
3. `HOPSWORKS_PROJECT_NAME` - Hopsworks 项目名称
4. `EMAIL_USERNAME` (可选) - 告警邮箱用户名
5. `EMAIL_PASSWORD` (可选) - 告警邮箱密码
6. `NOTIFICATION_EMAIL` (可选) - 接收告警的邮箱

### 工作流说明

已配置的 GitHub Actions 工作流:

- **每日特征更新** (`.github/workflows/daily_update.yml`)
  - 触发时间: 每天 13:30 CET
  - 功能: 自动获取最新数据并更新 Feature Store
  - 失败时发送邮件告警

### 手动触发

```bash
# 在GitHub Actions页面点击"Run workflow"按钮
# 或使用GitHub CLI:
gh workflow run daily_update.yml
```

---

## Hugging Face Spaces 部署

### 准备工作

1. **注册 Hugging Face 账户**: https://huggingface.co/join
2. **创建新 Space**:
   - 类型: Streamlit
   - 硬件: CPU Basic (免费)

### 部署步骤

#### 方法 1: 通过 Web 界面

```bash
# 1. 创建Space后,上传以下文件:
ui/app.py
requirements.txt
config/
features/
models/
predictions/

# 2. 在Space Settings中添加Secrets:
ENTSOE_API_KEY
HOPSWORKS_API_KEY
HOPSWORKS_PROJECT_NAME
```

#### 方法 2: 通过 Git

```bash
# 1. 添加Hugging Face remote
git remote add hf https://huggingface.co/spaces/<your-username>/<space-name>

# 2. 创建部署分支
git checkout -b hf-deploy

# 3. 准备文件
cp ui/app.py app.py
echo "---
title: SE3 Electricity Price Predictor
emoji: ⚡
colorFrom: blue
colorTo: green
sdk: streamlit
sdk_version: 1.29.0
app_file: app.py
pinned: false
---

# SE3电力价格预测

实时预测瑞典SE3区域电力价格
" > README.md

# 4. 推送到Hugging Face
git add .
git commit -m "Deploy to Hugging Face Spaces"
git push hf hf-deploy:main
```

### 配置文件示例

**requirements.txt** (精简版,仅 UI 依赖):

```txt
streamlit==1.29.0
pandas==2.1.3
plotly==5.18.0
hopsworks==3.7.0
python-dotenv==1.0.0
```

---

## Modal.com 部署(推荐用于管道)

### 为什么选择 Modal?

- ⚡ 冷启动快(< 1 秒)
- 💰 按使用付费,空闲时无费用
- 🔄 原生支持定时任务
- 📦 容器化部署,环境一致

### 安装 Modal CLI

```bash
pip install modal
modal token new  # 创建认证token
```

### 配置管道

**pipelines/modal_daily.py**:

```python
import modal

stub = modal.Stub("electricity-pipeline")

# 创建镜像
image = modal.Image.debian_slim().pip_install_from_requirements("requirements.txt")

@stub.function(
    image=image,
    schedule=modal.Period(days=1, hour=13, minute=30),  # 每天13:30运行
    secrets=[
        modal.Secret.from_name("entsoe-api-key"),
        modal.Secret.from_name("hopsworks-credentials")
    ]
)
def daily_update():
    from pipelines.daily_feature_pipeline import daily_update
    daily_update()

if __name__ == "__main__":
    stub.deploy()
```

### 部署到 Modal

```bash
# 1. 添加Secrets
modal secret create entsoe-api-key ENTSOE_API_KEY=your_key
modal secret create hopsworks-credentials \
  HOPSWORKS_API_KEY=your_key \
  HOPSWORKS_PROJECT_NAME=your_project

# 2. 部署
modal deploy pipelines/modal_daily.py

# 3. 查看日志
modal logs electricity-pipeline::daily_update
```

---

## Docker 部署

### Dockerfile

```dockerfile
FROM python:3.10-slim

WORKDIR /app

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY . .

# 暴露端口
EXPOSE 8501

# 启动Streamlit
CMD ["streamlit", "run", "ui/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

### 构建和运行

```bash
# 构建镜像
docker build -t electricity-price-predictor .

# 运行容器
docker run -p 8501:8501 \
  -e ENTSOE_API_KEY=your_key \
  -e HOPSWORKS_API_KEY=your_key \
  electricity-price-predictor
```

### Docker Compose

**docker-compose.yml**:

```yaml
version: "3.8"

services:
  ui:
    build: .
    ports:
      - "8501:8501"
    environment:
      - ENTSOE_API_KEY=${ENTSOE_API_KEY}
      - HOPSWORKS_API_KEY=${HOPSWORKS_API_KEY}
      - HOPSWORKS_PROJECT_NAME=${HOPSWORKS_PROJECT_NAME}
    volumes:
      - ./predictions:/app/predictions
    restart: unless-stopped

  scheduler:
    build: .
    command: python -m pipelines.2_daily_feature_pipeline
    environment:
      - ENTSOE_API_KEY=${ENTSOE_API_KEY}
      - HOPSWORKS_API_KEY=${HOPSWORKS_API_KEY}
      - HOPSWORKS_PROJECT_NAME=${HOPSWORKS_PROJECT_NAME}
    restart: on-failure
```

运行:

```bash
docker-compose up -d
```

---

## AWS Lambda 部署(推理管道)

### 准备 Lambda 函数

```python
# lambda_handler.py
import json
from pipelines.inference_pipeline import run_inference

def lambda_handler(event, context):
    try:
        success = run_inference()
        return {
            'statusCode': 200 if success else 500,
            'body': json.dumps({'status': 'success' if success else 'failed'})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
```

### 使用 SAM 部署

**template.yaml**:

```yaml
AWSTemplateFormatVersion: "2010-09-09"
Transform: AWS::Serverless-2016-10-31

Resources:
  InferenceFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: lambda_handler.lambda_handler
      Runtime: python3.10
      CodeUri: .
      MemorySize: 1024
      Timeout: 300
      Environment:
        Variables:
          ENTSOE_API_KEY: !Ref EntsoEApiKey
          HOPSWORKS_API_KEY: !Ref HopsworksApiKey
      Events:
        DailySchedule:
          Type: Schedule
          Properties:
            Schedule: cron(30 13 * * ? *)
```

部署:

```bash
sam build
sam deploy --guided
```

---

## 监控与告警

### CloudWatch 监控(AWS)

```python
import boto3

cloudwatch = boto3.client('cloudwatch')

# 发送自定义指标
cloudwatch.put_metric_data(
    Namespace='ElectricityPrediction',
    MetricData=[
        {
            'MetricName': 'PredictionMAE',
            'Value': mae_value,
            'Unit': 'None'
        }
    ]
)
```

### Sentry 错误追踪

```bash
pip install sentry-sdk
```

```python
import sentry_sdk

sentry_sdk.init(
    dsn="your-sentry-dsn",
    traces_sample_rate=1.0
)
```

---

## 性能优化

### 1. 缓存策略

```python
import functools
from datetime import timedelta

@functools.lru_cache(maxsize=128)
def get_cached_features(date_key):
    # 缓存特征数据
    pass
```

### 2. 批量处理

```python
# 使用批量API调用减少请求次数
batch_size = 100
for i in range(0, len(data), batch_size):
    batch = data[i:i+batch_size]
    process_batch(batch)
```

### 3. 异步处理

```python
import asyncio

async def fetch_data_async():
    # 异步获取数据
    pass
```

---

## 故障排查

### 日志配置

**config/logging.yaml**:

```yaml
version: 1
formatters:
  default:
    format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
handlers:
  console:
    class: logging.StreamHandler
    formatter: default
  file:
    class: logging.FileHandler
    filename: logs/app.log
    formatter: default
root:
  level: INFO
  handlers: [console, file]
```

### 健康检查端点

```python
# ui/app.py
@st.cache_resource
def health_check():
    checks = {
        'feature_store': test_hopsworks_connection(),
        'data_sources': test_api_connections(),
        'model': check_model_exists()
    }
    return all(checks.values())
```

---

**祝您部署顺利! 🚀**
