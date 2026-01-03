"""
模型训练管道
从Feature Store读取数据,训练模型并保存到Model Registry
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from features.feature_groups import FeatureStoreManager
from features.feature_engineering import FeatureEngineer
from models.trainer import ElectricityPriceModel, prepare_training_data
from config.settings import TRAINING_WINDOW_MONTHS, MODEL_NAME, TIMEZONE
from sklearn.model_selection import train_test_split
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def train_model():
    """主训练流程 - 完整 MLOps 工作流"""
    logger.info(f"\n{'='*70}")
    logger.info(f"模型训练管道 - {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*70}\n")
    
    try:
        # 1. 连接Feature Store
        logger.info("步骤 1/9: 连接Feature Store...")
        fsm = FeatureStoreManager()
        
        # 2. 从 Feature Groups 读取原始数据
        logger.info("步骤 2/9: 从 Feature Groups 读取原始数据...")
        
        # 计算时间范围(最近训练窗口个月)
        end_date = datetime.now(TIMEZONE)
        start_date = end_date - timedelta(days=TRAINING_WINDOW_MONTHS * 30)
        
        logger.info(f"  数据范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
        
        # 直接从 Feature Groups 读取并合并数据
        df = fsm.read_raw_feature_groups(
            start_time=start_date.strftime('%Y-%m-%d %H:%M:%S'),
            end_time=end_date.strftime('%Y-%m-%d %H:%M:%S')
        )
        
        logger.info(f"  ✅ 读取了 {len(df)} 条原始记录")
        
        # 3. 特征工程
        logger.info("步骤 3/9: 特征工程...")
        logger.info(f"  原始特征数: {len(df.columns)}")
        
        df_engineered = FeatureEngineer.engineer_features_pipeline(df, include_lag=True)
        
        logger.info(f"  工程特征数: {len(df_engineered.columns)}")
        logger.info(f"  新增特征: {len(df_engineered.columns) - len(df.columns)} 个")
        
        # 4. 保存工程特征到新的 Feature Group
        logger.info("步骤 4/9: 保存工程特征到 Feature Store...")
        fsm.create_engineered_feature_group(df_engineered)
        
        # 5. 创建/获取 Feature View
        logger.info("步骤 5/9: 创建/获取工程特征视图...")
        feature_view = fsm.get_engineered_feature_view()
        
        # 6. 从 Feature View 读取训练和测试数据
        logger.info("步骤 6/9: 从 Feature View 读取训练和测试数据...")
        
        # 计算测试集起始时间（最近20%的数据作为测试集）
        total_days = (end_date - start_date).days
        test_days = int(total_days * 0.2)
        test_start = end_date - timedelta(days=test_days)
        
        logger.info(f"  训练数据: {start_date.strftime('%Y-%m-%d')} 到 {test_start.strftime('%Y-%m-%d')}")
        logger.info(f"  测试数据: {test_start.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
        
        # 使用 Feature View 的 train_test_split
        X_train, X_test, y_train, y_test = feature_view.train_test_split(
            test_start=test_start.strftime('%Y-%m-%d %H:%M:%S')
        )
        
        logger.info(f"  ✅ 训练集: {len(X_train)} 样本, {len(X_train.columns)} 特征")
        logger.info(f"  ✅ 测试集: {len(X_test)} 样本")
        
        # 从训练集中分出验证集
        train_val_split = int(len(X_train) * 0.85)  # 85%训练，15%验证
        X_val = X_train.iloc[train_val_split:]
        y_val = y_train.iloc[train_val_split:]
        X_train = X_train.iloc[:train_val_split]
        y_train = y_train.iloc[:train_val_split]
        
        logger.info(f"  ✅ 验证集: {len(X_val)} 样本（从训练集分出）")
        
        # 7. 训练模型
        logger.info("步骤 7/9: 训练模型...")
        
        model = ElectricityPriceModel(model_type='xgboost')
        model.train(X_train, y_train, X_val, y_val)
        
        # 8. 评估模型
        logger.info("步骤 8/9: 评估模型...")
        
        train_metrics = model.evaluate(X_train, y_train)
        val_metrics = model.evaluate(X_val, y_val)
        test_metrics = model.evaluate(X_test, y_test)
        
        logger.info("\n📊 性能汇总:")
        logger.info(f"  训练集 MAE: {train_metrics['MAE']:.2f} EUR/MWh")
        logger.info(f"  验证集 MAE: {val_metrics['MAE']:.2f} EUR/MWh")
        logger.info(f"  测试集 MAE: {test_metrics['MAE']:.2f} EUR/MWh")
        
        # 9. 保存到Model Registry
        logger.info("步骤 9/9: 保存模型到Hopsworks...")
        
        # 本地保存
        model_path = f"models/{MODEL_NAME}.pkl"
        os.makedirs("models", exist_ok=True)
        model.save_model(model_path)
        
        # 保存到Hopsworks Model Registry
        mr = fsm.get_model_registry()
        
        # 创建模型元数据
        model_metrics = {
            'train_mae': train_metrics['MAE'],
            'train_rmse': train_metrics['RMSE'],
            'train_r2': train_metrics['R2'],
            'val_mae': val_metrics['MAE'],
            'val_rmse': val_metrics['RMSE'],
            'val_r2': val_metrics['R2'],
            'test_mae': test_metrics['MAE'],
            'test_rmse': test_metrics['RMSE'],
            'test_r2': test_metrics['R2'],
            'training_date': datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S'),
            'training_samples': len(X_train),
            'feature_count': len(X.columns)
        }
        
        # 注册模型
        model_dir = "models"
        
        electricity_model = mr.python.create_model(
            name=MODEL_NAME,
            metrics=model_metrics,
            description=f"XGBoost电力价格预测模型 - 训练于 {datetime.now().strftime('%Y-%m-%d')}",
            input_example=X_test.iloc[:5].to_numpy()
        )
        
        electricity_model.save(model_dir)
        
        logger.info(f"\n{'='*70}")
        logger.info("✅ 模型训练完成!")
        logger.info(f"  模型名称: {MODEL_NAME}")
        logger.info(f"  测试集MAE: {test_metrics['MAE']:.2f} EUR/MWh")
        logger.info(f"  模型路径: {model_path}")
        logger.info(f"{'='*70}\n")
        
        return True
        
    except Exception as e:
        logger.error(f"\n{'='*70}")
        logger.error("❌ 模型训练失败!")
        logger.error(f"错误信息: {e}")
        logger.error(f"{'='*70}\n")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主函数"""
    success = train_model()
    
    if success:
        logger.info("训练管道执行成功")
        exit(0)
    else:
        logger.error("训练管道执行失败")
        exit(1)


if __name__ == "__main__":
    main()

