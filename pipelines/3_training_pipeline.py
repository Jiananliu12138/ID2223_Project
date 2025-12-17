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
    """主训练流程"""
    logger.info(f"\n{'='*70}")
    logger.info(f"模型训练管道 - {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*70}\n")
    
    try:
        # 1. 连接Feature Store
        logger.info("步骤 1/8: 连接Feature Store...")
        fsm = FeatureStoreManager()
        
        # 2. 读取特征数据
        logger.info("步骤 2/8: 读取特征数据...")
        
        # 计算时间范围(最近18个月)
        end_date = datetime.now(TIMEZONE)
        start_date = end_date - timedelta(days=TRAINING_WINDOW_MONTHS * 30)
        
        logger.info(f"  数据范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
        
        df = fsm.read_feature_data(
            start_time=start_date.strftime('%Y-%m-%d %H:%M:%S'),
            end_time=end_date.strftime('%Y-%m-%d %H:%M:%S')
        )
        
        logger.info(f"  读取了 {len(df)} 条记录")
        
        # 3. 特征工程
        logger.info("步骤 3/8: 特征工程...")
        df = FeatureEngineer.engineer_features_pipeline(df, include_lag=True)
        
        # 4. 准备训练数据
        logger.info("步骤 4/8: 准备训练数据...")
        X, y = prepare_training_data(df, target_col='price')
        
        # 5. 数据分割
        logger.info("步骤 5/8: 分割训练/验证/测试集...")
        
        # 时间序列分割(不能随机打乱)
        train_size = int(len(X) * 0.7)
        val_size = int(len(X) * 0.15)
        
        X_train = X.iloc[:train_size]
        y_train = y.iloc[:train_size]
        
        X_val = X.iloc[train_size:train_size+val_size]
        y_val = y.iloc[train_size:train_size+val_size]
        
        X_test = X.iloc[train_size+val_size:]
        y_test = y.iloc[train_size+val_size:]
        
        logger.info(f"  训练集: {len(X_train)} 样本")
        logger.info(f"  验证集: {len(X_val)} 样本")
        logger.info(f"  测试集: {len(X_test)} 样本")
        
        # 6. 训练模型
        logger.info("步骤 6/8: 训练模型...")
        
        model = ElectricityPriceModel(model_type='xgboost')
        model.train(X_train, y_train, X_val, y_val)
        
        # 7. 评估模型
        logger.info("步骤 7/8: 评估模型...")
        
        train_metrics = model.evaluate(X_train, y_train)
        val_metrics = model.evaluate(X_val, y_val)
        test_metrics = model.evaluate(X_test, y_test)
        
        logger.info("\n性能汇总:")
        logger.info(f"  训练集 MAE: {train_metrics['MAE']:.2f} EUR/MWh")
        logger.info(f"  验证集 MAE: {val_metrics['MAE']:.2f} EUR/MWh")
        logger.info(f"  测试集 MAE: {test_metrics['MAE']:.2f} EUR/MWh")
        
        # 8. 保存到Model Registry
        logger.info("步骤 8/8: 保存模型到Hopsworks...")
        
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

