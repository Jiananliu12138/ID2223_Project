"""
Model training pipeline
Read data from Feature Store, train model and save to Model Registry
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
    """Main training pipeline - Complete MLOps workflow (11 steps)"""
    logger.info(f"\n{'='*70}")
    logger.info(f"Model training pipeline - {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*70}\n")
    
    try:
        # Step 1: Connect to Feature Store
        logger.info("Step 1/9: Connecting to Feature Store...")
        fsm = FeatureStoreManager()
        
        # Step 2: Read raw data from Feature Groups
        logger.info("Step 2/9: Reading raw data from Feature Groups...")
        
        # Calculate time range (most recent training window months)
        end_date = datetime.now(TIMEZONE)
        start_date = end_date - timedelta(days=TRAINING_WINDOW_MONTHS * 30)
        
        logger.info(f"  Data range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # 直接从 Feature Groups 读取并合并数据
        df = fsm.read_raw_feature_groups(
            start_time=start_date.strftime('%Y-%m-%d %H:%M:%S'),
            end_time=end_date.strftime('%Y-%m-%d %H:%M:%S')
        )
        
        logger.info(f"  ✅ Read {len(df)} raw records")
        
        # Step 3: Feature engineering
        logger.info("Step 3/9: Feature engineering...")
        logger.info(f"  Original feature count: {len(df.columns)}")
        
        df_engineered = FeatureEngineer.engineer_features_pipeline(df, include_lag=True)
        
        logger.info(f"  Engineered feature count: {len(df_engineered.columns)}")
        logger.info(f"  New features added: {len(df_engineered.columns) - len(df.columns)}")
        
        # Step 4: Timezone standardization
        logger.info("Step 4/9: Timezone standardization...")
        if 'timestamp' in df_engineered.columns:
            df_engineered['timestamp'] = pd.to_datetime(df_engineered['timestamp'])
            
            # If naive datetime, add timezone
            if df_engineered['timestamp'].dt.tz is None:
                df_engineered['timestamp'] = df_engineered['timestamp'].dt.tz_localize(TIMEZONE)
                logger.info(f"  Set timezone to {TIMEZONE}")
            else:
                # If timezone exists, convert to configured timezone
                df_engineered['timestamp'] = df_engineered['timestamp'].dt.tz_convert(TIMEZONE)
                logger.info(f"  Converted timezone to {TIMEZONE}")
        
        # Step 5: Save engineered features to new Feature Group
        logger.info("Step 5/9: Saving engineered features to Feature Store...")
        fsm.create_engineered_feature_group(df_engineered)
        
        # Step 6: Create/get Feature View
        logger.info("Step 6/10: Creating/getting engineered feature view...")
        feature_view = fsm.get_engineered_feature_view()
        
        # Step 7: Read training and testing data from Feature View
        logger.info("Step 7/10: Reading training and testing data from Feature View...")
        
        # Calculate test set start time (last 20% of data as test set)
        total_days = (end_date - start_date).days
        test_days = int(total_days * 0.2)
        test_start = end_date - timedelta(days=test_days)
        
        logger.info(f"  Training data: {start_date.strftime('%Y-%m-%d')} to {test_start.strftime('%Y-%m-%d')}")
        logger.info(f"  Testing data: {test_start.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # 使用 Feature View 的 train_test_split
        X_train, X_test, y_train, y_test = feature_view.train_test_split(
            test_start=test_start.strftime('%Y-%m-%d %H:%M:%S')
        )
        
        logger.info(f"  ✅ Training set: {len(X_train)} samples, {len(X_train.columns)} features")
        logger.info(f"  ✅ Testing set: {len(X_test)} samples")
        
        # Separate validation set from training set
        train_val_split = int(len(X_train) * 0.85)  # 85% training, 15% validation
        X_val = X_train.iloc[train_val_split:]
        y_val = y_train.iloc[train_val_split:]
        X_train = X_train.iloc[:train_val_split]
        y_train = y_train.iloc[:train_val_split]
        
        logger.info(f"  ✅ Validation set: {len(X_val)} samples (separated from training set)")
        
        # Step 7.5: Data cleaning (remove non-numeric columns)
        logger.info("Step 7.5/10: Data cleaning...")
        
        # Remove timestamp column and other non-numeric columns
        exclude_cols = ['timestamp']
        cols_to_drop = [col for col in X_train.columns if col in exclude_cols or X_train[col].dtype == 'object']
        
        if cols_to_drop:
            logger.info(f"  Columns to remove: {cols_to_drop}")
            X_train = X_train.drop(columns=cols_to_drop)
            X_val = X_val.drop(columns=cols_to_drop)
            X_test = X_test.drop(columns=cols_to_drop)
        
        logger.info(f"  ✅ Feature count after cleaning: {len(X_train.columns)}")
        
        # Step 8: Train model
        logger.info("Step 8/10: Training model...")
        
        model = ElectricityPriceModel(model_type='xgboost')
        model.train(X_train, y_train, X_val, y_val)
        
        # Step 9: Evaluate model
        logger.info("Step 9/11: Evaluating model...")
        
        train_metrics = model.evaluate(X_train, y_train)
        val_metrics = model.evaluate(X_val, y_val)
        test_metrics = model.evaluate(X_test, y_test)
        
        logger.info("\n📊 Performance summary:")
        logger.info(f"  Training set MAE: {train_metrics['MAE']:.2f} EUR/MWh")
        logger.info(f"  Validation set MAE: {val_metrics['MAE']:.2f} EUR/MWh")
        logger.info(f"  Testing set MAE: {test_metrics['MAE']:.2f} EUR/MWh")
        
        # Step 10: Save to local
        logger.info("Step 10/11: Saving model locally...")
        
        # Local save
        model_path = f"models/{MODEL_NAME}.pkl"
        os.makedirs("models", exist_ok=True)
        model.save_model(model_path)
        logger.info(f"  ✅ Local model: {model_path}")
        
        # Step 11: Save to Model Registry
        logger.info("Step 11/11: Saving model to Hopsworks Model Registry...")
        
        # Save to Hopsworks Model Registry
        mr = fsm.get_model_registry()
        
        # Create model metadata (only numeric values)
        model_metrics = {
            'train_mae': float(train_metrics['MAE']),
            'train_rmse': float(train_metrics['RMSE']),
            'train_r2': float(train_metrics['R2']),
            'val_mae': float(val_metrics['MAE']),
            'val_rmse': float(val_metrics['RMSE']),
            'val_r2': float(val_metrics['R2']),
            'test_mae': float(test_metrics['MAE']),
            'test_rmse': float(test_metrics['RMSE']),
            'test_r2': float(test_metrics['R2']),
            'training_samples': int(len(X_train)),
            'feature_count': int(len(X_train.columns))
        }
        
        # Register model
        model_dir = "models"
        training_date = datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')
        
        electricity_model = mr.python.create_model(
            name=MODEL_NAME,
            metrics=model_metrics,
            description=f"XGBoost electricity price prediction model | Trained: {training_date} | Test MAE: {test_metrics['MAE']:.2f} EUR/MWh | Features: {len(X_train.columns)}",
            input_example=X_test.iloc[:5].to_numpy()
        )
        
        electricity_model.save(model_dir)
        
        logger.info(f"\n{'='*70}")
        logger.info("✅ Model training complete!")
        logger.info(f"  Model name: {MODEL_NAME}")
        logger.info(f"  Testing set MAE: {test_metrics['MAE']:.2f} EUR/MWh")
        logger.info(f"  Model path: {model_path}")
        logger.info(f"{'='*70}\n")
        
        return True
        
    except Exception as e:
        logger.error(f"\n{'='*70}")
        logger.error("❌ Model training failed!")
        logger.error(f"Error message: {e}")
        logger.error(f"{'='*70}\n")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main function"""
    success = train_model()
    
    if success:
        logger.info("Training pipeline executed successfully")
        exit(0)
    else:
        logger.error("Training pipeline execution failed")
        exit(1)


if __name__ == "__main__":
    main()

