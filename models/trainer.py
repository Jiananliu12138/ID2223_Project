"""
Model training module
"""
import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import xgboost as xgb
import lightgbm as lgb
from typing import Tuple, Dict
import logging
import joblib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ElectricityPriceModel:
    """Electricity price prediction model"""
    
    def __init__(self, model_type: str = 'xgboost'):
        """
        Initialize model
        
        Args:
            model_type: Model type ('xgboost' or 'lightgbm')
        """
        self.model_type = model_type
        self.model = None
        self.feature_names = None
        self.feature_importance = None
        
    def get_default_params(self) -> dict:
        """
        Get default hyperparameters
        
        Returns:
            Dictionary of hyperparameters
        """
        if self.model_type == 'xgboost':
            return {
                'objective': 'reg:squarederror',
                'max_depth': 8,
                'learning_rate': 0.05,
                'n_estimators': 500,
                'subsample': 0.8,
                'colsample_bytree': 0.8,
                'min_child_weight': 3,
                'gamma': 0.1,
                'reg_alpha': 0.1,
                'reg_lambda': 1.0,
                'random_state': 42,
                'n_jobs': -1
            }
        else:  # lightgbm
            return {
                'objective': 'regression',
                'metric': 'mae',
                'max_depth': 8,
                'learning_rate': 0.05,
                'n_estimators': 500,
                'subsample': 0.8,
                'colsample_bytree': 0.8,
                'min_child_samples': 20,
                'reg_alpha': 0.1,
                'reg_lambda': 1.0,
                'random_state': 42,
                'n_jobs': -1,
                'verbose': -1
            }
    
    def train(self,
             X_train: pd.DataFrame,
             y_train: pd.Series,
             X_val: pd.DataFrame = None,
             y_val: pd.Series = None,
             params: dict = None) -> None:
        """
        Train model
        
        Args:
            X_train: Training features
            y_train: Training target
            X_val: Validation features
            y_val: Validation target
            params: Custom hyperparameters
        """
        logger.info(f"Starting {self.model_type} model training...")
        logger.info(f"Training set size: {len(X_train)}")
        if X_val is not None:
            logger.info(f"Validation set size: {len(X_val)}")
        
        # Save feature names
        self.feature_names = X_train.columns.tolist()
        
        # Get parameters
        model_params = params if params else self.get_default_params()
        
        # Train model
        if self.model_type == 'xgboost':
            # If validation set exists, add early stopping parameters
            if X_val is not None:
                model_params['early_stopping_rounds'] = 50
            
            self.model = xgb.XGBRegressor(**model_params)
            
            if X_val is not None:
                eval_set = [(X_train, y_train), (X_val, y_val)]
                self.model.fit(
                    X_train, y_train,
                    eval_set=eval_set,
                    verbose=False
                )
            else:
                self.model.fit(X_train, y_train)
                
            # 特征重要性
            self.feature_importance = pd.DataFrame({
                'feature': self.feature_names,
                'importance': self.model.feature_importances_
            }).sort_values('importance', ascending=False)
            
        else:  # lightgbm
            self.model = lgb.LGBMRegressor(**model_params)
            
            if X_val is not None:
                self.model.fit(
                    X_train, y_train,
                    eval_set=[(X_val, y_val)],
                    callbacks=[lgb.early_stopping(50), lgb.log_evaluation(0)]
                )
            else:
                self.model.fit(X_train, y_train)
            
            # 特征重要性
            self.feature_importance = pd.DataFrame({
                'feature': self.feature_names,
                'importance': self.model.feature_importances_
            }).sort_values('importance', ascending=False)
        
        logger.info("Model training complete!")
        logger.info(f"Top 10 important features:\n{self.feature_importance.head(10)}")
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        Predict
        
        Args:
            X: Feature DataFrame
            
        Returns:
            Array of predicted values
        """
        if self.model is None:
            raise ValueError("Model not trained, please call train() method first")
        
        return self.model.predict(X)
    
    def evaluate(self, X: pd.DataFrame, y: pd.Series) -> Dict[str, float]:
        """
        Evaluate model
        
        Args:
            X: Features
            y: True values (can be Series or DataFrame)
            
        Returns:
            Dictionary of evaluation metrics
        """
        y_pred = self.predict(X)
        
        # Ensure y is numpy array (handle DataFrame or Series)
        if isinstance(y, pd.DataFrame):
            y_true = y.values.ravel()
        elif isinstance(y, pd.Series):
            y_true = y.values
        else:
            y_true = np.array(y).ravel()
        
        metrics = {
            'MAE': mean_absolute_error(y_true, y_pred),
            'RMSE': np.sqrt(mean_squared_error(y_true, y_pred)),
            'R2': r2_score(y_true, y_pred),
            'MAPE': np.mean(np.abs((y_true - y_pred) / np.maximum(y_true, 1e-8))) * 100  # Avoid division by zero
        }
        
        logger.info("Model evaluation results:")
        for metric, value in metrics.items():
            logger.info(f"  {metric}: {value:.4f}")
        
        return metrics
    
    def save_model(self, path: str) -> None:
        """
        Save model
        
        Args:
            path: Save path
        """
        if self.model is None:
            raise ValueError("Model not trained, cannot save")
        
        model_data = {
            'model': self.model,
            'model_type': self.model_type,
            'feature_names': self.feature_names,
            'feature_importance': self.feature_importance
        }
        
        joblib.dump(model_data, path)
        logger.info(f"Model saved to: {path}")
    
    def load_model(self, path: str) -> None:
        """
        Load model
        
        Args:
            path: Model path
        """
        model_data = joblib.load(path)
        
        self.model = model_data['model']
        self.model_type = model_data['model_type']
        self.feature_names = model_data['feature_names']
        self.feature_importance = model_data['feature_importance']
        
        logger.info(f"Model loaded from: {path}")


def prepare_training_data(df: pd.DataFrame,
                         target_col: str = 'price') -> Tuple[pd.DataFrame, pd.Series]:
    """
    Prepare training data
    
    Args:
        df: Complete DataFrame
        target_col: Target column name
        
    Returns:
        (X, y) Features and target
    """
    # Remove rows with missing target values
    df = df.dropna(subset=[target_col])
    
    # Separate features and target
    exclude_cols = [target_col, 'timestamp']
    feature_cols = [col for col in df.columns if col not in exclude_cols]
    
    X = df[feature_cols].fillna(0)  # Fill remaining missing values
    y = df[target_col]
    
    logger.info(f"Number of features: {X.shape[1]}")
    logger.info(f"Number of samples: {X.shape[0]}")
    
    return X, y


def main():
    """Test function"""
    from sklearn.model_selection import train_test_split
    
    # Create test data
    np.random.seed(42)
    n_samples = 1000
    
    X = pd.DataFrame({
        'feature_1': np.random.randn(n_samples),
        'feature_2': np.random.randn(n_samples),
        'feature_3': np.random.randn(n_samples),
        'hour': np.random.randint(0, 24, n_samples),
        'load': np.random.uniform(5000, 10000, n_samples)
    })
    
    # Create target (linear relationship with noise)
    y = (X['feature_1'] * 10 + X['feature_2'] * 5 +
         X['hour'] * 2 + X['load'] * 0.01 +
         np.random.randn(n_samples) * 5)
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    # Train model
    model = ElectricityPriceModel(model_type='xgboost')
    model.train(X_train, y_train, X_test, y_test)
    
    # Evaluate
    metrics = model.evaluate(X_test, y_test)
    
    print(f"\nTest MAE: {metrics['MAE']:.2f}")


if __name__ == "__main__":
    main()

