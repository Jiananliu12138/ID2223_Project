"""
模型训练模块
"""
import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import xgboost as xgb
from xgboost.callback import EarlyStopping
import lightgbm as lgb
from typing import Tuple, Dict
import logging
import joblib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ElectricityPriceModel:
    """电力价格预测模型"""
    
    def __init__(self, model_type: str = 'xgboost'):
        """
        初始化模型
        
        Args:
            model_type: 模型类型 ('xgboost' 或 'lightgbm')
        """
        self.model_type = model_type
        self.model = None
        self.feature_names = None
        self.feature_importance = None
        
    def get_default_params(self) -> dict:
        """
        获取默认超参数
        
        Returns:
            超参数字典
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
        训练模型
        
        Args:
            X_train: 训练特征
            y_train: 训练目标
            X_val: 验证特征
            y_val: 验证目标
            params: 自定义超参数
        """
        logger.info(f"开始训练 {self.model_type} 模型...")
        logger.info(f"训练集大小: {len(X_train)}")
        if X_val is not None:
            logger.info(f"验证集大小: {len(X_val)}")
        
        # 保存特征名
        self.feature_names = X_train.columns.tolist()
        
        # 获取参数
        model_params = params if params else self.get_default_params()
        
        # 训练模型
        if self.model_type == 'xgboost':
            self.model = xgb.XGBRegressor(**model_params)
            
            if X_val is not None:
                eval_set = [(X_train, y_train), (X_val, y_val)]
                # XGBoost 2.0+ 使用回调函数代替 early_stopping_rounds
                self.model.fit(
                    X_train, y_train,
                    eval_set=eval_set,
                    callbacks=[EarlyStopping(rounds=50, save_best=True)],
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
        
        logger.info("模型训练完成!")
        logger.info(f"Top 10 重要特征:\n{self.feature_importance.head(10)}")
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        预测
        
        Args:
            X: 特征DataFrame
            
        Returns:
            预测值数组
        """
        if self.model is None:
            raise ValueError("模型未训练,请先调用train()方法")
        
        return self.model.predict(X)
    
    def evaluate(self, X: pd.DataFrame, y: pd.Series) -> Dict[str, float]:
        """
        评估模型
        
        Args:
            X: 特征
            y: 真实值
            
        Returns:
            评估指标字典
        """
        y_pred = self.predict(X)
        
        metrics = {
            'MAE': mean_absolute_error(y, y_pred),
            'RMSE': np.sqrt(mean_squared_error(y, y_pred)),
            'R2': r2_score(y, y_pred),
            'MAPE': np.mean(np.abs((y - y_pred) / y)) * 100  # 注意可能除零
        }
        
        logger.info("模型评估结果:")
        for metric, value in metrics.items():
            logger.info(f"  {metric}: {value:.4f}")
        
        return metrics
    
    def save_model(self, path: str) -> None:
        """
        保存模型
        
        Args:
            path: 保存路径
        """
        if self.model is None:
            raise ValueError("模型未训练,无法保存")
        
        model_data = {
            'model': self.model,
            'model_type': self.model_type,
            'feature_names': self.feature_names,
            'feature_importance': self.feature_importance
        }
        
        joblib.dump(model_data, path)
        logger.info(f"模型已保存到: {path}")
    
    def load_model(self, path: str) -> None:
        """
        加载模型
        
        Args:
            path: 模型路径
        """
        model_data = joblib.load(path)
        
        self.model = model_data['model']
        self.model_type = model_data['model_type']
        self.feature_names = model_data['feature_names']
        self.feature_importance = model_data['feature_importance']
        
        logger.info(f"模型已从 {path} 加载")


def prepare_training_data(df: pd.DataFrame, 
                         target_col: str = 'price') -> Tuple[pd.DataFrame, pd.Series]:
    """
    准备训练数据
    
    Args:
        df: 完整DataFrame
        target_col: 目标列名
        
    Returns:
        (X, y) 特征和目标
    """
    # 移除缺失目标值的行
    df = df.dropna(subset=[target_col])
    
    # 分离特征和目标
    exclude_cols = [target_col, 'timestamp']
    feature_cols = [col for col in df.columns if col not in exclude_cols]
    
    X = df[feature_cols].fillna(0)  # 填充剩余缺失值
    y = df[target_col]
    
    logger.info(f"特征数量: {X.shape[1]}")
    logger.info(f"样本数量: {X.shape[0]}")
    
    return X, y


def main():
    """测试函数"""
    from sklearn.model_selection import train_test_split
    
    # 创建测试数据
    np.random.seed(42)
    n_samples = 1000
    
    X = pd.DataFrame({
        'feature_1': np.random.randn(n_samples),
        'feature_2': np.random.randn(n_samples),
        'feature_3': np.random.randn(n_samples),
        'hour': np.random.randint(0, 24, n_samples),
        'load': np.random.uniform(5000, 10000, n_samples)
    })
    
    # 创建目标(带噪声的线性关系)
    y = (X['feature_1'] * 10 + X['feature_2'] * 5 + 
         X['hour'] * 2 + X['load'] * 0.01 + 
         np.random.randn(n_samples) * 5)
    
    # 分割数据
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    # 训练模型
    model = ElectricityPriceModel(model_type='xgboost')
    model.train(X_train, y_train, X_test, y_test)
    
    # 评估
    metrics = model.evaluate(X_test, y_test)
    
    print(f"\n测试MAE: {metrics['MAE']:.2f}")


if __name__ == "__main__":
    main()

