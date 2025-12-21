"""
数据清洗与预处理模块
"""
import pandas as pd
import numpy as np
from config.settings import MAX_MISSING_HOURS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataCleaner:
    """数据清洗工具类"""
    
    @staticmethod
    def check_missing_data(df: pd.DataFrame) -> dict:
        """
        检查缺失数据统计
        
        Args:
            df: 输入DataFrame
            
        Returns:
            缺失数据统计字典
        """
        missing_stats = {}
        for col in df.columns:
            missing_count = df[col].isna().sum()
            missing_pct = (missing_count / len(df)) * 100
            if missing_count > 0:
                missing_stats[col] = {
                    'count': missing_count,
                    'percentage': round(missing_pct, 2)
                }
        
        return missing_stats
    
    @staticmethod
    def interpolate_missing(df: pd.DataFrame, 
                           max_gap_hours: int = 3,
                           method: str = 'linear') -> pd.DataFrame:
        """
        插值填充缺失数据
        
        Args:
            df: 输入DataFrame
            max_gap_hours: 最大允许插值的连续缺失小时数
            method: 插值方法 ('linear', 'time', 'polynomial')
            
        Returns:
            填充后的DataFrame
        """
        df = df.copy()
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            # 识别缺失值块
            is_missing = df[col].isna()
            missing_blocks = (is_missing != is_missing.shift()).cumsum()
            
            for block_id in missing_blocks[is_missing].unique():
                block_mask = (missing_blocks == block_id) & is_missing
                block_size = block_mask.sum()
                
                if block_size <= max_gap_hours:
                    # 小缺口:使用插值
                    df.loc[block_mask, col] = df[col].interpolate(method=method)[block_mask]
                    logger.info(f"{col}: 插值填充 {block_size} 个缺失值")
                else:
                    # 大缺口:使用前向填充
                    df.loc[block_mask, col] = df[col].ffill()[block_mask]
                    logger.warning(f"{col}: 连续缺失 {block_size} 小时,使用前向填充")
        
        # 最后处理剩余的缺失值(如开头)
        df = df.bfill().ffill()
        
        return df
    
    @staticmethod
    def remove_outliers(df: pd.DataFrame, 
                       columns: list,
                       n_std: float = 4.0) -> pd.DataFrame:
        """
        移除异常值(基于标准差)
        
        Args:
            df: 输入DataFrame
            columns: 需要检查的列名列表
            n_std: 标准差倍数阈值
            
        Returns:
            处理后的DataFrame
        """
        df = df.copy()
        
        for col in columns:
            if col in df.columns:
                mean = df[col].mean()
                std = df[col].std()
                
                lower_bound = mean - n_std * std
                upper_bound = mean + n_std * std
                
                outliers = (df[col] < lower_bound) | (df[col] > upper_bound)
                n_outliers = outliers.sum()
                
                if n_outliers > 0:
                    logger.info(f"{col}: 发现 {n_outliers} 个异常值,将替换为边界值")
                    df.loc[df[col] < lower_bound, col] = lower_bound
                    df.loc[df[col] > upper_bound, col] = upper_bound
        
        return df
    
    @staticmethod
    def validate_price_range(df: pd.DataFrame, 
                            price_col: str = 'price',
                            min_price: float = -500,
                            max_price: float = 1000) -> pd.DataFrame:
        """
        验证价格范围(欧洲市场可能有负价格)
        
        Args:
            df: 输入DataFrame
            price_col: 价格列名
            min_price: 最小合理价格 (EUR/MWh)
            max_price: 最大合理价格 (EUR/MWh)
            
        Returns:
            验证后的DataFrame
        """
        df = df.copy()
        
        if price_col in df.columns:
            invalid_prices = (df[price_col] < min_price) | (df[price_col] > max_price)
            n_invalid = invalid_prices.sum()
            
            if n_invalid > 0:
                logger.warning(f"发现 {n_invalid} 个超出合理范围的价格值")
                # 替换为NaN,后续用插值处理
                df.loc[invalid_prices, price_col] = np.nan
        
        return df
    
    @staticmethod
    def ensure_hourly_continuity(df: pd.DataFrame, 
                                 timestamp_col: str = 'timestamp') -> pd.DataFrame:
        """
        确保时间序列连续性,填充缺失的小时
        
        Args:
            df: 输入DataFrame
            timestamp_col: 时间戳列名
            
        Returns:
            连续的DataFrame
        """
        df = df.copy()
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        df = df.sort_values(timestamp_col)
        
        # 创建完整的小时索引
        full_range = pd.date_range(
            start=df[timestamp_col].min(),
            end=df[timestamp_col].max(),
            freq='h'  # 小写h (大写H已弃用)
        )
        
        # 重新索引
        df = df.set_index(timestamp_col)
        df = df.reindex(full_range)
        df.index.name = timestamp_col
        df = df.reset_index()
        
        missing_hours = df.isna().any(axis=1).sum()
        if missing_hours > 0:
            logger.info(f"填充了 {missing_hours} 个缺失的时间点")
        
        return df
    
    @staticmethod
    def clean_pipeline(df: pd.DataFrame) -> pd.DataFrame:
        """
        完整的数据清洗管道
        
        Args:
            df: 原始DataFrame
            
        Returns:
            清洗后的DataFrame
        """
        logger.info("开始数据清洗流程...")
        
        # 1. 确保时间连续性
        df = DataCleaner.ensure_hourly_continuity(df)
        
        # 2. 检查缺失数据
        missing_stats = DataCleaner.check_missing_data(df)
        if missing_stats:
            logger.info(f"缺失数据统计: {missing_stats}")
        
        # 3. 价格范围验证
        if 'price' in df.columns:
            df = DataCleaner.validate_price_range(df)
        
        # 4. 插值填充
        df = DataCleaner.interpolate_missing(df, max_gap_hours=MAX_MISSING_HOURS)
        
        # 5. 移除异常值(除了价格,价格可能合理地很极端)
        outlier_cols = [col for col in df.columns 
                       if col not in ['timestamp', 'price'] and df[col].dtype in [np.float64, np.int64]]
        if outlier_cols:
            df = DataCleaner.remove_outliers(df, outlier_cols)
        
        logger.info("数据清洗完成")
        return df


def main():
    """测试函数"""
    # 创建测试数据
    dates = pd.date_range('2024-01-01', periods=100, freq='h')
    df = pd.DataFrame({
        'timestamp': dates,
        'price': np.random.uniform(20, 80, 100),
        'load': np.random.uniform(5000, 10000, 100)
    })
    
    # 人工引入缺失值
    df.loc[10:12, 'price'] = np.nan
    df.loc[50:56, 'load'] = np.nan
    
    print("清洗前:")
    print(DataCleaner.check_missing_data(df))
    
    # 清洗
    df_clean = DataCleaner.clean_pipeline(df)
    
    print("\n清洗后:")
    print(DataCleaner.check_missing_data(df_clean))


if __name__ == "__main__":
    main()

