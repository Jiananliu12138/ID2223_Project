"""
Data Cleaning and Preprocessing Module
"""
import pandas as pd
import numpy as np
from config.settings import MAX_MISSING_HOURS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataCleaner:
    """Data cleaning utility class"""
    
    @staticmethod
    def check_missing_data(df: pd.DataFrame) -> dict:
        """
        Check missing data statistics
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary of missing data statistics
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
        Interpolate and fill missing data
        
        Args:
            df: Input DataFrame
            max_gap_hours: Maximum allowed consecutive missing hours for interpolation
            method: Interpolation method ('linear', 'time', 'polynomial')
            
        Returns:
            Filled DataFrame
        """
        df = df.copy()
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            # Identify missing value blocks
            is_missing = df[col].isna()
            missing_blocks = (is_missing != is_missing.shift()).cumsum()
            
            for block_id in missing_blocks[is_missing].unique():
                block_mask = (missing_blocks == block_id) & is_missing
                block_size = block_mask.sum()
                
                if block_size <= max_gap_hours:
                    # Small gaps: use interpolation
                    df.loc[block_mask, col] = df[col].interpolate(method=method)[block_mask]
                    logger.info(f"{col}: Interpolated {block_size} missing values")
                else:
                    # Large gaps: use forward fill
                    df.loc[block_mask, col] = df[col].ffill()[block_mask]
                    logger.warning(f"{col}: Consecutive {block_size} hours missing, using forward fill")
        
        # Finally handle remaining missing values (e.g., at the beginning)
        df = df.bfill().ffill()
        
        return df
    
    @staticmethod
    def remove_outliers(df: pd.DataFrame, 
                       columns: list,
                       n_std: float = 4.0) -> pd.DataFrame:
        """
        Remove outliers (based on standard deviation)
        
        Args:
            df: Input DataFrame
            columns: List of column names to check
            n_std: Standard deviation multiplier threshold
            
        Returns:
            Processed DataFrame
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
                    logger.info(f"{col}: Found {n_outliers} outliers, will replace with boundary values")
                    df.loc[df[col] < lower_bound, col] = lower_bound
                    df.loc[df[col] > upper_bound, col] = upper_bound
        
        return df
    
    @staticmethod
    def validate_price_range(df: pd.DataFrame, 
                            price_col: str = 'price',
                            min_price: float = -500,
                            max_price: float = 1000) -> pd.DataFrame:
        """
        Validate price range (European market may have negative prices)
        
        Args:
            df: Input DataFrame
            price_col: Price column name
            min_price: Minimum reasonable price (EUR/MWh)
            max_price: Maximum reasonable price (EUR/MWh)
            
        Returns:
            Validated DataFrame
        """
        df = df.copy()
        
        if price_col in df.columns:
            invalid_prices = (df[price_col] < min_price) | (df[price_col] > max_price)
            n_invalid = invalid_prices.sum()
            
            if n_invalid > 0:
                logger.warning(f"Found {n_invalid} prices out of reasonable range")
                # Replace with NaN, will be handled with interpolation later
                df.loc[invalid_prices, price_col] = np.nan
        
        return df
    
    @staticmethod
    def ensure_hourly_continuity(df: pd.DataFrame, 
                                 timestamp_col: str = 'timestamp') -> pd.DataFrame:
        """
        Ensure hourly continuity of time series, fill missing hours
        
        Args:
            df: Input DataFrame
            timestamp_col: Timestamp column name
            
        Returns:
            Continuous DataFrame
        """
        df = df.copy()
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        df = df.sort_values(timestamp_col)
        
        # Create complete hourly index
        full_range = pd.date_range(
            start=df[timestamp_col].min(),
            end=df[timestamp_col].max(),
            freq='h'  # lowercase h (uppercase H is deprecated)
        )
        
        # Reindex
        df = df.set_index(timestamp_col)
        df = df.reindex(full_range)
        df.index.name = timestamp_col
        df = df.reset_index()
        
        missing_hours = df.isna().any(axis=1).sum()
        if missing_hours > 0:
            logger.info(f"Filled {missing_hours} missing time points")
        
        return df
    
    @staticmethod
    def clean_pipeline(df: pd.DataFrame) -> pd.DataFrame:
        """
        Complete data cleaning pipeline
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("Starting data cleaning pipeline...")
        
        # 1. Ensure time continuity
        df = DataCleaner.ensure_hourly_continuity(df)
        
        # 2. Check missing data
        missing_stats = DataCleaner.check_missing_data(df)
        if missing_stats:
            logger.info(f"Missing data statistics: {missing_stats}")
        
        # 3. Price range validation
        if 'price' in df.columns:
            df = DataCleaner.validate_price_range(df)
        
        # 4. Interpolate and fill
        df = DataCleaner.interpolate_missing(df, max_gap_hours=MAX_MISSING_HOURS)
        
        # 5. Remove outliers (except price, which may reasonably be extreme)
        outlier_cols = [col for col in df.columns 
                       if col not in ['timestamp', 'price'] and df[col].dtype in [np.float64, np.int64]]
        if outlier_cols:
            df = DataCleaner.remove_outliers(df, outlier_cols)
        
        logger.info("Data cleaning complete")
        return df


def main():
    """Test function"""
    # Create test data
    dates = pd.date_range('2024-01-01', periods=100, freq='h')
    df = pd.DataFrame({
        'timestamp': dates,
        'price': np.random.uniform(20, 80, 100),
        'load': np.random.uniform(5000, 10000, 100)
    })
    
    # Manually introduce missing values
    df.loc[10:12, 'price'] = np.nan
    df.loc[50:56, 'load'] = np.nan
    
    print("Before cleaning:")
    print(DataCleaner.check_missing_data(df))
    
    # Clean data
    df_clean = DataCleaner.clean_pipeline(df)
    
    print("\nAfter cleaning:")
    print(DataCleaner.check_missing_data(df_clean))


if __name__ == "__main__":
    main()

