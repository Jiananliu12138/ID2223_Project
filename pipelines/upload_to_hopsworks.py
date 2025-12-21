"""
上传本地数据到 Hopsworks Feature Store
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from pathlib import Path
from features.feature_groups import FeatureStoreManager, LOCAL_DATA_DIR
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def upload_all_data():
    """上传所有本地保存的数据到 Hopsworks"""
    
    # 检查本地数据目录
    if not LOCAL_DATA_DIR.exists():
        logger.error(f"❌ 本地数据目录不存在: {LOCAL_DATA_DIR}")
        return
    
    # 查找所有 parquet 文件
    electricity_files = sorted(LOCAL_DATA_DIR.glob("electricity_*.parquet"))
    weather_files = sorted(LOCAL_DATA_DIR.glob("weather_*.parquet"))
    
    if not electricity_files or not weather_files:
        logger.error("❌ 未找到本地数据文件，请先运行 1_backfill_features.py")
        return
    
    logger.info(f"\n{'='*70}")
    logger.info(f"找到 {len(electricity_files)} 个电力数据文件")
    logger.info(f"找到 {len(weather_files)} 个天气数据文件")
    logger.info(f"{'='*70}\n")
    
    # 用户确认
    confirm = input("开始上传到 Hopsworks? (y/n): ")
    if confirm.lower() != 'y':
        logger.info("用户取消上传")
        return
    
    # 连接到 Hopsworks
    logger.info("\n🔗 连接到 Hopsworks...")
    try:
        fsm = FeatureStoreManager(local_only=False)
    except Exception as e:
        logger.error(f"❌ 连接失败: {e}")
        logger.error("请检查 .env 文件中的 HOPSWORKS_API_KEY 和 HOPSWORKS_PROJECT_NAME")
        return
    
    # 上传数据
    success_count = 0
    fail_count = 0
    
    for elec_file, weather_file in zip(electricity_files, weather_files):
        month = elec_file.stem.replace("electricity_", "")
        
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"上传月份: {month}")
            logger.info(f"{'='*60}")
            
            # 读取本地文件
            logger.info(f"📂 读取本地文件...")
            electricity_df = pd.read_parquet(elec_file)
            weather_df = pd.read_parquet(weather_file)
            
            logger.info(f"   电力数据: {len(electricity_df)} 行")
            logger.info(f"   天气数据: {len(weather_df)} 行")
            
            # 上传到 Hopsworks
            fsm.create_electricity_feature_group(electricity_df)
            fsm.create_weather_feature_group(weather_df)
            
            logger.info(f"✅ 月份 {month} 上传成功!")
            success_count += 1
            
        except Exception as e:
            logger.error(f"❌ 月份 {month} 上传失败: {e}")
            fail_count += 1
            
            # 询问是否继续
            if fail_count > 0:
                retry = input(f"\n上传失败，是否继续下一个月份? (y/n): ")
                if retry.lower() != 'y':
                    logger.info("用户中止上传")
                    break
    
    # 总结
    logger.info(f"\n{'='*70}")
    logger.info(f"上传完成!")
    logger.info(f"  成功: {success_count} 个月份")
    logger.info(f"  失败: {fail_count} 个月份")
    logger.info(f"{'='*70}")


def upload_specific_month(month: str):
    """
    上传指定月份的数据
    
    Args:
        month: 月份字符串，如 '2024-01'
    """
    elec_file = LOCAL_DATA_DIR / f"electricity_{month}.parquet"
    weather_file = LOCAL_DATA_DIR / f"weather_{month}.parquet"
    
    if not elec_file.exists() or not weather_file.exists():
        logger.error(f"❌ 月份 {month} 的数据文件不存在")
        return
    
    # 连接到 Hopsworks
    logger.info("🔗 连接到 Hopsworks...")
    try:
        fsm = FeatureStoreManager(local_only=False)
    except Exception as e:
        logger.error(f"❌ 连接失败: {e}")
        return
    
    try:
        # 读取并上传
        logger.info(f"📂 读取月份 {month} 的数据...")
        electricity_df = pd.read_parquet(elec_file)
        weather_df = pd.read_parquet(weather_file)
        
        fsm.create_electricity_feature_group(electricity_df)
        fsm.create_weather_feature_group(weather_df)
        
        logger.info(f"✅ 月份 {month} 上传成功!")
        
    except Exception as e:
        logger.error(f"❌ 上传失败: {e}")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="上传本地数据到 Hopsworks")
    parser.add_argument('--month', type=str, help='指定月份，如 2024-01')
    parser.add_argument('--all', action='store_true', help='上传所有月份')
    
    args = parser.parse_args()
    
    if args.month:
        upload_specific_month(args.month)
    elif args.all:
        upload_all_data()
    else:
        # 默认：上传所有
        upload_all_data()


if __name__ == "__main__":
    main()

