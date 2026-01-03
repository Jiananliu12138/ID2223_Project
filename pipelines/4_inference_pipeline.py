"""
推理管道
获取最新特征,使用训练好的模型预测未来24小时电价
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from features.feature_groups import FeatureStoreManager
from features.feature_engineering import FeatureEngineer
from models.trainer import ElectricityPriceModel
from config.settings import MODEL_NAME, TIMEZONE
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_inference():
    """推理流程"""
    logger.info(f"\n{'='*70}")
    logger.info(f"推理管道 - {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*70}\n")
    
    try:
        # 1. 连接Feature Store
        logger.info("步骤 1/6: 连接Feature Store...")
        fsm = FeatureStoreManager()
        
        # 2. 从 Feature Groups 读取原始数据
        logger.info("步骤 2/6: 从 Feature Groups 读取原始数据...")
        
        # 获取包含历史数据的时间范围（需要历史数据来计算滞后特征）
        now = datetime.now(TIMEZONE)
        # 获取过去7天到未来2天的数据（确保有足够的历史数据计算滞后特征）
        start_time = now - timedelta(days=7)
        end_time = now + timedelta(days=2)
        
        logger.info(f"  时间范围: {start_time.strftime('%Y-%m-%d')} 到 {end_time.strftime('%Y-%m-%d')}")
        
        # 从原始 Feature Groups 读取数据
        df = fsm.read_raw_feature_groups(
            start_time=start_time.strftime('%Y-%m-%d %H:%M:%S'),
            end_time=end_time.strftime('%Y-%m-%d %H:%M:%S')
        )
        
        logger.info(f"  ✅ 读取了 {len(df)} 条原始记录")
        
        # 3. 特征工程
        logger.info("步骤 3/6: 特征工程...")
        logger.info(f"  原始特征数: {len(df.columns)}")
        
        df = FeatureEngineer.engineer_features_pipeline(df, include_lag=True)
        
        logger.info(f"  工程特征数: {len(df.columns)}")
        
        # 检查是否有未来数据
        future_data = df[df['timestamp'] >= now].copy()
        
        if len(future_data) > 0:
            # 有未来数据，用于真实预测
            df = future_data
            prediction_mode = "forecast"
            logger.info(f"  ✅ 使用未来数据: {len(df)} 条（真实预测模式）")
        else:
            # 没有未来数据，使用最新的24小时数据（演示/回测模式）
            prediction_mode = "backtest"
            logger.warning("  ⚠️  没有未来数据，切换到演示/回测模式")
            logger.info("  💡 使用最新的24小时历史数据来展示模型预测能力")
            logger.info("  💡 若要真实预测，请先运行: python pipelines/2_daily_feature_pipeline.py")
            
            # 使用最新的24条记录
            df = df.tail(24).copy()
            logger.info(f"  ✅ 使用最新历史数据: {len(df)} 条（演示模式）")
        
        # 4. 加载模型
        logger.info("步骤 4/6: 加载模型...")
        
        model_path = f"models/{MODEL_NAME}.pkl"
        
        if not os.path.exists(model_path):
            # 尝试从Hopsworks下载
            logger.info("  本地模型不存在,从Hopsworks下载...")
            mr = fsm.get_model_registry()
            model_obj = mr.get_model(MODEL_NAME, version=1)
            model_dir = model_obj.download()
            model_path = os.path.join(model_dir, f"{MODEL_NAME}.pkl")
        
        model = ElectricityPriceModel()
        model.load_model(model_path)
        
        # 5. 数据清理和预测
        logger.info("步骤 5/6: 数据清理和执行预测...")
        
        # 保存 timestamp 用于结果
        timestamps = df['timestamp'].copy()
        
        # 移除非数值列
        exclude_cols = ['timestamp']
        cols_to_drop = [col for col in df.columns if col in exclude_cols or df[col].dtype == 'object']
        
        if cols_to_drop:
            logger.info(f"  移除列: {cols_to_drop}")
            df_clean = df.drop(columns=cols_to_drop)
        else:
            df_clean = df.copy()
        
        # 准备预测数据（确保特征顺序与训练时一致）
        feature_cols = model.feature_names
        
        # 检查缺失的特征
        missing_features = set(feature_cols) - set(df_clean.columns)
        if missing_features:
            logger.warning(f"  ⚠️  缺失特征: {missing_features}")
            logger.warning("  将用0填充缺失特征")
            for feat in missing_features:
                df_clean[feat] = 0
        
        X_pred = df_clean[feature_cols].fillna(0)
        logger.info(f"  ✅ 预测特征: {len(feature_cols)} 个")
        
        # 执行预测
        predictions = model.predict(X_pred)
        logger.info(f"  ✅ 完成 {len(predictions)} 个预测")
        
        # 创建预测结果DataFrame
        results_df = pd.DataFrame({
            'timestamp': timestamps,
            'predicted_price': predictions,
            'mode': prediction_mode  # 添加模式标识
        })
        
        # 如果有实际价格,添加对比
        if 'price' in df.columns:
            results_df['actual_price'] = df['price'].values
            results_df['error'] = results_df['actual_price'] - results_df['predicted_price']
            results_df['abs_error'] = np.abs(results_df['error'])
        
        mode_name = "真实预测" if prediction_mode == "forecast" else "演示/回测"
        logger.info(f"  ✅ 预测了 {len(results_df)} 个小时的电价（{mode_name}模式）")
        
        # 6. 保存预测结果
        logger.info("步骤 6/6: 保存预测结果...")
        
        # 保存为JSON(供UI使用)
        output_dir = "predictions"
        os.makedirs(output_dir, exist_ok=True)
        
        output_file = os.path.join(
            output_dir, 
            f"predictions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        
        # 转换为JSON格式
        results_json = results_df.to_dict(orient='records')
        
        with open(output_file, 'w') as f:
            json.dump(results_json, f, indent=2, default=str)
        
        # 同时保存最新预测(覆盖)
        latest_file = os.path.join(output_dir, "latest_predictions.json")
        with open(latest_file, 'w') as f:
            json.dump(results_json, f, indent=2, default=str)
        
        logger.info(f"  ✅ 预测结果已保存到: {output_file}")
        logger.info(f"  ✅ 最新预测: {latest_file}")
        
        # 打印统计信息
        logger.info(f"\n{'='*70}")
        logger.info(f"📊 预测统计 ({mode_name}模式)")
        logger.info(f"{'='*70}")
        logger.info(f"  预测模式: {mode_name}")
        logger.info(f"  预测时段: {len(results_df)} 小时")
        logger.info(f"  时间范围: {results_df['timestamp'].min()} 到 {results_df['timestamp'].max()}")
        logger.info(f"  平均预测价格: {results_df['predicted_price'].mean():.2f} EUR/MWh")
        logger.info(f"  最低预测价格: {results_df['predicted_price'].min():.2f} EUR/MWh")
        logger.info(f"  最高预测价格: {results_df['predicted_price'].max():.2f} EUR/MWh")
        
        if 'actual_price' in results_df.columns:
            mae = results_df['abs_error'].mean()
            logger.info(f"  ✅ MAE（与实际价格对比）: {mae:.2f} EUR/MWh")
        
        # 找到最便宜的4小时时段(用于"洗衣计时器")
        logger.info(f"\n💡 最便宜的4小时（推荐用电时段）:")
        cheapest_hours = results_df.nsmallest(4, 'predicted_price')
        for _, row in cheapest_hours.iterrows():
            logger.info(f"  🕐 {row['timestamp']}: {row['predicted_price']:.2f} EUR/MWh")
        
        logger.info(f"\n{'='*70}")
        logger.info("✅ 推理完成!")
        logger.info(f"{'='*70}\n")
        
        return True
        
    except Exception as e:
        logger.error(f"\n{'='*70}")
        logger.error("❌ 推理失败!")
        logger.error(f"错误信息: {e}")
        logger.error(f"{'='*70}\n")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主函数"""
    success = run_inference()
    
    if success:
        logger.info("推理管道执行成功")
        exit(0)
    else:
        logger.error("推理管道执行失败")
        exit(1)


if __name__ == "__main__":
    main()

