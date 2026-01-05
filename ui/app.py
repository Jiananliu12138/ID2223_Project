"""
Streamlit可视化界面
展示电力价格预测和"洗衣计时器"
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import json
import os
import sys

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 页面配置
st.set_page_config(
    page_title="SE3电力价格预测",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定义CSS
st.markdown("""
<style>
    .main-header {
        font-size: 48px;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 20px;
    }
    .sub-header {
        font-size: 24px;
        color: #ff7f0e;
        margin-top: 30px;
        margin-bottom: 10px;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    .cheapest-hour {
        background-color: #d4edda;
        padding: 10px;
        border-left: 4px solid #28a745;
        margin: 5px 0;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=3600)
def load_predictions():
    """加载最新预测数据"""
    try:
        pred_file = "../predictions/latest_predictions.json"
        
        if not os.path.exists(pred_file):
            return None
        
        with open(pred_file, 'r') as f:
            data = json.load(f)
        
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Ensure timestamps are sorted so line charts draw smoothly
        df = df.sort_values('timestamp').reset_index(drop=True)

        return df
    except Exception as e:
        st.error(f"加载预测数据失败: {e}")
        return None


def plot_price_comparison(df: pd.DataFrame):
    """绘制价格对比图表"""
    # Use a sorted copy so lines follow time order
    df_sorted = df.sort_values('timestamp')
    fig = go.Figure()

    # Predicted price
    fig.add_trace(go.Scatter(
        x=df_sorted['timestamp'],
        y=df_sorted['predicted_price'],
        mode='lines+markers',
        name='预测价格',
        line=dict(color='#1f77b4', width=2),
        marker=dict(size=6)
    ))

    # Actual price (if available)
    if 'actual_price' in df_sorted.columns:
        actual_df = df_sorted.dropna(subset=['actual_price'])
        if len(actual_df) > 0:
            fig.add_trace(go.Scatter(
                x=actual_df['timestamp'],
                y=actual_df['actual_price'],
                mode='lines+markers',
                name='实际价格',
                line=dict(color='#ff7f0e', width=2),
                marker=dict(size=6)
            ))
    
    fig.update_layout(
        title='SE3区域电力价格预测',
        xaxis_title='时间',
        yaxis_title='价格 (EUR/MWh)',
        hovermode='x unified',
        height=500,
        template='plotly_white'
    )
    
    return fig


def plot_hourly_heatmap(df: pd.DataFrame):
    """绘制小时热力图"""
    # 添加日期和小时列
    df['date'] = df['timestamp'].dt.date
    df['hour'] = df['timestamp'].dt.hour
    
    # 透视表
    pivot = df.pivot(index='date', columns='hour', values='predicted_price')
    # Ensure rows and columns are in chronological order
    pivot = pivot.sort_index()
    pivot = pivot.reindex(sorted(pivot.columns), axis=1)

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns,
        y=pivot.index,
        colorscale='RdYlGn_r',
        colorbar=dict(title="EUR/MWh")
    ))
    
    fig.update_layout(
        title='每小时价格热力图',
        xaxis_title='小时',
        yaxis_title='日期',
        height=400
    )
    
    return fig


def display_laundry_ticker(df: pd.DataFrame):
    """显示"洗衣计时器" - 最便宜的用电时段"""
    st.markdown('<div class="sub-header">🧺 洗衣计时器 - 最佳用电时段</div>', 
                unsafe_allow_html=True)
    
    # 找出最便宜的4小时
    cheapest = df.nsmallest(4, 'predicted_price').sort_values('timestamp')
    
    st.info("💡 以下是未来24小时内电价最低的4个时段,适合运行洗衣机、烘干机等高耗电设备!")
    
    cols = st.columns(4)
    
    for idx, (_, row) in enumerate(cheapest.iterrows()):
        with cols[idx]:
            st.markdown(f"""
            <div class="metric-card">
                <h3 style="color: #28a745; margin: 0;">排名 #{idx+1}</h3>
                <p style="font-size: 18px; margin: 10px 0;">
                    <strong>{row['timestamp'].strftime('%m-%d %H:%M')}</strong>
                </p>
                <p style="font-size: 24px; color: #1f77b4; font-weight: bold; margin: 0;">
                    {row['predicted_price']:.2f} EUR/MWh
                </p>
            </div>
            """, unsafe_allow_html=True)


def display_metrics(df: pd.DataFrame):
    """显示关键指标"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_price = df['predicted_price'].mean()
        st.metric("平均电价", f"{avg_price:.2f} EUR/MWh")
    
    with col2:
        min_price = df['predicted_price'].min()
        st.metric("最低电价", f"{min_price:.2f} EUR/MWh", 
                 delta=f"{min_price - avg_price:.2f}")
    
    with col3:
        max_price = df['predicted_price'].max()
        st.metric("最高电价", f"{max_price:.2f} EUR/MWh",
                 delta=f"{max_price - avg_price:.2f}")
    
    with col4:
        if 'actual_price' in df.columns:
            mae = df.dropna(subset=['actual_price'])['abs_error'].mean()
            st.metric("预测误差 (MAE)", f"{mae:.2f} EUR/MWh")
        else:
            st.metric("数据状态", "✅ 已更新")


def main():
    """主函数"""
    # 标题
    st.markdown('<div class="main-header">⚡ SE3电力价格预测系统</div>', 
                unsafe_allow_html=True)
    
    st.markdown("""
    <div style="text-align: center; color: #666; margin-bottom: 30px;">
        实时预测斯德哥尔摩地区(SE3)的日前电力市场价格 | 
        基于XGBoost机器学习模型 | 
        由Hopsworks Feature Store驱动
    </div>
    """, unsafe_allow_html=True)
    
    # 侧边栏
    with st.sidebar:
        st.image("https://upload.wikimedia.org/wikipedia/commons/4/4c/Flag_of_Sweden.svg", 
                width=100)
        st.title("📊 控制面板")
        
        st.info("""
        **关于本系统**
        
        本系统使用机器学习预测瑞典SE3区域的电力价格,帮助用户:
        
        - 📈 了解未来24小时电价趋势
        - 💰 找到最便宜的用电时段
        - ⚡ 优化高耗电设备使用时间
        - 🌍 支持可再生能源消纳
        """)
        
        if st.button("🔄 刷新数据"):
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        st.markdown("**数据来源:**")
        st.markdown("- ENTSO-E Transparency Platform")
        st.markdown("- Open-Meteo Weather API")
        
    # 加载数据
    df = load_predictions()
    
    if df is None or len(df) == 0:
        st.error("⚠️ 暂无预测数据。请确保推理管道已运行。")
        st.info("运行推理管道: `python pipelines/4_inference_pipeline.py`")
        return
    
    # 显示最后更新时间
    st.success(f"📅 最后更新: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 关键指标
    display_metrics(df)
    
    st.markdown("---")
    
    # 主图表
    fig_main = plot_price_comparison(df)
    st.plotly_chart(fig_main, use_container_width=True)
    
    # 洗衣计时器
    display_laundry_ticker(df)
    
    st.markdown("---")
    
    # 详细分析
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 价格分布")
        fig_hist = px.histogram(
            df, 
            x='predicted_price',
            nbins=20,
            title='预测价格分布',
            labels={'predicted_price': '价格 (EUR/MWh)', 'count': '频次'}
        )
        st.plotly_chart(fig_hist, use_container_width=True)
    
    with col2:
        st.subheader("🕐 按小时统计")
        df['hour'] = df['timestamp'].dt.hour
        hourly_avg = df.groupby('hour')['predicted_price'].mean().reset_index()
        
        fig_hourly = px.bar(
            hourly_avg,
            x='hour',
            y='predicted_price',
            title='各小时平均价格',
            labels={'hour': '小时', 'predicted_price': '平均价格 (EUR/MWh)'}
        )
        st.plotly_chart(fig_hourly, use_container_width=True)
    
    # 数据表
    with st.expander("📋 查看详细数据"):
        st.dataframe(
            df[['timestamp', 'predicted_price']].style.format({
                'predicted_price': '{:.2f}'
            }),
            use_container_width=True
        )
    
    # 页脚
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #999; font-size: 12px;">
        ID2223 Scalable Machine Learning Project | 
        Powered by Hopsworks, XGBoost & Streamlit |
        © 2024
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()

