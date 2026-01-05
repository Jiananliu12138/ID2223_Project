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

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Page configuration
st.set_page_config(
    page_title="SE3 Electricity Price Prediction",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
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
    """Load the latest prediction data"""
    try:
        # Use an absolute path relative to this file so Streamlit working dir doesn't matter
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        pred_file = os.path.join(base_dir, 'predictions', 'latest_predictions.json')
        
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
        st.error(f"Failed to load prediction data: {e}")
        return None


def plot_price_comparison(df: pd.DataFrame):
    """Plot price comparison chart"""
    # Use a sorted copy so lines follow time order
    df_sorted = df.sort_values('timestamp').reset_index(drop=True)

    # Base (neutral) line to ensure a continuous timeline
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_sorted['timestamp'],
        y=df_sorted['predicted_price'],
        mode='lines',
        name='Predicted (base)',
        line=dict(color='lightgray', width=2),
        hoverinfo='skip',
        showlegend=False
    ))

    # Colored segments for backtest and forecast by masking values to NaN outside mode
    if 'mode' in df_sorted.columns:
        back_y = [v if m == 'backtest' else None for v, m in zip(df_sorted['predicted_price'], df_sorted['mode'])]
        fcast_y = [v if m == 'forecast' else None for v, m in zip(df_sorted['predicted_price'], df_sorted['mode'])]

        fig.add_trace(go.Scatter(
            x=df_sorted['timestamp'],
            y=back_y,
            mode='lines+markers',
            name='Backtest',
            line=dict(color='#636EFA', width=3),
            marker=dict(size=6)
        ))

        fig.add_trace(go.Scatter(
            x=df_sorted['timestamp'],
            y=fcast_y,
            mode='lines+markers',
            name='Forecast',
            line=dict(color='#EF553B', width=3),
            marker=dict(size=6)
        ))
    else:
        # Fallback: single color if no mode column
        fig.add_trace(go.Scatter(
            x=df_sorted['timestamp'],
            y=df_sorted['predicted_price'],
            mode='lines+markers',
            name='Predicted Price',
            line=dict(color='#1f77b4', width=2),
            marker=dict(size=6)
        ))

    # Actual price (if available) as a separate trace
    if 'actual_price' in df_sorted.columns:
        actual_df = df_sorted.dropna(subset=['actual_price'])
        if len(actual_df) > 0:
            fig.add_trace(go.Scatter(
                x=actual_df['timestamp'],
                y=actual_df['actual_price'],
                mode='lines+markers',
                name='Actual Price',
                line=dict(color='#00CC96', width=2, dash='dash'),
                marker=dict(size=6)
            ))
    
    fig.update_layout(
        title='SE3 Region Electricity Price Prediction',
        xaxis_title='Time',
        yaxis_title='Price (EUR/MWh)',
        hovermode='x unified',
        height=500,
        template='plotly_white'
    )
    
    return fig


def select_display_df(df: pd.DataFrame) -> pd.DataFrame:
    """Select which subset to display in the main UI.

    - If the predictions contain a 'mode' column and there are 'forecast' rows,
      prefer showing only the forecast records (future predictions).
    - Otherwise, return the full dataframe (maintains backward compatibility).
    """
    if 'mode' in df.columns:
        if (df['mode'] == 'forecast').any():
            return df[df['mode'] == 'forecast'].copy()
    return df


def plot_hourly_heatmap(df: pd.DataFrame):
    """Plot hourly heatmap"""
    # Add date and hour columns
    df['date'] = df['timestamp'].dt.date
    df['hour'] = df['timestamp'].dt.hour
    
    # Pivot table
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
        title='Hourly Price Heatmap',
        xaxis_title='Hour',
        yaxis_title='Date',
        height=400
    )
    
    return fig


def display_laundry_ticker(df: pd.DataFrame):
    """Display the 'Laundry Timer' - cheapest electricity periods"""
    st.markdown('<div class="sub-header">🧺 Best period for laundry</div>', 
                unsafe_allow_html=True)
    # Limit search to the next 24 hours for the cheapest 4 hours (prefer future periods)
    try:
        now = pd.Timestamp.now(tz='UTC')
        window_end = now + pd.Timedelta(hours=24)
        future_window = df[(df['timestamp'] >= now) & (df['timestamp'] <= window_end)]
    except Exception:
        # Fallback: use full dataset if time comparison fails
        future_window = df

    # Select 4 cheapest periods within next 24 hours; fallback to full data if none
    if len(future_window) >= 4:
        cheapest = future_window.nsmallest(4, 'predicted_price').reset_index(drop=True)
    else:
        cheapest = df.nsmallest(4, 'predicted_price').reset_index(drop=True)

    st.info("💡 Below is the cheapest 4-hour period for laundry.")
    
    cols = st.columns(4)
    
    # 'cheapest' is sorted ascending by price; index is price rank
    for idx, (_, row) in enumerate(cheapest.iterrows()):
        with cols[idx]:
            st.markdown(f"""
            <div class="metric-card">
                <h3 style="color: #28a745; margin: 0;">Ranked #{idx+1}</h3>
                <p style="font-size: 18px; margin: 10px 0;">
                    <strong>{row['timestamp'].strftime('%m-%d %H:%M')}</strong>
                </p>
                <p style="font-size: 24px; color: #1f77b4; font-weight: bold; margin: 0;">
                    {row['predicted_price']:.2f} EUR/MWh
                </p>
            </div>
            """, unsafe_allow_html=True)


def display_metrics(df: pd.DataFrame):
    """Display key metrics"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_price = df['predicted_price'].mean()
        st.metric("Average Price", f"{avg_price:.2f} EUR/MWh")
    
    with col2:
        min_price = df['predicted_price'].min()
        st.metric("Lowest Price", f"{min_price:.2f} EUR/MWh", 
                 delta=f"{min_price - avg_price:.2f}")
    
    with col3:
        max_price = df['predicted_price'].max()
        st.metric("Highest Price", f"{max_price:.2f} EUR/MWh",
                 delta=f"{max_price - avg_price:.2f}")
    
    with col4:
        if 'actual_price' in df.columns:
            mae = df.dropna(subset=['actual_price'])['abs_error'].mean()
            st.metric("Prediction Error (MAE)", f"{mae:.2f} EUR/MWh")
        else:
            st.metric("Data Status", "✅ Updated")


def main():
    """Main function"""
    # Header
    st.markdown('<div class="main-header">⚡ Electricity Price Prediction System for SE3</div>', 
                unsafe_allow_html=True)
    
    st.markdown("""
    <div style="text-align: center; color: #666; margin-bottom: 30px;">
        Real-time prediction of SE3 electricity prices | 
        Powered by XGBoost machine learning model | 
        Driven by Hopsworks Feature Store
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.image("https://upload.wikimedia.org/wikipedia/commons/4/4c/Flag_of_Sweden.svg", 
                width=100)
        st.title("📊 Dashboard")
        
        st.info("""
        About This System

This system uses machine learning to predict electricity prices in Sweden's SE3 region, helping users:

📈 Understand electricity price trends for the next 24 hours

💰 Find the cheapest periods to use electricity

⚡ Optimize the timing for running high‑power appliances (e.g., washers, dryers)
        """)
        
        if st.button("🔄 Refresh"):
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        st.markdown("**Data Source:**")
        st.markdown("- ENTSO-E Transparency Platform")
        st.markdown("- Open-Meteo Weather API")
        
    # Load data
    df = load_predictions()
    
    if df is None or len(df) == 0:
        st.error("⚠️ 暂无预测数据。请确保推理管道已运行。")
        st.info("运行推理管道: `python pipelines/4_inference_pipeline.py`")
        return
    
    # Show last updated timestamp
    st.success(f"📅 Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Key metrics (use whole dataset)
    display_metrics(df)
    
    st.markdown("---")
    
    # Main chart
    fig_main = plot_price_comparison(df)
    st.plotly_chart(fig_main, use_container_width=True)
    
    # Laundry timer
    display_laundry_ticker(df)
    
    st.markdown("---")
    
    # Detailed analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Price Distribution")
        fig_hist = px.histogram(
            df, 
            x='predicted_price',
            nbins=20,
            title='Predicted Price Distribution',
            labels={'predicted_price': 'price (EUR/MWh)', 'count': 'frequency'}
        )
        st.plotly_chart(fig_hist, use_container_width=True)
    
    with col2:
        st.subheader("🕐 Hourly Statistics")
        df['hour'] = df['timestamp'].dt.hour
        hourly_avg = df.groupby('hour')['predicted_price'].mean().reset_index()
        
        fig_hourly = px.bar(
            hourly_avg,
            x='hour',
            y='predicted_price',
            title='Average Price by Hour',
            labels={'hour': '小时', 'predicted_price': '平均价格 (EUR/MWh)'}
        )
        st.plotly_chart(fig_hourly, use_container_width=True)
    
    # Data table
    with st.expander("📋 Details"):
        # Show full table including mode if present
        cols = ['timestamp', 'predicted_price']
        if 'mode' in df.columns:
            cols.append('mode')
        if 'actual_price' in df.columns:
            cols.append('actual_price')

        st.dataframe(
            df[cols].style.format({
                'predicted_price': '{:.2f}'
            }),
            use_container_width=True
        )
    
    # Footer
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

