import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
import glob
import os
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

# --------------------------
# Config
# --------------------------
PARQUET_DIR = "/home/vboxuser/realtime-weather-pipeline/output/aggregated/"
REFRESH_INTERVAL = 60 # Seconds

st.set_page_config(page_title="Weather Dashboard", layout="wide")

# This component handles the 60-second refresh automatically
st_autorefresh(interval=REFRESH_INTERVAL * 1000, key="datarefresh")

st.title("Real-Time Weather Dashboard 🌦️")
st.sidebar.info(f"Auto-refreshing every {REFRESH_INTERVAL} seconds")

# --------------------------
# Helper to load latest parquet files
# --------------------------
@st.cache_data(ttl=REFRESH_INTERVAL)
def load_aggregated_data(parquet_dir):
    files = glob.glob(os.path.join(parquet_dir, "*.parquet"))
    if not files:
        return pd.DataFrame()
    try:
        df_list = [pq.read_table(f).to_pandas() for f in files]
        return pd.concat(df_list, ignore_index=True)
    except Exception:
        # Return empty if file is currently being written by Spark
        return pd.DataFrame()

# --------------------------
# Main Display Logic
# --------------------------
df = load_aggregated_data(PARQUET_DIR)

if df.empty:
    st.warning("Waiting for data from Spark... Make sure Terminal 3 is running.")
else:
    # Data Cleaning
    df['window_start'] = pd.to_datetime(df['window_start'])
    df['window_end'] = pd.to_datetime(df['window_end'])
    df = df.sort_values('window_start')

    # Metrics Row
    latest_time = df['window_end'].max()
    st.write(f"**Last Update From Pipeline:** {latest_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. Temperature Line Chart
    fig_temp = px.line(
        df,
        x='window_start',
        y='avg_temp',
        color='city',
        title="Average Temperature Trends (°C)",
        markers=True,
        template="plotly_dark"
    )
    st.plotly_chart(fig_temp, use_container_width=True, key="temp_chart")

    # 2. Humidity Bar Chart (Latest Data Only)
    latest_df = df[df['window_end'] == latest_time]
    fig_humidity = px.bar(
        latest_df,
        x='city',
        y='avg_humidity',
        color='city',
        title=f"Current Humidity Levels (Window Ending {latest_time.strftime('%H:%M')})",
        template="plotly_dark"
    )
    st.plotly_chart(fig_humidity, use_container_width=True, key="humidity_chart")