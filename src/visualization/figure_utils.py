# src/visualization/figure_utils.py

# Standard Library Imports
from datetime import datetime, timedelta

# Third-Party Imports
import pandas as pd
import plotly.graph_objects as go

# Local Application Imports
from config.config import DAY_SESSION_START_TIME, DEFAULT_LOOKBACK_MINUTES
from config.run_context import RunContext
from src.web.shared_state import shared_state


"""
存放公用的 Plotly 圖表物件、樣式字典與輔助函式。
"""

# ------------------------------------------------------------
# 📦 1. 共用顏色
# ------------------------------------------------------------
COLOR_BG = 'black'
COLOR_INCREASING = 'green'
COLOR_DECREASING = 'red'
COLOR_CANDLE_VOL_DAY = 'yellow'

# ------------------------------------------------------------
# 📦 2. 共用圖表物件 (空白圖)
# ------------------------------------------------------------
BLANK_BLACK_FIGURE = go.Figure(
    layout=go.Layout(
        paper_bgcolor=COLOR_BG,
        plot_bgcolor=COLOR_BG,
        xaxis_visible=False,
        yaxis_visible=False
    )
)

# ------------------------------------------------------------
# 📦 3. 共用樣式字典 (Layouts)
# ------------------------------------------------------------

# --- (A) 主要 Layout (K線圖、主圖) ---
COMMON_LAYOUT_SETTINGS = dict(
    template='plotly_dark',             # 套用暗黑主題
    hovermode='x unified',              # X軸統一 hover 效果
    xaxis_rangeslider_visible=False,    # 關閉 X 軸的範圍滑桿
)

# --- (B) 共用 X 軸樣式 (適用所有子圖) ---
COMMON_XAXIS_SETTINGS = dict(
    showspikes=True,
    spikemode='across',
    spikesnap='cursor',
    showline=True,
    spikethickness=1,
    showticklabels=True,
    showgrid=True,
)

# --- (C) 共用 Y 軸樣式 (價格) ---
PRICE_YAXIS_SETTINGS = dict(
    title_text="Price", 
    tickformat=".0f", 
    showgrid=True, 
)

# --- (D) 共用 Y 軸樣式 (成交量) ---
VOLUME_YAXIS_SETTINGS = dict(
    title_text="Volume",
    showgrid=True, 
)

# ------------------------------------------------------------
# 📦 4. 輔助函式 (X 軸時間範圍)
# ------------------------------------------------------------

def get_time_range(df: pd.DataFrame, ctx: RunContext) -> tuple[datetime, datetime] | None:
    """
    (主要介面)
    根據即時或歷史模式，取得正確的圖表 X 軸時間範圍。
    """
    if ctx.real_time_mode:
        # 即時模式：顯示最後 N 分鐘的滑動視窗
        return _get_sliding_window(df, ctx.start_datetime, shared_state.ui_lookback_minutes)
    else:
        # 歷史模式：交給 autorange 處理
        return None
    

# --- (內部輔助函式) ---

def _get_sliding_window(
    df: pd.DataFrame,
    start: datetime,
    lookback_minutes: int = DEFAULT_LOOKBACK_MINUTES,
) -> tuple[datetime, datetime]:
    """
    (繪圖用) 計算「滑動」時間視窗
    (從最後一筆 tick 往前推 N 分鐘)
    """
    # (日盤 8:30 + 15 = 8:45; 夜盤 14:50 + 10 = 15:00)
    adjusted_start = start + timedelta(minutes=15 if start.time() == DAY_SESSION_START_TIME else 10)
    end = df['datetime'].iloc[-1] if not df.empty else adjusted_start
    
    # 確保視窗起點不會早於 adjusted_start (開盤前15分鐘)
    window_start = max(adjusted_start, end - timedelta(minutes=lookback_minutes))
    return window_start, end