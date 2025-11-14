# src/visualization/figure_utils.py

# Standard Library Imports
from datetime import datetime

# Third-Party Imports
import pandas as pd
import plotly.graph_objects as go

# Local Application Imports
from config.run_context import RunContext
from src.utils.session_time import get_observation_window, get_sliding_window
from src.web.shared_state import shared_state

"""
存放公用的 Plotly 圖表物件、樣式字典與輔助函式。
"""

# ------------------------------------------------------------
# 1. 共用顏色
# ------------------------------------------------------------
COLOR_BG = 'black'
COLOR_INCREASING = 'green'
COLOR_DECREASING = 'red'
COLOR_CANDLE_VOL_DAY = 'yellow'

# ------------------------------------------------------------
# 2. 共用圖表物件 (空白圖)
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
# 3. 共用樣式字典 (Layouts)
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
# 4. 共用輔助函式 (X 軸時間範圍)
# ------------------------------------------------------------

def get_time_range(df: pd.DataFrame, ctx: RunContext) -> tuple[datetime, datetime]:
    """
    根據即時或歷史模式，取得正確的圖表 X 軸時間範圍。
    """
    if ctx.real_time_mode:
        # 即時模式：顯示最後 30 分鐘的滑動視窗
        return get_sliding_window(df, ctx.start_datetime, shared_state.ui_lookback_minutes)
    else:
        # 歷史模式：顯示開盤後的完整固定視窗
        return get_observation_window(df, ctx.start_datetime)