# src/visualization/figure_utils.py (v2, 擴充版)

import plotly.graph_objects as go
from datetime import datetime

# 3. Local Application Imports
from config.config import TAIWAN_TZ
from config.types import SessionType
from config.run_context import RunContext
from src.utils.session_time import get_observation_window, get_sliding_window

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
# 2. 共用圖表物件
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

# Dash App 中圖表 (K線圖、主圖) 的共用版面設定
COMMON_LAYOUT_SETTINGS = dict(
    template='plotly_dark',             # 套用暗黑主題
    hovermode='x unified',              # X軸統一 hover 效果
    xaxis_rangeslider_visible=False,    # 關閉 X 軸的範圍滑桿
)

# 共用的 X 軸樣式 (適用所有子圖)
COMMON_XAXIS_SETTINGS = dict(
    showspikes=True,
    spikemode='across',
    spikesnap='cursor',
    showline=True,
    spikethickness=1,
    showticklabels=True,
    showgrid=True,
)

# 共用的 Y 軸樣式 (價格)
PRICE_YAXIS_SETTINGS = dict(
    title_text="Price", 
    tickformat=".0f", 
    showgrid=True, 
)

# 共用的 Y 軸樣式 (成交量)
VOLUME_YAXIS_SETTINGS = dict(
    title_text="Volume",
    showgrid=True, 
)

# ------------------------------------------------------------
# 4. 共用輔助函式
# ------------------------------------------------------------

def get_time_range(ctx: RunContext) -> tuple[datetime, datetime]:
    """
    根據即時或歷史模式，取得正確的圖表 X 軸時間範圍。
    """
    if ctx.real_time_mode:
        return get_sliding_window(ctx.start_datetime, ctx.end_datetime, TAIWAN_TZ)
    else:
        return get_observation_window(ctx.start_datetime, ctx.end_datetime, TAIWAN_TZ)