# src/visualization/candlestick_chart.py

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.utils.session_time import get_observation_window, get_sliding_window
import config.config as config
from config.run_context import RunContext

def plot_candlestick_with_volume_delta(df: pd.DataFrame, ctx: RunContext):
    """
    繪製K線圖與下方的買賣盤成交量分析圖 (Volume Delta)。
    - 上方子圖: OHLC K線圖 (Candlestick)。
    - 下方子圖: 主動買盤 (綠色) 與主動賣盤 (紅色) 的成交量長條圖。
    """
    # 1. 數據檢查：確認 DataFrame 不為空
    if df is None or df.empty:
        print("無交易資料，跳過繪圖。")
        return

    # 2. 動態計算 Bar 寬度，以適應時間不均的 Volume Bar
    df = df.sort_values('end_time')
    # 計算 Bar 之間時間間隔的中位數（秒）
    time_deltas = pd.to_datetime(df['end_time']).diff().dt.total_seconds()
    median_interval = time_deltas.median()
    # 將寬度設為間隔的 20%，並轉換為 Plotly 所需的毫秒
    bar_width_sec = median_interval * 0.2 if pd.notna(median_interval) and median_interval > 0 else 10
    bar_width_ms = bar_width_sec * 1000

    # 3. 建立子圖：上方K線圖(70%)，下方成交量圖(30%)
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,          # 共享 X 軸
        vertical_spacing=0.1,      # 子圖間距
        row_heights=[0.75, 0.25],     # 子圖高度比例
        subplot_titles=('Candlestick Chart', 'Volume Delta')
    )

    # 4. 繪製上方的 K 線圖
    fig.add_trace(go.Candlestick(
        x=df['end_time'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='OHLC',
        increasing_line_color='green',
        decreasing_line_color='red'
    ), row=1, col=1)

    # 5. 繪製下方的主動成交量圖
    # 主動買盤 (Aggressor Buy)
    fig.add_trace(go.Bar(
        x=df['end_time'],
        y=df['aggressive_buy_volume'],
        width=bar_width_ms,
        name='Aggressor Buy',
        marker_color='darkgreen',
        opacity=0.4
    ), row=2, col=1)

    # 主動賣盤 (Aggressor Sell)
    fig.add_trace(go.Bar(
        x=df['end_time'],
        y=df['aggressive_sell_volume'],
        width=bar_width_ms,
        name='Aggressor Sell',
        marker_color='darkred',
        opacity=0.4
    ), row=2, col=1)
    
    # # 將下方長條圖改為堆疊模式
    # fig.update_layout(barmode='stack')

    # 6. 設定圖表整體樣式與佈局
    fig.update_layout(
        title_text='Candlestick with Volume Delta',
        template='plotly_dark',     # 暗色主題
        hovermode='x unified',      # 統一的懸停提示
        height=700,
        xaxis_rangeslider_visible=False, # 隱藏下方的範圍滑桿
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), # 圖例置於圖表上方
        xaxis=dict(rangeslider_visible=False),
    )

    st_dt, ed_dt = get_observation_window(ctx.start_datetime, ctx.end_datetime, config.TAIWAN_TZ)
    # st_dt, ed_dt = get_sliding_window(ctx.start_datetime, ctx.end_datetime, config.TAIWAN_TZ)
    # 7. 更新座標軸標題
    fig.update_yaxes(title_text="Price", tickformat=".0f", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    fig.update_xaxes(
        showspikes=True, 
        spikemode='across', 
        spikesnap='cursor', 
        showline=True,
        spikethickness=1,
        # 【關鍵修改】強制顯示所有子圖的 x 軸刻度標籤
        showticklabels=True,
        range=[st_dt, ed_dt],
        autorange=False
    )

    # 8. 顯示圖表
    # fig.show()

    # -----------------------------------
    return fig
    # -----------------------------------