# src/visualization/candlestick_chart.py

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.utils.session_time import get_observation_window, get_sliding_window
import config.config as config
from config.run_context import RunContext


def plot_candlestick_with_volume_delta(df: pd.DataFrame, ctx: RunContext):
    """
    繪製 K 線圖與下方的買賣盤成交量分析圖 (Volume Delta)。
    - 上方子圖: OHLC K線圖 (Candlestick)
    - 下方子圖: 主動買盤(綠色)與主動賣盤(紅色)的成交量長條圖
    """
    if df is None or df.empty:
        print("無交易資料，跳過繪圖。")
        return

    # 動態計算 Bar 寬度 (適應時間不均)
    df = df.sort_values('end_time')
    time_deltas = pd.to_datetime(df['end_time']).diff().dt.total_seconds()
    median_interval = time_deltas.median()
    bar_width_ms = (median_interval * 0.2 * 1000) if pd.notna(median_interval) and median_interval > 0 else 10000

    # 建立子圖
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        row_heights=[0.75, 0.25],
        subplot_titles=('Volume-based Bars', 'Volume Delta')
    )

    # 上方 K 線圖
    fig.add_trace(go.Candlestick(
        x=df['end_time'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='OHLC',
        increasing_line_color='green',
        decreasing_line_color='red',
    ), row=1, col=1)

    # 下方 Volume Delta
    fig.add_trace(go.Bar(
        x=df['end_time'],
        y=df['aggressive_buy_volume'],
        width=bar_width_ms,
        name='Aggressor Buy',
        marker_color='darkgreen',
        opacity=0.4
    ), row=2, col=1)
    fig.add_trace(go.Bar(
        x=df['end_time'],
        y=df['aggressive_sell_volume'],
        width=bar_width_ms,
        name='Aggressor Sell',
        marker_color='darkred',
        opacity=0.4
    ), row=2, col=1)

    # 設定圖表整體樣式
    fig.update_layout(
        title_text='Volume-based Bars with Volume Delta',
        template='plotly_dark',
        hovermode='x unified',
        height=800,
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    # 計算觀察時間區間
    if ctx.real_time_mode and ctx.auto_refresh:
        st_dt, ed_dt = get_sliding_window(ctx.start_datetime, ctx.end_datetime, config.TAIWAN_TZ)
    else:
        st_dt, ed_dt = get_observation_window(ctx.start_datetime, ctx.end_datetime, config.TAIWAN_TZ)

    # 更新 Y 軸與 X 軸
    fig.update_yaxes(title_text="Price", tickformat=".0f", row=1, col=1, showgrid=True, gridcolor='gray', gridwidth=0.5)
    fig.update_yaxes(title_text="Volume", row=2, col=1, showgrid=True, gridcolor='gray', gridwidth=0.5)

    # 只更新底部子圖 X 軸
    fig.update_xaxes(
        row=2, col=1,
        showspikes=True,
        spikemode='across',
        spikesnap='cursor',
        showline=True,
        spikethickness=1,
        showticklabels=True,
        range=[st_dt, ed_dt],
        autorange=False,
        showgrid=True,
        gridcolor='gray',
        gridwidth=0.5
    )

    return fig


def plot_candlestick(df: pd.DataFrame, ctx: RunContext):
    """
    繪製單純的 K 線圖 (Candlestick) 與成交量。
    """
    if df is None or df.empty:
        print("無交易資料，跳過繪圖。")
        return

    # 取第一筆與第二筆時間差
    delta_seconds = (df['datetime'].iloc[1] - df['datetime'].iloc[0]).total_seconds()

    # 轉換成分鐘
    delta_minutes = int(delta_seconds // 60)

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        row_heights=[0.65, 0.35],
        subplot_titles=(f'{delta_minutes}-min K-Bars', f'{delta_minutes}-min Volume')
    )

    # 上方 K 線圖
    fig.add_trace(go.Candlestick(
        x=df['datetime'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='OHLC',
        increasing_line_color='green',
        decreasing_line_color='red',
    ), row=1, col=1)

    # 下方成交量
    fig.add_trace(go.Bar(
        x=df['datetime'],
        y=df['volume'],
        name='Volume',
        marker_color='yellow',
        opacity=1
    ), row=2, col=1)

    # 設定樣式
    fig.update_layout(
        title_text=f'{delta_minutes}-min K-Bars with Volume',
        template='plotly_dark',
        hovermode='x unified',
        height=800,
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    if ctx.real_time_mode and ctx.auto_refresh:
        st_dt, ed_dt = get_sliding_window(ctx.start_datetime, ctx.end_datetime, config.TAIWAN_TZ)
    else:
        st_dt, ed_dt = get_observation_window(ctx.start_datetime, ctx.end_datetime, config.TAIWAN_TZ)

    # 更新 Y 軸與 X 軸
    fig.update_yaxes(title_text="Price", tickformat=".0f", row=1, col=1, showgrid=True, gridcolor='gray', gridwidth=0.5)
    fig.update_yaxes(title_text="Volume", row=2, col=1, showgrid=True, gridcolor='gray', gridwidth=0.5)

    fig.update_xaxes(
        showspikes=True,
        spikemode='across',
        spikesnap='cursor',
        showline=True,
        spikethickness=1,
        showticklabels=True,
        range=[st_dt, ed_dt],
        autorange=False,
        showgrid=True,
        gridcolor='gray',
        gridwidth=0.5
    )

    return fig
