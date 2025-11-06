# src/visualization/candlestick_chart.py (v3, 重構版)

# Standard Library Imports
import logging

# Third-Party Imports
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Local Application Imports
from config.run_context import RunContext
import src.visualization.figure_utils as fig_utils


def plot_candlestick_with_volume_delta(df: pd.DataFrame, ctx: RunContext):
    """
    繪製 K 線圖與下方的買賣盤成交量分析圖 (Volume Delta)。
    """
    if df is None or df.empty:
        logging.warning("Candlestick (Volume Delta): 無交易資料，跳過繪圖。")
        return fig_utils.BLANK_BLACK_FIGURE

    df = df.sort_values('end_time')
    time_deltas = pd.to_datetime(df['end_time']).diff().dt.total_seconds()
    median_interval = time_deltas.median()
    bar_width_ms = (median_interval * 0.2 * 1000) if pd.notna(median_interval) and median_interval > 0 else 10000

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        row_heights=[0.75, 0.25],
        subplot_titles=('Volume-based Bars', 'Volume Delta')
    )

    # 上方 K 線圖 (使用共用顏色)
    fig.add_trace(go.Candlestick(
        x=df['end_time'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='OHLC',
        increasing_line_color=fig_utils.COLOR_INCREASING,
        decreasing_line_color=fig_utils.COLOR_DECREASING,
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

    # 設定圖表整體樣式 (使用共用設定)
    fig.update_layout(
        title_text='Volume-based Bars with Volume Delta',
        height=800,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        **fig_utils.COMMON_LAYOUT_SETTINGS
    )

    # 更新 Y 軸與 X 軸 (使用共用設定)
    fig.update_yaxes(**fig_utils.PRICE_YAXIS_SETTINGS, row=1, col=1)
    fig.update_yaxes(**fig_utils.VOLUME_YAXIS_SETTINGS, row=2, col=1)

    fig.update_xaxes(
        row=2, col=1,
        range=fig_utils.get_time_range(ctx),
        autorange=False,
        **fig_utils.COMMON_XAXIS_SETTINGS
    )

    return fig


def plot_candlestick(df: pd.DataFrame, ctx: RunContext):
    """
    繪製單純的 K 線圖 (Candlestick) 與成交量。
    """
    if df is None or df.empty:
        logging.warning("Candlestick (Time-based): 無交易資料，跳過繪圖。")
        return fig_utils.BLANK_BLACK_FIGURE

    delta_seconds = (df['datetime'].iloc[1] - df['datetime'].iloc[0]).total_seconds()
    delta_minutes = int(delta_seconds // 60)

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        row_heights=[0.65, 0.35],
        subplot_titles=(f'{delta_minutes}-min K-Bars', f'{delta_minutes}-min Volume')
    )

    # 上方 K 線圖 (使用共用顏色)
    fig.add_trace(go.Candlestick(
        x=df['datetime'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='OHLC',
        increasing_line_color=fig_utils.COLOR_INCREASING,
        decreasing_line_color=fig_utils.COLOR_DECREASING,
    ), row=1, col=1)

    # 下方成交量 (使用共用顏色)
    fig.add_trace(go.Bar(
        x=df['datetime'],
        y=df['volume'],
        name='Volume',
        marker_color=fig_utils.COLOR_CANDLE_VOL_DAY,
        opacity=1
    ), row=2, col=1)

    # 設定樣式 (使用共用設定)
    fig.update_layout(
        title_text=f'{delta_minutes}-min K-Bars with Volume',
        height=800,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        **fig_utils.COMMON_LAYOUT_SETTINGS
    )

    # 更新 Y 軸與 X 軸 (使用共用設定)
    fig.update_yaxes(**fig_utils.PRICE_YAXIS_SETTINGS, row=1, col=1)
    fig.update_yaxes(**fig_utils.VOLUME_YAXIS_SETTINGS, row=2, col=1)

    # (使用共用設定)
    fig.update_xaxes(
        range=fig_utils.get_time_range(ctx),
        autorange=False,
        **fig_utils.COMMON_XAXIS_SETTINGS # (修改)
    )

    return fig

