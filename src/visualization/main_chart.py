# src/visualization/main_chart.py


import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from src.utils.session_time import get_observation_window, get_sliding_window
import config

# 導入相依的處理函式
# 假設您已將 _prepare_plot_data 移至 metrics.py
from src.processing.metrics import prepare_plot_data 

# ----------------------------------------------------------------------------
# 以下是原 Notebook 中的繪圖輔助函式，直接複製過來
# 為了簡潔，這裡只顯示函式名稱，您需要將完整內容放入
# ----------------------------------------------------------------------------

def _add_price_traces(fig: go.Figure, df: pd.DataFrame):
    """在 fig 的第1列新增價格相關走勢圖。"""
    row, col = 1, 1
    # 疊加其他指標線 (使用原始高密度資料)
    fig.add_trace(go.Scattergl(x=df["datetime"], y=df["underlying_price"], name="[現貨] TAIEX", line=dict(color="blue", width=1), visible=True), row=row, col=col)
    fig.add_trace(go.Scattergl(x=df["datetime"], y=df["rrp_by_taiex"], name="[期貨] 參考價", line=dict(color="gray", width=1), visible=True), row=row, col=col)
    fig.add_trace(go.Scattergl(x=df["datetime"], y=df["close"], name="[期貨] TXF", line=dict(color="white", width=1)), row=row, col=col)
    fig.add_trace(go.Scattergl(x=df["datetime"], y=df["avg_price"], name="VWAP", line=dict(color="orange", dash="solid", width=1)), row=row, col=col)
    fig.add_trace(go.Scattergl(x=df["datetime"], y=df["high"], name="High", line=dict(color="green", dash="dash", width=1)), row=row, col=col)
    fig.add_trace(go.Scattergl(x=df["datetime"], y=df["low"], name="Low", line=dict(color="Red", dash="dash", width=1)), row=row, col=col)
    
    # # 更新 Y 軸設定
    # high, low = max(df.iloc[-1].high, df.iloc[-1].rrp_high), min(df.iloc[-1].low, df.iloc[-1].rrp_low)
    # padding = (high - low) * 0.1
    fig.update_yaxes(title_text="價格", tickformat=".0f", row=row, col=col,) 
    #                  range=[low - padding, high + padding])
    fig.update_xaxes(rangeslider_visible=False, row=row, col=col)


def _add_volume_traces(fig: go.Figure, df: pd.DataFrame):
    """在 fig 的第4列新增買賣盤成交量圖。"""
    row, col = 4, 1
    fig.add_trace(go.Scatter(x=df["datetime"], y=df["bid_side_total_vol"], name="買盤成交總量(口)", line=dict(color="green"), line_shape='hv', fill='tozeroy', fillcolor='rgba(0, 255, 0, 0.4)'), row=row, col=col)
    fig.add_trace(go.Scatter(x=df["datetime"], y=df["ask_side_total_vol"], name="賣盤成交總量(口)", line=dict(color="red"), line_shape='hv', fill='tozeroy', fillcolor='rgba(255, 0, 0, 0.4)'), row=row, col=col)
    fig.update_yaxes(title_text="買賣盤成交量(口)", tickformat=".0f", row=row, col=col, autorange=True)


def _add_premium_traces(fig: go.Figure, df: pd.DataFrame):
    """在 fig 的第2列新增折溢價圖。"""
    row, col = 2, 1
    fig.add_trace(go.Scatter(x=df["datetime"], y=df["fut_premium"], name="[期貨-現貨] 折溢價", line=dict(color="blue", width=1), visible=True), row=row, col=col)
    fig.add_trace(go.Scatter(x=df["datetime"], y=df["fut_to_rrp_premium"], name="[期貨-參考價] 折溢價", line=dict(color="gray", width=1)), row=row, col=col)
    fig.add_trace(go.Scatter(x=df["datetime"], y=df["fut_to_vwap_premium"], name="[期貨-VWAP] 折溢價", line=dict(color="orange", width=1)), row=row, col=col)
    fig.update_yaxes(title_text="折溢價(-/+)", tickformat=".0f", row=row, col=col, autorange=True)


def _add_net_volume_traces(fig: go.Figure, df: pd.DataFrame):
    """在 fig 的第3列新增淨主動成交量圖。"""
    row, col = 3, 1
    fig.add_trace(go.Scatter(x=df["datetime"], y=df["cumu_net_agg_vol"].where(df["cumu_net_agg_vol"] > 0), name="淨主動成交量(多方)", mode="lines", line=dict(color="green"), fill="tozeroy", fillcolor="rgba(0, 128, 0, 0.4)"), row=row, col=col)
    fig.add_trace(go.Scatter(x=df["datetime"], y=df["cumu_net_agg_vol"].where(df["cumu_net_agg_vol"] < 0), name="淨主動成交量(空方)", mode="lines", line=dict(color="red"), fill="tozeroy", fillcolor="rgba(255, 0, 0, 0.4)"), row=row, col=col)
    fig.update_yaxes(title_text="淨主動成交量(口)", tickformat=".0f", row=row, col=col, autorange=True)


def _configure_layout(fig: go.Figure):
    """設定圖表的整體佈局、標題與圖例。"""
    # 1. 更新圖表的整體佈局
    fig.update_layout(
        title=dict(text="價格走勢與淨主動成交量", y=0.95),
        template='plotly_dark',
        height=2000,
        showlegend=True,
        legend=dict(x=0.5, y=1.1, orientation="h", xanchor="center", yanchor="bottom"),
    )


    st_dt, ed_dt = get_observation_window(config.START_DATETIME, config.END_DATETIME, config.TAIWAN_TZ)
    # st_dt, ed_dt = get_sliding_window(config.START_DATETIME, config.END_DATETIME, config.TAIWAN_TZ)
    # 2. 將通用設定一次性應用到所有 X 軸上
    fig.update_xaxes(
        showspikes=True, 
        spikemode='across', 
        spikesnap='cursor', 
        showline=True,
        # 【關鍵修改】強制顯示所有子圖的 x 軸刻度標籤
        showticklabels=True,
        range=[st_dt, ed_dt],
        autorange=False
    )


# ----------------------------------------------------------------------------
# 主要的繪圖函式 (重構後)
# ----------------------------------------------------------------------------

def plot_tick_analysis(df: pd.DataFrame, txf_prev_close: float, taiex_prev_close: float) -> go.Figure:
    """
    視覺化 Tick 資料，整合價格、基差、主動成交量等多維度分析。
    
    Args:
        df (pd.DataFrame): 包含 Tick 資料的 DataFrame。
        txf_prev_close (float): 台指期貨昨日收盤價。
        taiex_prev_close (float): 加權指數昨日收盤價。
        
    Returns:
        go.Figure: 一個包含完整分析圖的 Plotly Figure 物件。
    """
    # 1. 準備繪圖資料及衍生指標
    plot_df = prepare_plot_data(df, txf_prev_close, taiex_prev_close)

    # 2. 建立 4x1 的子圖畫布
    fig = make_subplots(
        rows=4, cols=1,
        shared_xaxes=True,
        row_heights=[0.375, 0.225, 0.2, 0.2],
        vertical_spacing=0.05,
        subplot_titles=("價格走勢", "期貨折溢價", "買賣盤成交總量差", "買賣盤成交總量")
    )

    # 3. 依序繪製各個子圖
    _add_price_traces(fig, plot_df)
    _add_premium_traces(fig, plot_df)
    _add_net_volume_traces(fig, plot_df)
    _add_volume_traces(fig, plot_df)

    # 4. 設定圖表全域樣式
    _configure_layout(fig)

    # 5. 【關鍵修改】回傳 Figure 物件，而不是顯示或儲存它
    return fig