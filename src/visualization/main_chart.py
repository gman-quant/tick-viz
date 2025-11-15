# src/visualization/main_chart.py

# Standard Library Imports
import logging

# Third-Party Imports
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Local Application Imports
from config.run_context import RunContext
import src.visualization.figure_utils as fig_utils


# ------------------------------------------------------------
# 📦 (Subplot 1) 價格相關走勢圖
# ------------------------------------------------------------
def _add_price_traces(fig: go.Figure, df: pd.DataFrame):
    """在 fig 的第1列新增價格相關走勢圖。"""
    row, col = 1, 1
    
    fig.add_trace(go.Scattergl(x=df["datetime"], y=df["close"], name="[期貨] TXF", line=dict(color="rgba(255, 255, 255, 0.5)", width=1)), row=row, col=col)
    fig.add_trace(go.Scattergl(x=df["datetime"], y=df["rvwap"], name="RVWAP", line=dict(color="yellow", dash="solid", width=1)), row=row, col=col)
    fig.add_trace(go.Scattergl(x=df["datetime"], y=df["avg_price"], name="VWAP", line=dict(color="orange", dash="solid", width=1)), row=row, col=col)
    fig.add_trace(go.Scattergl(x=df["datetime"], y=df["rrp_rhigh"], name="[期貨] 參考價", line=dict(color='rgba(144, 238, 144, 0.3)', width=1), visible=True), row=row, col=col)
    fig.add_trace(go.Scattergl(x=df["datetime"], y=df["rrp_rlow"], name="[期貨] 參考價", line=dict(color='rgba(255, 192, 203, 0.3)', width=1), visible=True), row=row, col=col)
    fig.add_trace(go.Scattergl(x=df["datetime"], y=df["high"], name="High", line=dict(color=fig_utils.COLOR_INCREASING, dash="dash", width=1)), row=row, col=col)
    fig.add_trace(go.Scattergl(x=df["datetime"], y=df["low"], name="Low", line=dict(color=fig_utils.COLOR_DECREASING, dash="dash", width=1)), row=row, col=col)

    # --- 套用共用 Y 軸設定 ---
    fig.update_yaxes(**fig_utils.PRICE_YAXIS_SETTINGS, row=row, col=col)
    fig.update_xaxes(rangeslider_visible=False, row=row, col=col)

# ------------------------------------------------------------
# 📦 (Subplot 2) 淨成交強度 (動能)
# ------------------------------------------------------------
def _add_volume_change_traces(fig: go.Figure, df: pd.DataFrame):
    """在 fig 的第2列新增成交量變化圖（Bid 與 Ask 分開繪製）。"""
    row, col = 2, 1

    # --- 繪製多方強度 (正值) ---
    fig.add_trace(go.Scatter(
        x=df["datetime"],
        y=df["net_agg_vol_change"].where(df["net_agg_vol_change"] > 0),
        name="淨成交強度指標",
        mode="lines",
        line=dict(color=fig_utils.COLOR_INCREASING, width=1),
        line_shape="hv",
        fill="tozeroy",
        fillcolor="rgba(0, 128, 0, 0.2)"
    ), row=row, col=col)

    # --- 繪製空方強度 (負值) ---
    fig.add_trace(go.Scatter(
        x=df["datetime"],
        y=df["net_agg_vol_change"].where(df["net_agg_vol_change"] < 0), 
        name="淨成交強度指標",
        mode="lines",
        line=dict(color=fig_utils.COLOR_DECREASING, width=1),
        line_shape="hv",
        fill="tozeroy",
        fillcolor="rgba(255, 0, 0, 0.2)"
    ), row=row, col=col)

    # --- Y 軸設定 ---
    fig.update_yaxes(
        title_text="淨成交強度",
        tickformat=".2f",
        row=row,
        col=col,
        showgrid=True,
    )

# ------------------------------------------------------------
# 📦 (已停用) 折溢價圖
# ------------------------------------------------------------
# def _add_premium_traces(fig: go.Figure, df: pd.DataFrame):
#     """在 fig 的第3列新增折溢價圖。(此函式中圖表較特殊，暫不套用共用Y軸)"""
#     row, col = 3, 1
#     
#     fig.add_trace(go.Scatter(x=df["datetime"], y=df["close-rvwap"], name="Close - RVWAP", line=dict(color="gray", width=1)), row=row, col=col, secondary_y=False)
#     fig.add_trace(go.Scatter(x=df["datetime"], y=df["rvwap_to_vwap_premium"], name="RVWAP - VWAP", line=dict(color="yellow", width=1.5)), row=row, col=col, secondary_y=True)
#     
#     # 左軸
#     fig.update_yaxes(
#         title_text="折溢價 (Close - RVWAP)",
#         tickformat=".0f",
#         autorange=True,
#         matches=None,
#         nticks=10,
#         row=row, col=col,
#         secondary_y=False,
#         showgrid=True,
#     )
#     # 右軸
#     fig.update_yaxes(
#         title_text="折溢價 (RVWAP - VWAP)",
#         tickformat=".0f",
#         showgrid=True,
#         autorange=True,
#         zeroline=False,
#         matches=None,
#         nticks=10,
#         ticks="outside",
#         row=row, col=col,
#         secondary_y=True
#     )

# ------------------------------------------------------------
# 📦 (Subplot 3) 淨主動成交量 (累計)
# ------------------------------------------------------------
def _add_net_volume_traces(fig: go.Figure, df: pd.DataFrame):
    """在 fig 的第3列(原第4列)新增淨主動成交量圖。"""
    row, col = 3, 1
    
    # --- 右軸 (累計總成交量) ---
    fig.add_trace(go.Scatter(
        x=df["datetime"], 
        y=df["total_volume"], 
        name="累計成交量", 
        mode="lines", 
        line=dict(color="rgba(255, 255, 0, 0.4)"), 
        line_shape="hv"),
        row=row, col=col,
        secondary_y=True 
    )
    # --- 左軸 (淨主動成交 - 多方) ---
    fig.add_trace(go.Scatter(
        x=df["datetime"], 
        y=df["cumu_net_agg_vol"].where(df["cumu_net_agg_vol"] > 0), 
        name="淨主動成交量(多方)", mode="lines", 
        line=dict(color=fig_utils.COLOR_INCREASING),
        line_shape="hv",
        fill="tozeroy", 
        fillcolor="rgba(0, 128, 0, 0.4)"), 
        row=row, col=col,
        secondary_y=False
    )
    # --- 左軸 (淨主動成交 - 空方) ---
    fig.add_trace(go.Scatter(
        x=df["datetime"], 
        y=df["cumu_net_agg_vol"].where(df["cumu_net_agg_vol"] < 0), 
        name="淨主動成交量(空方)", 
        mode="lines", 
        line=dict(color=fig_utils.COLOR_DECREASING),
        line_shape="hv",
        fill="tozeroy", 
        fillcolor="rgba(255, 0, 0, 0.4)"), 
        row=row, col=col,
        secondary_y=False
    )
    
    # --- Y 軸設定 (雙軸) ---
    fig.update_yaxes(
        title_text="淨主動成交量", 
        tickformat=".0f", 
        row=row, col=col, 
        autorange=True, 
        secondary_y=False,
        showgrid=True,
    )
    fig.update_yaxes(
        title_text="累計成交量", 
        tickformat=".0f",
        autorange=True,
        showgrid=False,
        zeroline=False,
        ticks="outside",
        row=row, col=col,
        secondary_y=True
    )

# ------------------------------------------------------------
# 📦 (Layout) 全局版面設定
# ------------------------------------------------------------
def _configure_layout(fig: go.Figure, df: pd.DataFrame, ctx: RunContext):
    """設定圖表的整體佈局、標題與圖例。"""
    
    # --- 套用共用設定 (Layout) ---
    fig.update_layout(
        title=dict(text="TXF技術分析圖表", y=0.95),
        height=1600,
        showlegend=True,
        **fig_utils.COMMON_LAYOUT_SETTINGS,
        legend=dict(x=0.5, y=1.03, orientation="h", xanchor="center", yanchor="bottom"),
    )

    # --- 套用共用設定 (X 軸) ---
    fig.update_xaxes(
        range=fig_utils.get_time_range(df, ctx),
        autorange=False if ctx.real_time_mode else True,
        **fig_utils.COMMON_XAXIS_SETTINGS
    )


# ------------------------------------------------------------
# 📊 主要繪圖函式 (Main Chart)
# ------------------------------------------------------------
def create_tick_analysis_figure(plot_df: pd.DataFrame, txf_prev_close: float, taiex_prev_close: float, ctx: RunContext) -> go.Figure:
    """
    視覺化 Tick 資料，整合價格、基差、主動成交量等多維度分析。
    """
    
    # --- 0. 處理空資料 ---
    if plot_df is None or plot_df.empty:
        logging.warning("Main Chart: 無交易資料，跳過繪圖。")
        return fig_utils.BLANK_BLACK_FIGURE

    # --- 1. 建立子圖畫布 (3x1) ---
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        row_heights=[0.5, 0.25, 0.25], # 價格(50%), 動能(25%), 累計(25%)
        vertical_spacing=0.05,
        specs=[[{}], [{}], [{"secondary_y": True}]], # 第3列使用雙Y軸
        subplot_titles=("逐筆成交價", "淨成交強度指標", "成交量")
    )

    # --- 2. 依序繪製各個子圖 ---
    _add_price_traces(fig, plot_df)
    _add_volume_change_traces(fig, plot_df)
    _add_net_volume_traces(fig, plot_df)

    # --- 3. 設定圖表全域樣式 ---
    _configure_layout(fig, plot_df, ctx)

    # --- 4. 回傳 Figure 物件 ---
    return fig