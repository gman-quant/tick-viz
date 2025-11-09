# src/web/dash_app.py

# Third-Party Imports
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

# Local Application Imports
import config.config as config
from src.visualization import candlestick_chart, main_chart, stats_table
from src.visualization.figure_utils import BLANK_BLACK_FIGURE
from src.visualization.report_generator import generate_html_report


# ------------------------------------------------------------
# 📦 Dash App 實例與 Layout
# ------------------------------------------------------------
def create_dash_app(shared_state):
    """建立 Dash 應用（共享狀態由主程式傳入）"""
    ctx = shared_state.context
    app = Dash(__name__, title=ctx.report_title)

    # --- 1. 定義 App Layout ---
    app.layout = html.Div([
        
        # --- (A) 左上角報告生成按鈕 (浮動) ---
        html.Button(
            "⬜️　點擊生成報告",
            id="generate-report-btn",
            n_clicks=0,
            style={
                "position": "fixed",
                "top": "30px",
                "left": "20px",
                "zIndex": "9999",
                "backgroundColor": "#333",
                "color": "white",
                "border": "1px solid #666",
                "borderRadius": "8px",
                "padding": "8px 16px",
                "cursor": "pointer",
                "fontSize": "15px",
                "boxShadow": "0px 2px 6px rgba(0,0,0,0.5)",
            }
        ),

        # --- (B) 統計資料表 (Div) ---
        html.Div(id="stats-html", style={"color": "white", "paddingTop": "10px"}),

        # --- (C) 圖表區 ---
        dcc.Graph(id="main-analysis-chart"),
        dcc.Graph(id="candlestick-chart"),

        # --- (D) 定時器 (Callbacks 用) ---
        dcc.Interval(id="update-interval", interval=config.UPDATE_INTERVAL * 1000),
        dcc.Interval(id="reset-button-interval", interval=config.UPDATE_INTERVAL * 1000,
                     n_intervals=0, disabled=False)
    ], style={
        "margin": 0,
        "padding": 0,
        "backgroundColor": "black",
        "minHeight": "100vh",
        "position": "relative",
    })

    # ------------------------------------------------------------
    # 📦 Callback 1: 定期刷新儀表板 (主圖表, K線, 統計表)
    # ------------------------------------------------------------
    @app.callback(
        [Output("main-analysis-chart", "figure"),
         Output("candlestick-chart", "figure"),
         Output("stats-html", "children")],
        [Input("update-interval", "n_intervals")]
    )
    def update_dashboard(n):
        # --- 1. 從 shared_state 安全讀取資料 ---
        with shared_state.lock:            
            ctx = shared_state.context
            plot_df = shared_state.plot_df
            df_kbars = shared_state.kbars_1min
            txf_prev_close = shared_state.txf_prev_close
            taiex_prev_close = shared_state.taiex_prev_close
            latest_df = shared_state.latest_df

        # --- 2. 檢查資料是否就緒 ---
        if plot_df is None or plot_df.empty or df_kbars is None or latest_df is None:
            return BLANK_BLACK_FIGURE, BLANK_BLACK_FIGURE, "等待資料中..."

        # --- 3. 生成圖表 (使用預先算好的資料) ---
        fig_main = main_chart.create_tick_analysis_figure(
            plot_df, txf_prev_close, taiex_prev_close, ctx
        )
        fig_candle = candlestick_chart.plot_candlestick(df_kbars, period='1min', ctx=ctx)
        
        # --- 4. 計算統計 (計算量小, 即時計算) ---
        stats = stats_table.compute_stats(latest_df, txf_prev_close)
        stats_div = stats_table.generate_stats_div(stats)

        return fig_main, fig_candle, stats_div

    # ------------------------------------------------------------
    # 📦 Callback 2: "生成報告" 按鈕
    # ------------------------------------------------------------
    @app.callback(
        [Output("generate-report-btn", "children"),
         Output("reset-button-interval", "disabled"),
         Output("reset-button-interval", "n_intervals")],
        [Input("generate-report-btn", "n_clicks")],
        prevent_initial_call=True
    )
    def generate_report(n_clicks):
        if not n_clicks:
            raise PreventUpdate

        # --- 1. 取得共享資料 ---
        with shared_state.lock:
            ctx = shared_state.context
            df = shared_state.latest_df
            txf_prev_close = shared_state.txf_prev_close
            taiex_prev_close = shared_state.taiex_prev_close

        # --- 2. 檢查資料 ---
        if df is None or df.empty:
            return "⚠️　資料不足，無法生成", False, 0
        
        # --- 3. 生成靜態報告 ---
        stats_html = stats_table.generate_stats_html(stats_table.compute_stats(df, txf_prev_close))
        generate_html_report(
            df=df,
            stats_html=stats_html,
            ctx=ctx,
            txf_prev_close=txf_prev_close,
            taiex_prev_close=taiex_prev_close
        )

        # --- 4. 更新按鈕文字 (成功) 並重置計時器 ---
        return "✅　已生成新報告", False, 0

    # ------------------------------------------------------------
    # 📦 Callback 3: 自動回復 "生成報告" 按鈕文字
    # ------------------------------------------------------------
    @app.callback(
        Output("generate-report-btn", "children", allow_duplicate=True),
        Input("reset-button-interval", "n_intervals"),
        prevent_initial_call="initial_duplicate"
    )
    def reset_button_text(n):
        if n == 0:
            raise PreventUpdate
        return "⬜️　點擊生成報告"

    return app