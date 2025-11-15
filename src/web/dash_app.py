# src/web/dash_app.py

# Third-Party Imports
from dash import Dash, dcc, html
from dash.dependencies import Input, Output 
from dash.exceptions import PreventUpdate
import dash

# Local Application Imports
from config.config import UI_REFRESH_INTERVAL_SECONDS
from config.types import SessionType
from src.visualization import candlestick_chart, main_chart, stats_table
from src.visualization.figure_utils import BLANK_BLACK_FIGURE
from src.visualization.report_generator import generate_html_report


# ------------------------------------------------------------
# 📦 Dash App 實例與 Layout
# ------------------------------------------------------------
def create_dash_app(shared_state):
    """建立 Dash 應用（共享狀態由主程式傳入）"""
    ctx = shared_state.context
    
    app = Dash(__name__, title=ctx.report_title, prevent_initial_callbacks=True)

    # --- 1. 定義 App Layout ---
    app.layout = html.Div([
        
        # --- (A) 浮動控制列 (Wrapper) ---
        html.Div(
            [
                # --- (A.1) 左上角報告生成按鈕 ---
                html.Button(
                    "⬜️ 點擊生成報告",
                    id="generate-report-btn",
                    n_clicks=0,
                    style={
                        "backgroundColor": "#333",
                        "color": "white",
                        "border": "1px solid #666",
                        "borderTopLeftRadius": "8px",
                        "borderBottomLeftRadius": "8px",
                        "borderTopRightRadius": "0px",
                        "borderBottomRightRadius": "0px",
                        "padding": "8px 16px",
                        "cursor": "pointer",
                        "fontSize": "15px",
                        "boxShadow": "0px 2px 6px rgba(0,0,0,0.5)",
                    }
                ),

                # --- (A.2) (新) UI 控制列 ---
                html.Div(
                    [
                        html.Label(
                            "顯示最近（分鐘）：",
                            style={
                                "color": "#ccc", 
                                "marginRight": "15px", 
                                "fontSize": "15px",
                            },
                        ),
                        dcc.Input(
                            id="lookback-minutes-input",
                            type="number",
                            value=shared_state.ui_lookback_minutes, 
                            min=10,
                            max=840,
                            step=1,
                            debounce=True, 
                            style={
                                "width": "45px", 
                                "backgroundColor": "#222",
                                "color": "white",
                                "border": "1px solid #555",
                                "borderRadius": "4px",
                                "padding": "5px",
                                "fontSize": "15px",
                            },
                        ),
                    ],
                    style={ 
                        "display": "flex",
                        "alignItems": "center",
                        "marginLeft": "0px", 
                        "padding": "6px 16px", 
                        "backgroundColor": "#333",
                        "border": "1px solid #666",
                        "borderTopLeftRadius": "0px",
                        "borderBottomLeftRadius": "0px",
                        "borderTopRightRadius": "8px",
                        "borderBottomRightRadius": "8px",
                        "boxShadow": "0px 2px 6px rgba(0,0,0,0.5)",
                    },
                ),
            ],
            style={ # Wrapper 樣式
                "position": "fixed",
                "top": "30px",
                "left": "20px",
                "zIndex": "9999",
                "display": "flex", 
                "alignItems": "center",
            }
        ),

        # --- (B) 統計資料表 (Div) ---
        html.Div(
            id="stats-html", 
            style={
                "color": "white", 
                "paddingTop": "10px"
            }
        ),

        # --- (C) 圖表區 ---
        dcc.Graph(id="main-analysis-chart", figure=BLANK_BLACK_FIGURE),
        dcc.Graph(id="candlestick-chart", figure=BLANK_BLACK_FIGURE),

        # --- (D) 定時器 ---
        dcc.Interval(id="update-interval", interval=UI_REFRESH_INTERVAL_SECONDS*1000),
        dcc.Interval(
            id="reset-button-interval", 
            interval=1000,
            n_intervals=0, 
            disabled=True
        ),
        
        # --- (E) 隱藏的 Div ---
        html.Div(id="hidden-output-div", style={"display": "none"}),

    ], style={
        "margin": 0,
        "padding": 0,
        "backgroundColor": "black",
        "minHeight": "100vh",
        "position": "relative",
    })

    # ------------------------------------------------------------
    # 📦 Callback 1: 定期刷新儀表板
    # ------------------------------------------------------------
    @app.callback(
        [Output("main-analysis-chart", "figure"),
         Output("candlestick-chart", "figure"),
         Output("stats-html", "children")],
        [
            Input("update-interval", "n_intervals"),
            Input("hidden-output-div", "children")
        ]
    )
    def update_dashboard(n_intervals, lookback_trigger_output):
        
        # 判斷哪個 Input 觸發了 callback
        triggered_id = dash.ctx.triggered_id
            
        # --- 0. (高效) 檢查是否處於休市 ---
        if shared_state.context.session_type == SessionType.CLOSED:
            # 如果是定時器觸發，則停止 (休市不更新)
            # 如果是 lookback 觸發，則允許 (休市仍可調整圖表範圍)
            if triggered_id == "update-interval":
                raise PreventUpdate 

        # --- 1. (開盤) 從 shared_state 安全讀取資料 ---
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

        # --- 3. 生成圖表 ---
        fig_main = main_chart.create_tick_analysis_figure(
            plot_df, txf_prev_close, taiex_prev_close, ctx
        )
        fig_candle = candlestick_chart.plot_candlestick(df_kbars, period='1min', ctx=ctx)
        
        # --- 4. 計算統計 ---
        stats = stats_table.compute_stats(latest_df, txf_prev_close)
        stats_div = stats_table.generate_stats_div(stats)

        return fig_main, fig_candle, stats_div

    # ------------------------------------------------------------
    # 📦 (新) Callback 1.5: 更新 Lookback 分鐘數
    # ------------------------------------------------------------
    @app.callback(
        Output("hidden-output-div", "children"), 
        Input("lookback-minutes-input", "value"),
        prevent_initial_call=True
    )
    def update_lookback_minutes(value):
        if value is None:
            print("[Dash] Lookback input received None. Ignoring.")
            raise PreventUpdate

        try:
            lookback_val = int(value)
            if lookback_val <= 0:
                print(f"[Dash] Invalid lookback value (<=0): {lookback_val}. Ignoring.")
                raise PreventUpdate

            with shared_state.lock:
                # (*** 優化 ***) 檢查值是否真的改變了
                if shared_state.ui_lookback_minutes == lookback_val:
                    raise PreventUpdate # 值相同，不觸發更新
                shared_state.ui_lookback_minutes = lookback_val
            
            print(f"[Dash] Set ui_lookback_minutes to: {lookback_val}")
            
            return f"Lookback set to {lookback_val}"

        except (ValueError, TypeError):
            print(f"[Dash] Error converting lookback value: {value}. Ignoring.")
            raise PreventUpdate

    # ------------------------------------------------------------
    # 📦 Callback 2: "生成報告" 按鈕
    # ------------------------------------------------------------
    @app.callback(
        [Output("generate-report-btn", "children"),
         Output("reset-button-interval", "disabled")], 
        [Input("generate-report-btn", "n_clicks")],
        prevent_initial_call=True
    )
    def generate_report(n_clicks):
        if not n_clicks:
            raise PreventUpdate

        with shared_state.lock:
            ctx = shared_state.context
            df = shared_state.latest_df
            txf_prev_close = shared_state.txf_prev_close
            taiex_prev_close = shared_state.taiex_prev_close

        if df is None or df.empty:
            return "⚠️ S資料不足", False
        
        stats_html = stats_table.generate_stats_html(stats_table.compute_stats(df, txf_prev_close))
        generate_html_report(
            df=df,
            stats_html=stats_html,
            ctx=ctx,
            txf_prev_close=txf_prev_close,
            taiex_prev_close=taiex_prev_close
        )

        return "✅ 已生成新報告", False

    # ------------------------------------------------------------
    # 📦 Callback 3: 自動回復 "生成報告" 按鈕文字
    # ------------------------------------------------------------
    @app.callback(
        [Output("generate-report-btn", "children", allow_duplicate=True),
         Output("reset-button-interval", "disabled", allow_duplicate=True)],
        Input("reset-button-interval", "n_intervals")
    )
    def reset_button_text(n):
        return "⬜️ 點擊生成報告", True

    return app