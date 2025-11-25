# src/visualization/report_generator.py

# Standard Library Imports
import logging
from datetime import datetime

# Third-Party Imports
import plotly.io as pio
import pandas as pd

# Local Application Imports
from config.config import OUTPUT_DIR
from config.run_context import RunContext
from src.processing.metrics import prepare_plot_data
from src.processing.bars.kbars import generate_kbars
from src.visualization import candlestick_chart, main_chart


# ------------------------------------------------------------
# 📦 1. 圖表生成 (輔助函式)
# ------------------------------------------------------------
def _generate_figures(df, ctx, txf_prev_close, taiex_prev_close):
    """
    生成主分析圖與各 K 線圖，返回圖表物件列表 (List[go.Figure])。
    """
    logging.debug("⚙️ 正在生成 Plotly 圖表...")
    
    # --- (A) 建立「靜態報告」專用的 Context ---
    # (建立一個新 Context，並將 real_time_mode 設為 False)
    # (這能確保：get_time_range (in figure_utils) 會使用「完整盤勢」而非「滑動視窗」)
    report_ctx = ctx.as_static_report_context()

    # --- (B) 準備繪圖資料 ---
    plot_df = prepare_plot_data(df, txf_prev_close, taiex_prev_close)
    
    # --- (C) 生成主分析圖 (使用 'report_ctx') ---
    figures = [
        main_chart.create_tick_analysis_figure(plot_df, txf_prev_close, taiex_prev_close, report_ctx)
    ]

    # --- (D) 生成各週期 K 線圖 (使用 'report_ctx') ---
    for period in [1, 3, 5, 10]:
        df_kbars = generate_kbars(df, period=f"{period}min", ctx=report_ctx)
        figures.append(candlestick_chart.plot_candlestick(df_kbars, period=period, ctx=report_ctx)) 

    logging.debug(f"✅ 成功生成 {len(figures)} 張圖表。")
    return figures

# ------------------------------------------------------------
# 📄 靜態 HTML 報告生成
# ------------------------------------------------------------
def generate_html_report(
    df: pd.DataFrame,
    stats_html: str,
    ctx: RunContext,
    txf_prev_close: float,
    taiex_prev_close: float
):
    """
    將多個 Plotly 圖表和統計數據的 HTML 字串，合併成一個完整的 HTML 報告檔案。
    """
    # --- 1. 生成所有圖表物件 ---
    figures = _generate_figures(df, ctx, txf_prev_close, taiex_prev_close)
    
    # --- 2. 將圖表物件轉換為 HTML 字串 ---
    charts_html_body = ""
    for fig in figures:
        if fig:
            # include_plotlyjs='cdn' 確保 HTML 檔案能離線開啟
            # full_html=False 只生成圖表的 <div> 片段
            charts_html_body += pio.to_html(fig, include_plotlyjs='cdn', full_html=False)

    # --- 3. 組合完整的 HTML 頁面 ---
    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    full_html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="last-updated" content="{last_updated}">
        <title>{ctx.report_title}</title>
        <style>
            body {{
                background-color: black;
                color: white;
                font-family: 'Inter', sans-serif, 'Microsoft JhengHei';
            }}
        </style>
    </head>
    <body>
        <!-- 插入統計表格 HTML -->
        {stats_html}
        
        <!-- 插入所有圖表 HTML -->
        {charts_html_body}
    </body>
    </html>
    """
    
    # --- 4. 寫入檔案 ---
    output_path = OUTPUT_DIR / f"{ctx.report_title}.html"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(full_html_content, encoding="utf-8")
    
    # --- 5. 輸出日誌 ---
    url = (
        f"http://localhost:8080/{ctx.report_title}.html"
        if ctx.real_time_mode
        else f"file://{output_path.resolve()}"
    )
    logging.info(f"🌐 [Report] 報表網址：{url}\n")

