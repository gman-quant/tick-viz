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


def _generate_figures(df, ctx, txf_prev_close, taiex_prev_close):
    """
    生成主分析圖與各 K 線 圖，返回圖表列表
    """
    logging.debug("⚙️ 正在生成 Plotly 圖表...")
    plot_df = prepare_plot_data(df, txf_prev_close, taiex_prev_close)
    
    figures = [
        main_chart.create_tick_analysis_figure(plot_df, txf_prev_close, taiex_prev_close, ctx)
    ]

    for period in ['1min', '3min', '5min', '10min']:
        df_kbars = generate_kbars(df, period=period, ctx=ctx)
        figures.append(candlestick_chart.plot_candlestick(df_kbars, period=period, ctx=ctx)) 

    logging.debug(f"✅ 成功生成 {len(figures)} 張圖表。")
    return figures


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
    
    # === 關鍵修正：在函式內部自己呼叫 _generate_figures ===
    figures = _generate_figures(df, ctx, txf_prev_close, taiex_prev_close)
    
    # 初始化一個空字串，用來存放所有圖表的 HTML
    charts_html_body = ""
    
    # 遍歷所有 (現在已定義的) figure 物件
    for fig in figures:
        if fig:
            # 將每個 figure 轉為 HTML 片段，並附加到 body 字串中
            charts_html_body += pio.to_html(fig, include_plotlyjs='cdn', full_html=False)

    # 用當下時間當作「更新標記」
    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 使用 f-string 組合出最終的完整 HTML 結構
    full_html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="last-updated" content="{last_updated}">
        <title>{ctx.report_title}</title>
        <style>
            /* 您可以在這裡定義一些 CSS 樣式 */
            body {{
                background-color: black;
                color: white;
                font-family: 'Inter', sans-serif, 'Microsoft JhengHei';
            }}
        </style>
    </head>
    <body>
        {stats_html}
        
        {charts_html_body}
    </body>
    </html>
    """
    
    output_path = OUTPUT_DIR / f"{ctx.report_title}.html"
    # 確保輸出目錄存在
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # 將組合好的 HTML 內容寫入指定的檔案
    output_path.write_text(full_html_content, encoding="utf-8")
    
    url = (
        f"http://localhost:8080/{ctx.report_title}.html"
        if ctx.real_time_mode
        else f"file://{output_path.resolve()}"
    )
    logging.info(f"🌐 [Report] 報表網址：{url}\n")