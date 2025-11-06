# src/visualization/report_generator.py (v2, 統一使用 logging)

# Standard Library Imports
import logging
from datetime import datetime
from typing import List

# Third-Party Imports
import plotly.graph_objects as go
import plotly.io as pio

# Local Application Imports
from config.config import OUTPUT_DIR
from config.run_context import RunContext


def generate_html_report(
    figures: List[go.Figure], 
    stats_html: str, 
    ctx: RunContext
):
    """
    將多個 Plotly 圖表和統計數據的 HTML 字串，合併成一個完整的 HTML 報告檔案。

    Args:
        figures (List[go.Figure]): 一個包含多個 Plotly Figure 物件的列表。
        stats_html (str): 包含盤中統計資訊的 HTML 字串。
        ctx (RunContext): 執行時的上下文，用於取得報告標題等。
    """

    output_path = OUTPUT_DIR / f"{ctx.report_title}.html"
    # 確保輸出目錄存在
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 初始化一個空字串，用來存放所有圖表的 HTML
    charts_html_body = ""
    
    # 遍歷所有傳入的 figure 物件
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
    
    # 將組合好的 HTML 內容寫入指定的檔案
    output_path.write_text(full_html_content, encoding="utf-8")
    
    # (修改) 改用 logging.info
    logging.info(f"✅ [Report] 報告已成功生成至: {output_path}")
    http_url = f"http://localhost:8080/{ctx.report_title}.html"
    logging.info(f"🌐 [Report] 報表網址：{http_url}\n")