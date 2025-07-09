# src/visualization/report_generator.py

from datetime import datetime
from pathlib import Path
from typing import List

import plotly.graph_objects as go
import plotly.io as pio

from config import IS_REALTIME_MODE


def generate_html_report(
    figures: List[go.Figure], 
    stats_html: str, 
    output_path: Path, 
    report_title: str,
):
    """
    將多個 Plotly 圖表和統計數據的 HTML 字串，合併成一個完整的 HTML 報告檔案。

    Args:
        figures (List[go.Figure]): 一個包含多個 Plotly Figure 物件的列表。
        stats_html (str): 包含盤中統計資訊的 HTML 字串。
        output_path (Path): 最終 HTML 報告的完整儲存路徑。
        report_title (str): HTML 網頁的標題。
        refresh_interval (int): 網頁自動刷新的秒數。
    """
    # 確保輸出目錄存在
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 初始化一個空字串，用來存放所有圖表的 HTML
    charts_html_body = ""
    
    # 遍歷所有傳入的 figure 物件
    for fig in figures:
        if fig:
            # 將每個 figure 轉為 HTML 片段，並附加到 body 字串中
            # include_plotlyjs='cdn' -> 使用網路上的 JS 函式庫，使檔案變小
            # full_html=False -> 只產生圖表的 <div> 區塊，不產生完整的 <html> 結構
            charts_html_body += pio.to_html(fig, include_plotlyjs='cdn', full_html=False)

    # 用當下時間當作「更新標記」
    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ⬇️ 根據模式決定是否加上 WebSocket 腳本
    if IS_REALTIME_MODE:
        websocket_script = """
        <script>
            const ws = new WebSocket("ws://localhost:8080/ws");
            ws.onmessage = function (event) {
                if (event.data === "reload") {
                    console.log("📢 收到更新通知，重新載入頁面");
                    location.reload();
                }
            };
        </script>
        """
    else:
        websocket_script = ""  # 歷史模式不需要 WebSocket

    # 使用 f-string 組合出最終的完整 HTML 結構
    full_html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="last-updated" content="{last_updated}">
        <title>{report_title}</title>
        <style>
            /* 您可以在這裡定義一些 CSS 樣式 */
            body {{
                background-color: black;
                color: white;
                font-family: 'Inter', sans-serif, 'Microsoft JhengHei';
            }}
        </style>
        {websocket_script}
    </head>
    <body>
        {stats_html}
        
        {charts_html_body}
    </body>
    </html>
    """
    
    # 將組合好的 HTML 內容寫入指定的檔案
    output_path.write_text(full_html_content, encoding="utf-8")
    print(f"✅ 報告已成功生成至: {output_path}")
    http_url = f"http://localhost:8080/{report_title}.html"
    print(f"🌐 報表網址：{http_url}\n")