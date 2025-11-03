# src/visualization/stats_table.py

from dash import html
from datetime import datetime


def compute_stats(df, txf_prev_close):
    """
    計算統計數值，返回 dict
    """
    o, c = df.iloc[0].close, df.iloc[-1].close
    max_high, min_low = df.iloc[-1].high, df.iloc[-1].low

    price_change = int(round(c - o))
    intraday_range = int(round(max_high - min_low))
    open_price = int(round(o))
    intraday_high = int(round(max_high))
    intraday_low = int(round(min_low))
    close_price = int(round(c))
    pct_change = price_change / o * 100
    pct_range = intraday_range / o * 100
    open_gap = int(round(o - txf_prev_close))
    close_change = int(round(c - txf_prev_close))
    pct_close_change = close_change / txf_prev_close * 100

    return {
        "open": open_price,
        "close": close_price,
        "max_high": intraday_high,
        "min_low": intraday_low,
        "price_change": price_change,
        "pct_change": pct_change,
        "intraday_range": intraday_range,
        "pct_range": pct_range,
        "open_gap": open_gap,
        "close_change": close_change,
        "pct_close_change": pct_close_change
    }


def color(val):
    """正負顏色判斷"""
    return "green" if val >= 0 else "red"


def generate_stats_div(stats):
    """生成 Dash 元件版統計表格"""
    
    # 表頭單元格樣式（有底線）
    header_style = {
        "padding": "2px",
        "borderBottom": "1px solid #888",
        "color": "white",
        "fontWeight": "bold",
        "width": f"{100/8:.1f}%"
    }

    # 資料列單元格樣式（無底線）
    cell_style = {
        "padding": "2px",
        "color": "white",
        "width": f"{100/8:.1f}%"
    }

    headers = ["日漲跌", "波幅", "開盤跳空", "開盤",
               "最高", "最低", "最新價", "收盤漲跌"]

    row_values = [
        f"{stats['price_change']:+.0f} ({stats['pct_change']:+.2f}%)",
        f"{stats['intraday_range']:.0f} ({stats['pct_range']:.2f}%)",
        f"{stats['open_gap']:.0f}",
        f"{stats['open']:.0f}",
        f"{stats['max_high']:.0f}",
        f"{stats['min_low']:.0f}",
        f"{stats['close']:.0f}",
        f"{stats['close_change']:+.0f} ({stats['pct_close_change']:+.2f}%)"
    ]

    colors = [
        color(stats['pct_change']),
        "white",
        color(stats['open_gap']),
        "white",
        "white",
        "white",
        "white",
        color(stats['pct_close_change'])
    ]

    row = html.Tr([
        html.Td(value, style={**cell_style, "color": c})
        for value, c in zip(row_values, colors)
    ])

    table = html.Table([
        html.Thead(html.Tr([html.Th(h, style=header_style) for h in headers])),
        html.Tbody([row])
    ], style={
        "width": "100%",
        "borderCollapse": "collapse",
        "textAlign": "center",
        "color": "white",
        "fontFamily": "'monospace', 'Inter', sans-serif"
    })

    container = html.Div([
        html.Div([
            html.Div("TXF即時統計資訊",
                     style={"textAlign": "center", "fontSize": 20, "fontWeight": "bold"}),
            html.Div(f"更新: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                     style={"position": "absolute", "right": 0, "top": 0,
                            "fontSize": 14, "color": "#cccccc"})
        ], style={"position": "relative", "width": "100%", "marginBottom": 20}),
        table
    ], style={
        "margin": 20,
        "padding": 15,
        "border": "1px solid #555",
        "borderRadius": 8,
        "backgroundColor": "#000",
        "boxShadow": "0 2px 4px rgba(255,255,255,0.1)",
        "color": "white",
        "fontFamily": "'Inter','Microsoft JhengHei'"
    })

    return container


def generate_stats_html(stats):
    """輸出 HTML 字串版統計表格（表頭加底線，數字列不加底線）"""
    
    color_style = lambda val: "color: green;" if val >= 0 else "color: red;"
    cols = ''.join(['<col style="width:12.5%;">' for _ in range(8)])

    header_html = ''.join(
        [f'<th style="padding:2px; font-weight:bold; border-bottom:1px solid #888;">{h}</th>'
         for h in ["日漲跌", "波幅", "開盤跳空", "開盤",
                   "最高", "最低", "最新價", "收盤漲跌"]]
    )

    html_content = f"""
    <div style="font-family: 'Inter', sans-serif; margin: 20px; padding: 15px;
                border: 1px solid #555; border-radius: 8px; background-color: #000;
                color: #ffffff; box-shadow: 0 2px 4px rgba(255,255,255,0.1);">
        <div style="position: relative; width: 100%; margin-bottom: 20px;">
            <div style="text-align: center; font-size: 20px; font-weight: bold;">
                TXF即時統計資訊
            </div>
            <div style="position: absolute; right: 0; top: 0; font-size: 14px; color: #cccccc;">
                更新: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            </div>
        </div>
        <table style="width: 100%; border-collapse: collapse; font-family: 'monospace', 'Inter', sans-serif;
                      text-align: center; color: #ffffff;">
            <colgroup>{cols}</colgroup>
            <thead><tr>{header_html}</tr></thead>
            <tbody>
                <tr>
                    <td style="padding:2px; {color_style(stats['pct_change'])}">{stats['price_change']:+.0f} ({stats['pct_change']:+.2f}%)</td>
                    <td style="padding:2px;">{stats['intraday_range']:.0f} ({stats['pct_range']:.2f}%)</td>
                    <td style="padding:2px; {color_style(stats['open_gap'])}">{stats['open_gap']:.0f}</td>
                    <td style="padding:2px;">{stats['open']:.0f}</td>
                    <td style="padding:2px;">{stats['max_high']:.0f}</td>
                    <td style="padding:2px;">{stats['min_low']:.0f}</td>
                    <td style="padding:2px; font-weight:bold; font-size:1.1em;">{stats['close']:.0f}</td>
                    <td style="padding:2px; {color_style(stats['pct_close_change'])}">{stats['close_change']:+.0f} ({stats['pct_close_change']:+.2f}%)</td>
                </tr>
            </tbody>
        </table>
    </div>
    """
    return html_content
