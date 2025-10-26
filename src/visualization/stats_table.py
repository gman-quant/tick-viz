# src/visualization/stats_table.py

from datetime import datetime

def generate_stats_html(df, txf_prev_close):
    """輸出當盤價格統計與波動資訊（含漲跌幅與日內區間），百分比保留兩位小數，其他數字取整數並用中文表示"""
    print("TXF即時統計資訊")
    max_high, min_low = df.iloc[-1].high, df.iloc[-1].low
    o, c = df.iloc[0].close, df.iloc[-1].close
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

    #------------------------------------------------------------------------------------------------------------------
    # 根據正負判斷顏色
    change_color_style = "color: green;" if pct_change >= 0 else "color: red;"
    change_color_style2 = "color: green;" if open_gap >= 0 else "color: red;"
    change_color_style3 = "color: green;" if pct_close_change >= 0 else "color: red;"
    
    stats_html_content = f"""
    <div style="font-family: 'Inter', sans-serif; margin: 20px; padding: 15px; border: 1px solid #555; border-radius: 8px; background-color: #000000; color: #ffffff; box-shadow: 0 2px 4px rgba(255,255,255,0.1);">
        <div style="position: relative; width: 100%; color: #ffffff; margin-top: 0; margin-bottom: 20px;">
            <div style="text-align: center; font-size: 20px; font-weight: bold;">
                TXF即時統計資訊
            </div>
            <div style="position: absolute; right: 0; top: 0; font-size: 14px; color: #cccccc;">
                更新: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            </div>
        </div>
        
        <table style="width: 100%; border-collapse: collapse; font-family: 'monospace', 'Inter', sans-serif; text-align: center; color: #ffffff;">
            <colgroup>
                <col style="width: 12.5%;">
                <col style="width: 12.5%;">
                <col style="width: 12.5%;">
                <col style="width: 12.5%;">
                <col style="width: 12.5%;">
                <col style="width: 12.5%;">
                <col style="width: 12.5%;">
                <col style="width: 12.5%;">
            </colgroup>
            <thead>
                <tr>
                    <th style="padding: 2px; color: #ffffff; font-weight: bold; border-bottom: 1px solid #888;">日漲跌</th>
                    <th style="padding: 2px; color: #ffffff; font-weight: bold; border-bottom: 1px solid #888;">波幅</th>
                    <th style="padding: 2px; color: #ffffff; font-weight: bold; border-bottom: 1px solid #888;">開盤跳空</th>
                    <th style="padding: 2px; color: #ffffff; font-weight: bold; border-bottom: 1px solid #888;">開盤</th>
                    <th style="padding: 2px; color: #ffffff; font-weight: bold; border-bottom: 1px solid #888;">最高</th>
                    <th style="padding: 2px; color: #ffffff; font-weight: bold; border-bottom: 1px solid #888;">最低</th>
                    <th style="padding: 2px; color: #ffffff; font-weight: bold; border-bottom: 1px solid #888;">最新價</th>
                    <th style="padding: 2px; color: #ffffff; font-weight: bold; border-bottom: 1px solid #888;">收盤漲跌</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td style="padding: 2px; {change_color_style}">{price_change:+.0f} ({pct_change:+.2f}%)</td>
                    <td style="padding: 2px;">{intraday_range:.0f} ({pct_range:.2f}%)</td>
                    <td style="padding: 2px; {change_color_style2}">{open_gap:.0f}</td>
                    <td style="padding: 2px;">{open_price:.0f}</td>
                    <td style="padding: 2px;">{intraday_high:.0f}</td>
                    <td style="padding: 2px;">{intraday_low:.0f}</td>
                    <td style="padding: 2px; font-size: 1.1em; font-weight: bold;">{close_price:.0f}</td>
                    <td style="padding: 2px; {change_color_style3}">{close_change:+.0f} ({pct_close_change:+.2f}%)</td>
                </tr>
            </tbody>
        </table>
    </div>
    """

    return stats_html_content