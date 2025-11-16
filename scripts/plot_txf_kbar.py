# scripts/plot_txf_kbar.py

# Standard Library Imports
from pathlib import Path

# Third-Party Imports
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

# Local Application Imports
from config.config import OUTPUT_DIR

# ------------------------------------------------------------
# 📦 1. 資料載入與前處理
# ------------------------------------------------------------
def load_and_preprocess(csv_path: str) -> pd.DataFrame:
    """
    載入 CSV，並處理日夜盤時間戳，計算 VWAP。
    """
    df = pd.read_csv(csv_path)

    session_open_time = {
        'night': '15:00:00',
        'day': '08:45:00'
    }

    # --- 根據日夜盤給予正確的開盤時間 ---
    df['end_time'] = df.apply(
        lambda row: pd.to_datetime(f"{row['date']} {session_open_time[row['session']]}"),
        axis=1
    )

    df['display_time'] = df['date'].astype(str) + ' ' + df['session']
    df = df.sort_values('end_time').reset_index(drop=True)
    
    # --- 建立 X 軸索引 (用於 Plotly) ---
    df['x_index'] = df.index

    # --- 計算 VWAP (用於 RVWAP) ---
    df['vwap'] = (df['high'] + df['low'] + df['close']) / 3

    return df

# ------------------------------------------------------------
# 📦 2. 繪製 K 線圖 (含均線與成交量)
# ------------------------------------------------------------
def plot_candlestick_with_volume(df: pd.DataFrame, html_output_path=f"{OUTPUT_DIR}/TXF-Daily-Chart.html"):
    """
    繪製包含日/夜盤 K 線、多組 SMA/RVWAP 均線及成交量的 Plotly 圖表。
    """
    if df.empty:
        print("無資料可畫圖")
        return

    bar_width = max(0.3, 0.3 * (df['x_index'].diff().median() or 1))

    # --- (A) 計算 SMA 與 RVWAP 指標 ---
    ma_days   = [5, 10, 20, 30, 60, 120, 200]
    ma_colors = [
        "rgba(255, 255, 0, 1.0)",   # 5  → 金黃
        "rgba(0, 255, 255, 1.0)",   # 10 → 青藍
        "rgba(255, 0, 255, 1.0)",   # 20 → 品紅
        "rgba(255, 140, 0, 1.0)",   # 30 → 橘色
        "rgba(50, 205, 50, 1.0)",   # 60 → 青綠
        "rgba(173, 216, 230, 1.0)", # 120 → 淡藍
        "rgba(255, 255, 255, 1.0)"  # 200 → 白色
    ]

    for p in ma_days:
        # (注意：日夜盤合併計算，window * 2)
        df[f"SMA{p}"] = df['close'].rolling(window=2 * p, min_periods=1).mean()
        df[f"RVWAP{p}"] = (
            (df['vwap'] * df['volume']).rolling(window=2 * p, min_periods=1).sum()
            / df['volume'].rolling(window=2 * p, min_periods=1).sum()
        )

    # --- (B) 建立子圖表 (Subplots) ---
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.7, 0.3],
        subplot_titles=['TXF Candlestick', 'Volume']
    )

    # --- (C) 繪製 K 線 (日盤) ---
    df_day = df[df['session'] == 'day']
    fig.add_trace(go.Candlestick(
        x=df_day['x_index'],
        open=df_day['open'],
        high=df_day['high'],
        low=df_day['low'],
        close=df_day['close'],
        name='Day Price',
        increasing_line_color='rgba(0, 255, 0, 1)',
        decreasing_line_color='rgba(255, 0, 0, 1)',
        hovertext=df_day['display_time'],
        hoverinfo='text+y'
    ), row=1, col=1)

    # --- (D) 繪製 K 線 (夜盤, 半透明) ---
    df_night = df[df['session'] == 'night']
    fig.add_trace(go.Candlestick(
        x=df_night['x_index'],
        open=df_night['open'],
        high=df_night['high'],
        low=df_night['low'],
        close=df_night['close'],
        name='Night Price',
        increasing_line_color='rgba(0, 127, 0, 1)',
        decreasing_line_color='rgba(127, 0, 0, 1)',
        hovertext=df_night['display_time'],
        hoverinfo='text+y'
    ), row=1, col=1)

    # --- (E) 繪製 SMA 均線 ---
    for p, c in zip(ma_days, ma_colors):
        fig.add_trace(go.Scatter(
            x=df['x_index'],
            y=df[f'SMA{p}'],
            mode='lines',
            line=dict(color=c, width=1),
            name=f"SMA{p}"
        ), row=1, col=1)

    # --- (F) 繪製 RVWAP 均線 (預設隱藏) ---
    for p, c in zip(ma_days, ma_colors):
        fig.add_trace(go.Scatter(
            x=df['x_index'],
            y=df[f'RVWAP{p}'],
            mode='lines',
            line=dict(color=c, width=1, dash='dot'),
            name=f"RVWAP{p}",
            visible='legendonly'  # 預設隱藏
        ), row=1, col=1)

    # --- (G) 繪製成交量 (日盤) ---
    fig.add_trace(go.Bar(
        x=df_day['x_index'],
        y=df_day['volume'],
        name='Day Volume',
        width=bar_width,
        marker_color='rgba(255, 255, 0, 1.0)',
        opacity=0.8,
        hovertext=df_day['display_time'],
        hoverinfo='text+y'
    ), row=2, col=1)

    # --- (H) 繪製成交量 (夜盤) ---
    fig.add_trace(go.Bar(
        x=df_night['x_index'],
        y=df_night['volume'],
        name='Night Volume',
        width=bar_width,
        marker_color='rgba(127, 127, 0, 1.0)',
        opacity=0.5,
        hovertext=df_night['display_time'],
        hoverinfo='text+y'
    ), row=2, col=1)

    # --- (I) 設定 Layout 與 X/Y 軸 ---
    fig.update_layout(
        title='TXF Daily Candlestick',
        template='plotly_dark',
        hovermode='x unified',
        height=1600,
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.08, xanchor="right", x=1),
    )
    fig.update_xaxes(
        tickmode='array',
        tickvals=df['x_index'],
        ticktext=[''] * len(df), # 隱藏 X 軸標籤 (避免雜亂)
        tickangle=45,
        showspikes=True,
        showgrid=True,
        gridwidth=0.5,
        spikemode='across',
        spikesnap='cursor',
        showline=True
    )
    fig.update_yaxes(
        title_text="Price",
        tickformat=".0f",
        showgrid=True,
        gridwidth=0.5,
        row=1, col=1
    )
    fig.update_yaxes(
        title_text="Volume",
        showgrid=True,
        gridwidth=0.5,
        row=2, col=1
    )

    # --- (J) 產出 HTML 檔案 ---
    html_str = pio.to_html(fig, full_html=True, include_plotlyjs='cdn')
    
    # (手動注入 CSS 確保背景為黑色)
    html_str = html_str.replace(
        "<head>",
        """<head>
        <style>
            body { background-color: black; color: white; }
        </style>"""
    )

    output_path = Path(html_output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html_str, encoding="utf-8")
    print(f"✅ 輸出 HTML: {output_path.resolve()}")


# ------------------------------------------------------------
# 📦 3. 執行腳本
# ------------------------------------------------------------
if __name__ == "__main__":
    df = load_and_preprocess("data/daily_txf.csv")
    plot_candlestick_with_volume(df)

'''
cd Projects/tick-viz
source venv/bin/activate
python -m scripts.generate_daily_csv
python -m scripts.plot_txf_kbar
'''