import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
import webbrowser
from pathlib import Path

from config.config import OUTPUT_DIR


def load_and_preprocess(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    session_open_time = {
        'night': '15:00:00',
        'day': '08:45:00'
    }

    df['end_time'] = df.apply(
        lambda row: pd.to_datetime(f"{row['date']} {session_open_time[row['session']]}"),
        axis=1
    )

    df['display_time'] = df['date'].astype(str) + ' ' + df['session']
    df = df.sort_values('end_time').reset_index(drop=True)
    df['x_index'] = df.index

    # ➤ 新增 VWAP 欄位 (必要)
    df['vwap'] = (df['high'] + df['low'] + df['close']) / 3

    return df


def plot_candlestick_with_volume(df: pd.DataFrame, html_output_path=f"{OUTPUT_DIR}/TXF-Daily-Chart.html"):
    if df.empty:
        print("無資料可畫圖")
        return

    bar_width = max(0.3, 0.3 * (df['x_index'].diff().median() or 1))

    # --- ➤ 計算 SMA 與 RVWAP ---
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
        df[f"SMA{p}"] = df['close'].rolling(window=2 * p, min_periods=1).mean()
        df[f"RVWAP{p}"] = (
            (df['vwap'] * df['volume']).rolling(window=2 * p, min_periods=1).sum()
            / df['volume'].rolling(window=2 * p, min_periods=1).sum()
        )

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.7, 0.3],
        subplot_titles=['TXF Candlestick', 'Volume']
    )

    # ➤ 日盤 K 線
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

    # ➤ 夜盤 K 線（調透明）
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

    # ➤ 加入 SMA 線
    for p, c in zip(ma_days, ma_colors):
        fig.add_trace(go.Scatter(
            x=df['x_index'],
            y=df[f'SMA{p}'],
            mode='lines',
            line=dict(color=c, width=1),
            name=f"SMA{p}"
        ), row=1, col=1)

    # ➤ 加入 RVWAP 線（預設隱藏）
    for p, c in zip(ma_days, ma_colors):
        fig.add_trace(go.Scatter(
            x=df['x_index'],
            y=df[f'RVWAP{p}'],
            mode='lines',
            line=dict(color=c, width=1, dash='dot'),
            name=f"RVWAP{p}",
            visible='legendonly'  # 預設隱藏
        ), row=1, col=1)

    # ➤ Volume - 日盤
    fig.add_trace(go.Bar(
        x=df_day['x_index'],
        y=df_day['volume'],
        name='Day Volume',
        width=bar_width,
        marker_color='rgba(255, 255, 0, 1.0)',
        opacity=0.6,
        hovertext=df_day['display_time'],
        hoverinfo='text+y'
    ), row=2, col=1)

    # ➤ Volume - 夜盤
    fig.add_trace(go.Bar(
        x=df_night['x_index'],
        y=df_night['volume'],
        name='Night Volume',
        width=bar_width,
        marker_color='rgba(127, 127, 0, 1.0)',
        opacity=0.4,
        hovertext=df_night['display_time'],
        hoverinfo='text+y'
    ), row=2, col=1)

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
        ticktext=[''] * len(df),
        tickangle=45,
        showspikes=True,
        spikemode='across',
        spikesnap='cursor',
        showline=True
    )

    fig.update_yaxes(
        title_text="Price",
        tickformat=".0f",
        showgrid=True,
        gridwidth=0.5,
        gridcolor='gray',
        row=1, col=1
    )

    fig.update_yaxes(
        title_text="Volume",
        showgrid=True,
        gridwidth=0.5,
        gridcolor='gray',
        row=2, col=1
    )

    # ➤ 產出 HTML 檔
    html_str = pio.to_html(fig, full_html=True, include_plotlyjs='cdn')
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

    # 自動開啟（可依需要取消註解）
    # webbrowser.open(output_path.resolve().as_uri())


# --- 執行 ---
df = load_and_preprocess("data/daily_txf.csv")
plot_candlestick_with_volume(df)

'''
source venv/bin/activate
python -m src.processing.kbar.process_all_ticks_to_daily_csv
python plot_txf_kbar.py
'''
