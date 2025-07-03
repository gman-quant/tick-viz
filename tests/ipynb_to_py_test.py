# %%
# Cell 1: 環境設定與參數 (Setup and Configuration)


# ==== 系統與標準函式庫 ====
import os
import time
from datetime import date, datetime, timedelta, timezone, time as dt_time
from dotenv import load_dotenv
from pathlib import Path

# ==== 時區與日期解析 ====
from zoneinfo import ZoneInfo
from dateutil import parser

# ==== 資料處理 ====
import pandas as pd
import orjson

# ==== Kafka 消費者 ====
from confluent_kafka import Consumer, TopicPartition, KafkaError, KafkaException

# ==== 圖表繪製 ====
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

# ==== 永豐期貨 Shioaji API ====
import shioaji as sj

# ==== Kafka 參數設定 ====
KAFKA_BROKER = '192.168.1.50:9092'
TOPIC = 'tick-txf'
GROUP_ID = 'tick-consumer-group'

# ==== 時區與時間區間設定 ====
TAIWAN_TZ = ZoneInfo("Asia/Taipei")
start_datetime     = datetime(2025, 7, 2,  8, 30, 0, 0, tzinfo=TAIWAN_TZ)
fixed_end_datetime = datetime(2025, 7, 2, 13, 45, 0, 0, tzinfo=TAIWAN_TZ) # datetime.now(tz=TAIWAN_TZ)
# start_datetime       = datetime(2025, 7, 1, 14, 50, 0, 0, tzinfo=TAIWAN_TZ)
# fixed_end_datetime   = datetime(2025, 7, 2,  5,  0, 0, 0, tzinfo=TAIWAN_TZ) # datetime.now(tz=TAIWAN_TZ)

# %%
# Cell 2: 核心函式定義 (Function Definitions)


# ==== 工具函數 ====
def parse_tick_datetime(raw_dt: str) -> datetime:
    """
    將原始 tick 的字串 datetime 轉為帶時區的 datetime 物件（Asia/Taipei）
    
    Args:
        raw_dt: 字串格式的 datetime，例如 '2025-06-13T08:30:00.123456'
    
    Returns:
        datetime 物件（附帶 Asia/Taipei 時區資訊）
    """
    try:
        dt = parser.isoparse(raw_dt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TAIWAN_TZ)
        else:
            dt = dt.astimezone(TAIWAN_TZ)
        return dt
    except Exception as e:
        print(f"⚠️ 無法解析 datetime: {raw_dt}，錯誤: {e}")
        return None


def fetch_ticks(consumer, offsets, start_datetime, end_datetime, tick_dict):
    """
    從 Kafka 擷取指定時間區間內的 tick 資料，並合併更新到傳入的 tick_dict 中。
    以 tick_dict 的 key（以 datetime 字串作為唯一鍵）避免重複資料，
    最後回傳完整的 DataFrame 以及最新的 offset 列表供下一次使用。

    Args:
        consumer: Kafka Consumer 實例，用於從 Kafka 拉取消息。
        offsets: list，指定 Kafka topic partition 的起始 offsets（由 offsets_for_times 查得）。
        start_datetime: datetime，資料擷取的起始時間（已轉為 Asia/Taipei 時區）。
        end_datetime: datetime，資料擷取的結束時間（已轉為 Asia/Taipei 時區）。
        tick_dict: dict，持續累積的 tick 資料容器，key 為 datetime 字串，value 為 tick 記錄 dict。

    Returns:
        df: pandas.DataFrame，包含目前 tick_dict 所有 tick 的完整 DataFrame（無重複）。
        new_offsets: list，更新後的 offsets，用於下一輪資料拉取。
    """
    consumer.assign(offsets)

    print(f"🔄 從 {start_datetime} (Asia/Taipei) 開始讀取資料...")

    try:
        while True:
            try:
                msg = consumer.poll(1.0)
            except KafkaException as e:
                print(f"⚠️ Kafka polling error: {e}")
                break
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                else:
                    print("⚠️ Kafka 錯誤：", msg.error())
                    continue

            try:
                record = orjson.loads(msg.value())
            except Exception as e:
                print("⚠️ JSON decode error:", e)
                continue

            tick_dt_taiwan = parse_tick_datetime(record['datetime'])
            if tick_dt_taiwan is None:
                continue  # 跳過無效時間

            if tick_dt_taiwan > end_datetime:
                print("⏹ 已達目標時間結束")
                break

            if start_datetime <= tick_dt_taiwan <= end_datetime and not record['simtrade']:
                # 以 datetime 為 key，更新或新增 tick 紀錄
                tick_dict[tick_dt_taiwan] = record

    except KeyboardInterrupt:
        print("🛑 使用者中止")

    # 將累積的 tick_dict 轉為 DataFrame
    df = pd.DataFrame(tick_dict.values())


    # # ================== DEBUG: 捕獲有問題的資料 ==================
    # print("\n--- 準備進行時間轉換，以下是原始的 datetime 欄位內容 ---")
    # # 使用 .to_string() 確保印出所有資料，而不是被...省略
    # print(df['datetime'].to_string())
    # print("--- 原始資料結束 ---\n")
    # # ==========================================================


    df['datetime'] = pd.to_datetime(df['datetime'], format='ISO8601')
    df.sort_values(by='datetime', inplace=True)

    print(f"\n✅ 共取得 {len(df)} 筆資料（{start_datetime} ~ {end_datetime}）")

    # 取得最新 consumer 位置 offsets，準備下一輪拉取用
    positions = consumer.position(offsets)
    
    new_offsets = []
    for pos in positions:
        if pos.offset >= 0:
            new_offsets.append(pos)
        else:
            # 如果 offset 無效，維持原本 offsets
            original_tp = next((tp for tp in offsets if tp.topic == pos.topic and tp.partition == pos.partition), None)
            if original_tp:
                new_offsets.append(original_tp)
        
    return df, new_offsets


def print_intraday_stats(df):
    """輸出當盤價格統計與波動資訊（含漲跌幅與日內區間），百分比保留兩位小數，其他數字取整數並用中文表示"""
    print("當前價格統計與波動資訊")

    max_high, min_low = df.iloc[-1].high, df.iloc[-1].low
    o, c = df.iloc[-1].open, df.iloc[-1].close

    price_change = int(round(c - o))
    intraday_range = int(round(max_high - min_low))
    open_price = int(round(o))
    intraday_high = int(round(max_high))
    intraday_low = int(round(min_low))
    close_price = int(round(c))
    pct_change = price_change / o * 100
    pct_range = intraday_range / o * 100

    # w = 17  # 整數，表示寬度
    # print(f"{'Change (Pts | %):':>{w}} {price_change:>5} | {pct_change:>5.2f}%")
    # print(f"{'Range (Pts | %):':>{w}} {intraday_range:>5} | {pct_range:>5.2f}%")
    # print(f"{'Open:':>{w}} {open_price}")
    # print(f"{'High:':>{w}} {intraday_high}")
    # print(f"{'Low:':>{w}} {intraday_low}")
    # print(f"{'Close:':>{w}} {close_price}")

    #------------------------------------------------------------------------------------------------------------------
    # 根據正負判斷顏色
    change_color_style = "color: green;" if pct_change >= 0 else "color: red;"
    
    stats_html_content = f"""
    <div style="font-family: 'Inter', sans-serif; margin: 20px; padding: 15px; border: 1px solid #555; border-radius: 8px; background-color: #000000; color: #ffffff; box-shadow: 0 2px 4px rgba(255,255,255,0.1);">
        <h3 style="margin-top: 0; color: #ffffff; text-align: center;">當前價格與波動資訊</h3>
        
        <table style="width: 100%; border-collapse: collapse; font-family: 'monospace', 'Inter', sans-serif; text-align: center; color: #ffffff;">
            <colgroup>
                <col style="width: 16%;">
                <col style="width: 16%;">
                <col style="width: 16%;">
                <col style="width: 16%;">
                <col style="width: 16%;">
                <col style="width: 20%;">
            </colgroup>
            <thead>
                <tr>
                    <th style="padding: 2px; color: #ffffff; font-weight: bold; border-bottom: 1px solid #888;">日漲跌</th>
                    <th style="padding: 2px; color: #ffffff; font-weight: bold; border-bottom: 1px solid #888;">波幅</th>
                    <th style="padding: 2px; color: #ffffff; font-weight: bold; border-bottom: 1px solid #888;">開盤</th>
                    <th style="padding: 2px; color: #ffffff; font-weight: bold; border-bottom: 1px solid #888;">最高</th>
                    <th style="padding: 2px; color: #ffffff; font-weight: bold; border-bottom: 1px solid #888;">最低</th>
                    <th style="padding: 2px; color: #ffffff; font-weight: bold; border-bottom: 1px solid #888;">最新價</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td style="padding: 2px; {change_color_style}">{price_change:+.0f} ({pct_change:+.2f}%)</td>
                    <td style="padding: 2px;">{intraday_range:.0f} ({pct_range:.2f}%)</td>
                    <td style="padding: 2px;">{open_price:.0f}</td>
                    <td style="padding: 2px;">{intraday_high:.0f}</td>
                    <td style="padding: 2px;">{intraday_low:.0f}</td>
                    <td style="padding: 2px; font-size: 1.1em; font-weight: bold;">{close_price:.0f}</td>
                </tr>
            </tbody>
        </table>
    </div>
    """

    return stats_html_content
    #------------------------------------------------------------------------------------------------------------------


def find_previous_close(target_date: date = None, max_lookback: int = 20) -> tuple[float, float]:
    """
    往前回溯最多 max_lookback 天，取得最近一個交易日的
    台指期 (TXF) 和加權指數 (TSE) 的日盤收盤價。

    參數：
        target_date: 預設為今天，會從前一天開始回溯查找。
        max_lookback: 最多往前查幾天。

    回傳：
        (txf_close, tse_close)
    """
    
    # === API 登入 ===
    def login_shioaji(api_key: str, secret_key: str, simulation: bool = True) -> sj.Shioaji:
        api = sj.Shioaji(simulation=simulation)
        api.login(api_key=api_key, secret_key=secret_key)
        return api

    # === 擷取某日的收盤價（過濾日盤時間） ===
    def get_last_close(api, contract, query_date: date, session_end: dt_time) -> float | None:
        kbars = api.kbars(contract=contract, start=str(query_date), end=str(query_date))
        df = pd.DataFrame(dict(**kbars))
        if df.empty:
            return None
        df['ts'] = pd.to_datetime(df['ts'])
        df = df[df['ts'].dt.time <= session_end]
        return df['Close'].iloc[-1] if not df.empty else None

    # === 永豐模擬帳號金鑰 ===
    load_dotenv()
    API_KEY = os.environ.get('SHIOAJI_API_KEY')
    SECRET_KEY = os.environ.get('SHIOAJI_SECRET_KEY')

    # === 預設日期：今天的前一天 ===
    if target_date is None:
        target_date = date.today()
    pre_date = target_date - timedelta(days=1)

    # === 日盤結束時間設定 ===
    txf_end = dt_time(13, 46)

    # === 登入並查詢資料 ===
    api = login_shioaji(API_KEY, SECRET_KEY)
    try:
        for _ in range(max_lookback):
            txf_close = get_last_close(api, api.Contracts.Futures.TXF.TXFR1, pre_date, txf_end)
            tse_close = get_last_close(api, api.Contracts.Indexs.TSE.TSE001, pre_date, txf_end)
            if txf_close and tse_close:
                return txf_close, tse_close
            pre_date -= timedelta(days=1)
        raise FileNotFoundError(f"找不到 {max_lookback} 天內的 TXF / TSE 收盤價")
    finally:
        api.logout()


# ==== 輔助函數：資料準備 ====
def _prepare_plot_data(df: pd.DataFrame, txf_prev_close: float, taiex_prev_close: float) -> pd.DataFrame:
    """
    準備繪圖所需的資料，包含計算衍生欄位與排序。

    Args:
        df: 原始 tick 資料 DataFrame。
        txf_prev_close: TXF 昨日收盤價。
        taiex_prev_close: TAIEX 昨日收盤價。

    Returns:
        pd.DataFrame: 處理完成，可用於繪圖的 DataFrame。
    """
    # --- 1. 選擇所需欄位並複製 (關鍵修改點) ---
    required_initial_cols = [
        "datetime",
        "underlying_price",
        "close",
        "bid_side_total_vol",
        "ask_side_total_vol",
        "high",      # 繪圖需要
        "low",       # 繪圖需要
        "avg_price"  # 繪圖需要
    ]
    # 檢查所有必要欄位是否存在於原始 df 中
    if not all(col in df.columns for col in required_initial_cols):
        missing_cols = [col for col in required_initial_cols if col not in df.columns]
        raise ValueError(f"輸入的 DataFrame 缺少繪圖所需的原始欄位：{missing_cols}")

    # 只選取需要處理的欄位，並創建一個副本，避免修改原始 df
    processed_df = df[required_initial_cols].copy()
    
    # === 衍生欄位計算 ===
    # rrp: Relative Reference Price，由現貨價推估的期貨理論價
    processed_df['rrp_by_taiex'] = processed_df["underlying_price"] / taiex_prev_close * txf_prev_close
    processed_df['ave'] = (processed_df['rrp_by_taiex'] + processed_df["underlying_price"]) / 2
    processed_df["fut_premium"] = processed_df["close"] - processed_df["underlying_price"]
    processed_df["fut_to_rrp_premium"] = processed_df["close"] - processed_df['rrp_by_taiex']
    processed_df["cumu_net_agg_vol"] = processed_df["bid_side_total_vol"] - processed_df["ask_side_total_vol"]
    
    return processed_df

# ==== 輔助函數：繪製各子圖 ====
def _add_price_traces(fig: go.Figure, original_df: pd.DataFrame):
    """
    在 fig 的第1列新增價格相關走勢圖。
    使用 Candlestick 繪製主價格，並疊加其他指標線。
    """
    row, col = 1, 1
    # 疊加其他指標線 (使用原始高密度資料)
    fig.add_trace(go.Scattergl(x=original_df["datetime"], y=original_df["underlying_price"], name="[現貨] TAIEX", line=dict(color="blue", width=1), visible='legendonly'), row=row, col=col)
    fig.add_trace(go.Scattergl(x=original_df["datetime"], y=original_df["rrp_by_taiex"], name="[期貨] 參考價", line=dict(color="gray", width=1), visible=True), row=row, col=col)
    fig.add_trace(go.Scattergl(x=original_df["datetime"], y=original_df["close"], name="[期貨] TXF", line=dict(color="white", width=1)), row=row, col=col)
    fig.add_trace(go.Scattergl(x=original_df["datetime"], y=original_df["avg_price"], name="VWAP", line=dict(color="orange", dash="solid", width=1)), row=row, col=col)
    fig.add_trace(go.Scattergl(x=df["datetime"], y=df["high"], name="High", line=dict(color="green", dash="dash", width=1)), row=row, col=col)
    fig.add_trace(go.Scattergl(x=df["datetime"], y=df["low"], name="Low", line=dict(color="Red", dash="dash", width=1)), row=row, col=col)
    # 更新 Y 軸設定
    high, low = df.iloc[-1].high, df.iloc[-1].low
    padding = (high - low) * 0.1
    fig.update_yaxes(title_text="價格", tickformat=".0f", row=row, col=col, 
                     range=[low - padding, high + padding])
    
    # 關閉 K線圖的範圍滑桿，避免與主滑桿衝突
    fig.update_xaxes(rangeslider_visible=False, row=row, col=col)

def _add_volume_traces(fig: go.Figure, df: pd.DataFrame):
    """在 fig 的第4列新增買賣盤成交量圖。"""
    row, col = 4, 1
    
    fig.add_trace(go.Scatter(
        x=df["datetime"], 
        y=df["bid_side_total_vol"], 
        name="買盤成交總量(口)", 
        line=dict(color="green"), 
        line_shape='hv', 
        fill='tozeroy', 
        fillcolor='rgba(0, 255, 0, 0.4)'
    ), row=row, col=col)
    
    fig.add_trace(go.Scatter(
        x=df["datetime"], 
        y=df["ask_side_total_vol"], 
        name="賣盤成交總量(口)", 
        line=dict(color="red"), 
        line_shape='hv', 
        fill='tozeroy', 
        fillcolor='rgba(255, 0, 0, 0.4)'
    ), row=row, col=col)
    
    fig.update_yaxes(title_text="買賣盤成交量(口)", tickformat=".0f", row=row, col=col, autorange=True)

def _add_premium_traces(fig: go.Figure, df: pd.DataFrame):
    """在 fig 的第2列新增折溢價圖。"""
    row, col = 2, 1
    fig.add_trace(go.Scatter(x=df["datetime"], y=df["fut_premium"], name="[期貨-現貨] 折溢價", line=dict(color="blue", width=1), visible='legendonly'), row=row, col=col)
    fig.add_trace(go.Scatter(x=df["datetime"], y=df["fut_to_rrp_premium"], name="[期貨-參考價] 折溢價", line=dict(color="gray", width=1)), row=row, col=col)
    fig.update_yaxes(title_text="折溢價(-/+)", tickformat=".0f", row=row, col=col, autorange=True)

def _add_net_volume_traces(fig: go.Figure, df: pd.DataFrame):
    """在 fig 的第3列新增淨主動成交量圖。"""
    row, col = 3, 1
    # 繪製多方淨主動成交量 (綠色填滿)
    fig.add_trace(go.Scatter(
        x=df["datetime"], y=df["cumu_net_agg_vol"].where(df["cumu_net_agg_vol"] > 0),
        name="淨主動成交量(多方)", mode="lines", line=dict(color="green"),
        fill="tozeroy", fillcolor="rgba(0, 128, 0, 0.4)"
    ), row=row, col=col)
    # 繪製空方淨主動成交量 (紅色填滿)
    fig.add_trace(go.Scatter(
        x=df["datetime"], y=df["cumu_net_agg_vol"].where(df["cumu_net_agg_vol"] < 0),
        name="淨主動成交量(空方)", mode="lines", line=dict(color="red"),
        fill="tozeroy", fillcolor="rgba(255, 0, 0, 0.4)"
    ), row=row, col=col)
    fig.update_yaxes(title_text="淨主動成交量(口)", tickformat=".0f", row=row, col=col, autorange=True)

# ==== 輔助函數：圖表佈局設定 ====
def _configure_layout(fig: go.Figure):
    """設定圖表的整體佈局、標題與圖例。"""
    fig.update_layout(
        title=dict(text="價格走勢與淨主動成交量", y=0.95),
        template='plotly_dark',     # 暗色主題
        height=2000, # 調整為較適合螢幕的高度
        showlegend=True,
        legend=dict(x=0.5, y=1.1, orientation="h", xanchor="center", yanchor="bottom"),
        xaxis=dict(
            rangeslider_visible=False, 
            showspikes=True, spikemode='across', spikesnap='cursor', showline=True,
        ),
        xaxis2=dict(showspikes=True, spikemode='across', spikesnap='cursor', showline=True),
        xaxis3=dict(showspikes=True, spikemode='across', spikesnap='cursor', showline=True),
        xaxis4=dict(showspikes=True, spikemode='across', spikesnap='cursor', showline=True),
        xaxis_showticklabels=True,
        xaxis2_showticklabels=True,
        xaxis3_showticklabels=True,
        xaxis4_showticklabels=True,
    )

# ==== 主繪圖函數 (重構後) ====
def plot_tick_analysis(df: pd.DataFrame, txf_prev_close: float, taiex_prev_close: float, refresh_interval_seconds: int = 15):
    """
    視覺化 Tick 資料，整合價格、基差、主動成交量等多維度分析。

    Args:
        df (pd.DataFrame): 包含 Tick 資料的 DataFrame。
        txf_prev_close (float): 台指期貨昨日收盤價。
        taiex_prev_close (float): 加權指數昨日收盤價。
        refresh_interval_seconds (int): 刷新間隔秒數 (此處為保留參數)。
    """
    # 1. 準備繪圖資料及衍生指標
    plot_df = _prepare_plot_data(df, txf_prev_close, taiex_prev_close)

    # 2. 建立 4x1 的子圖畫布
    fig = make_subplots(
        rows=4, cols=1,
        shared_xaxes=True,
        row_heights=[0.375, 0.225, 0.2, 0.2],
        vertical_spacing=0.05,
        subplot_titles=("價格走勢", "期貨折溢價", "買賣盤成交總量差", "買賣盤成交總量")
    )

    # 3. 依序繪製各個子圖
    _add_price_traces(fig, plot_df)
    _add_premium_traces(fig, plot_df)
    _add_net_volume_traces(fig, plot_df)
    _add_volume_traces(fig, plot_df)

    # 4. 設定圖表全域樣式
    _configure_layout(fig)

    # 5. 顯示圖表
    # fig.show()

    #------------------------------------------------------------------------------
    fig_candlestick = plot_candlestick_with_volume_delta(df_Vol_Kbars)
    
    # === 呼叫新的輔助函數來生成統計資訊 HTML ===
    stats_html = print_intraday_stats(df)

    # 合併多張圖成一個 HTML 字串
    figs = [fig_candlestick, fig]
    html_body = ""
    for fig in figs:
        html_body += pio.to_html(fig, include_plotlyjs='cdn', full_html=False)

    title_name = f"TXF-Charts_{start_datetime.strftime("%Y-%m-%d_%H%M")}"
    full_html = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="refresh" content="{refresh_interval_seconds}">
        <title>{title_name}</title>
        <style>
            /* 自定義樣式 */
            .positive-change {{ color: green; }}
            .negative-change {{ color: red; }}
            body {{
                background-color: black;
                color: white;
            }}
        </style>
    </head>
    <body>
    {stats_html}
    {html_body}
    </body>
    </html>
    """
    output_dir = Path("/Users/gtai/Downloads")
    output_dir.mkdir(parents=True, exist_ok=True)  # 確保資料夾存在
    
    output_file = output_dir / f"{title_name}.html"  # 組合完整路徑
    
    output_file.write_text(full_html, encoding="utf-8")  # 寫入內容
    # with open(f"{title_name}.html", "w", encoding="utf-8") as f:
    #     f.write(full_html)
    #------------------------------------------------------------------------------

def generate_volume_bars(tick: pd.DataFrame, volume_per_bar=500):
    """
    高效生成成交量 K棒 (Volume Bars)。
    函數僅依賴 datetime, close, 以及買賣雙方的累計成交量欄位。
    """
    if tick.empty:
        print("無交易資料，跳過。")
        return pd.DataFrame()

    # 1. 計算每個 tick 所屬的 Bar 編號
    total_cumulative_volume = tick['bid_side_total_vol'] + tick['ask_side_total_vol']
    bar_id = (total_cumulative_volume // volume_per_bar).astype(int)

    # 2. 分組聚合，取得 Bar 的基礎資訊
    agg_funcs = {
        'datetime': ['first', 'last'],              # 開關盤時間
        'close': ['first', 'max', 'min', 'last'],   # O, H, L, C
        'bid_side_total_vol': 'last',               # Bar 結束時的買盤總成交量(外盤成交; tick type=1)
        'ask_side_total_vol': 'last',               # Bar 結束時的賣盤總成交量(內盤成交; tick type=2)
    }
    bars_df = tick.groupby(bar_id).agg(agg_funcs)

    # 3. 重新命名聚合後的欄位
    bars_df.columns = [
        'start_time', 'end_time', 'open', 'high', 'low', 'close',
        'last_bid_total', 'last_ask_total'
    ]

    # 4. 計算 Bar 內的成交量
    # 用 .diff() 計算與前一 Bar 的差值，即為此 Bar 的成交量
    bars_df['aggressive_buy_volume'] = bars_df['last_bid_total'].diff().fillna(bars_df['last_bid_total'])
    bars_df['aggressive_sell_volume'] = bars_df['last_ask_total'].diff().fillna(bars_df['last_ask_total'])
    

    # 5. 清理並排列最終欄位
    # 刪除計算用的輔助欄位
    bars_df = bars_df.drop(columns=['last_bid_total', 'last_ask_total'])
    
    # 定義最終欄位順序
    final_columns = [
        'start_time', 'end_time', 'open', 'high', 'low', 'close', 
        'aggressive_buy_volume', 'aggressive_sell_volume'
    ]

    return bars_df[final_columns].reset_index(drop=True)
    

def plot_candlestick_with_volume_delta(df: pd.DataFrame):
    """
    繪製K線圖與下方的買賣盤成交量分析圖 (Volume Delta)。
    - 上方子圖: OHLC K線圖 (Candlestick)。
    - 下方子圖: 主動買盤 (綠色) 與主動賣盤 (紅色) 的成交量長條圖。
    """
    # 1. 數據檢查：確認 DataFrame 不為空
    if df is None or df.empty:
        print("無交易資料，跳過繪圖。")
        return

    # 2. 動態計算 Bar 寬度，以適應時間不均的 Volume Bar
    df = df.sort_values('end_time')
    # 計算 Bar 之間時間間隔的中位數（秒）
    time_deltas = pd.to_datetime(df['end_time']).diff().dt.total_seconds()
    median_interval = time_deltas.median()
    # 將寬度設為間隔的 20%，並轉換為 Plotly 所需的毫秒
    bar_width_sec = median_interval * 0.2 if pd.notna(median_interval) and median_interval > 0 else 10
    bar_width_ms = bar_width_sec * 1000

    # 3. 建立子圖：上方K線圖(70%)，下方成交量圖(30%)
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,          # 共享 X 軸
        vertical_spacing=0.05,      # 子圖間距
        row_heights=[0.75, 0.25],     # 子圖高度比例
        subplot_titles=('Candlestick Chart', 'Volume Delta')
    )

    # 4. 繪製上方的 K 線圖
    fig.add_trace(go.Candlestick(
        x=df['end_time'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='OHLC',
        increasing_line_color='green',
        decreasing_line_color='red'
    ), row=1, col=1)

    # 5. 繪製下方的主動成交量圖
    # 主動買盤 (Aggressor Buy)
    fig.add_trace(go.Bar(
        x=df['end_time'],
        y=df['aggressive_buy_volume'],
        width=bar_width_ms,
        name='Aggressor Buy',
        marker_color='darkgreen',
        opacity=0.4
    ), row=2, col=1)

    # 主動賣盤 (Aggressor Sell)
    fig.add_trace(go.Bar(
        x=df['end_time'],
        y=df['aggressive_sell_volume'],
        width=bar_width_ms,
        name='Aggressor Sell',
        marker_color='darkred',
        opacity=0.4
    ), row=2, col=1)
    
    # # 將下方長條圖改為堆疊模式
    # fig.update_layout(barmode='stack')

    # 6. 設定圖表整體樣式與佈局
    fig.update_layout(
        title_text='Candlestick with Volume Delta',
        template='plotly_dark',     # 暗色主題
        hovermode='x unified',      # 統一的懸停提示
        height=700,
        xaxis_rangeslider_visible=False, # 隱藏下方的範圍滑桿
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), # 圖例置於圖表上方
        xaxis=dict(
            rangeslider_visible=False, 
            showspikes=True, spikemode='across', spikesnap='cursor', showline=True,
        ),
        xaxis2=dict(showspikes=True, spikemode='across', spikesnap='cursor', showline=True),
    )

    # 7. 更新座標軸標題
    fig.update_yaxes(title_text="Price", tickformat=".0f", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)

    # 8. 顯示圖表
    # fig.show()

    # -----------------------------------
    return fig
    # -----------------------------------


# %%
# Cell 3: 初始化與連線 (Initialization and Connection)


# ==== 1. 建立 Kafka Consumer ====
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'enable.auto.commit': False,
    'enable.partition.eof': True
})

# ==== 2. 查詢指定時間的起始 offset ====
start_dt_utc = start_datetime.astimezone(timezone.utc)
timestamp_ms = int(start_dt_utc.timestamp() * 1000)
metadata = consumer.list_topics(TOPIC)
partitions = list(metadata.topics[TOPIC].partitions.keys())
topic_partitions = [TopicPartition(TOPIC, p, timestamp_ms) for p in partitions]
fixed_offsets = consumer.offsets_for_times(topic_partitions)
current_offsets = fixed_offsets.copy() # 將初始 offset 設為當前 offset

# ==== 3. 讀取前一日收盤價 ====
txf_prev_close, taiex_prev_close = find_previous_close(target_date = start_datetime.date(), max_lookback = 20)

# ==== 4. 初始化資料容器 (只執行一次) ====
tick_dict = {}
print("✅ 初始化完成，隨時可以開始獲取資料。")

# %%
# Cell 4: 執行與動態更新


while True:
    # ==== 主執行迴圈 ====
    # 在每次迴圈開始時清除輸出
    
    # 0. 設定目標時間
    fix_end_datetime = True
    
    if fix_end_datetime:
        end_datetime = fixed_end_datetime
    else:
        end_datetime = datetime.now(tz=TAIWAN_TZ)
    
    print(f"模式: {'固定時間' if fix_end_datetime else '即時'} | 目標結束時間: {end_datetime} (Asia/Taipei)")
    print("🔄 準備從 Kafka 獲取資料...")

    try:
        # 1. 擷取資料
        df, current_offsets = fetch_ticks(
            consumer=consumer, offsets=current_offsets, 
            start_datetime=start_datetime, end_datetime=end_datetime, 
            tick_dict=tick_dict)
        df_Vol_Kbars = generate_volume_bars(df, 450)
        # 2. 繪圖
        if not df.empty:
            print("資料獲取完畢,準備繪圖...\n")
            plot_tick_analysis(df, txf_prev_close, taiex_prev_close, 120000)
        else:
            print("未獲取任何資料，無法繪圖。")

    except KeyError as e:
        print(f"⚠️ 發生 KeyError: {e}。這可能是因為目前還沒有符合條件的資料，或資料格式有誤。")
        print("請檢查您的 Kafka 訊息內容，以及 `fetch_ticks` 函式中 `tick_dict` 的建立方式。")
        print(f"目前設定的開始時間為: {start_datetime}，等待資料中...")
    except Exception as e:
        # 捕獲其他未預期的錯誤
        print(f"❌ 發生未知錯誤: {e}")
        print("請檢查程式碼的其他部分或錯誤訊息，以釐清問題。")

    break
    # time.sleep(15)



