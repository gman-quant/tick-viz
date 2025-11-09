# src/processing/volume_bars.py

# Standard Library Imports
import logging

# Third-Party Imports
import pandas as pd

# Local Application Imports
from config.types import SessionType


# ------------------------------------------------------------
# 📦 (Helper) 取得每根 Bar 的基準成交量
# ------------------------------------------------------------
def get_volume_per_bar(session_type: SessionType = SessionType.NIGHT) -> int:
    return 500 if session_type == SessionType.DAY else 250

# ------------------------------------------------------------
# 📦 生成成交量 K 棒 (Volume Bars)
# ------------------------------------------------------------
def generate_volume_bars(tick: pd.DataFrame, volume_per_bar: int = get_volume_per_bar()):
    """
    高效生成成交量 K棒 (Volume Bars)。
    函數僅依賴 datetime, close, 以及買賣雙方的累計成交量欄位。
    """
    
    # --- 1. 處理空資料 ---
    if tick.empty:
        logging.warning("VolumeBars: 無交易資料，跳過。")
        return pd.DataFrame()

    # --- 2. 計算每個 tick 所屬的 Bar 編號 ---
    total_cumulative_volume = tick['bid_side_total_vol'] + tick['ask_side_total_vol']
    bar_id = (total_cumulative_volume // volume_per_bar).astype(int)

    # --- 3. 分組聚合，取得 Bar 的基礎資訊 ---
    agg_funcs = {
        'datetime': ['first', 'last'],              # 開關盤時間
        'close': ['first', 'max', 'min', 'last'],   # O, H, L, C
        'bid_side_total_vol': 'last',               # Bar 結束時的買盤累計 (外盤)
        'ask_side_total_vol': 'last',               # Bar 結束時的賣盤累計 (內盤)
    }
    bars_df = tick.groupby(bar_id).agg(agg_funcs)

    # --- 4. 重新命名聚合後的欄位 ---
    bars_df.columns = [
        'start_time', 'end_time', 'open', 'high', 'low', 'close',
        'last_bid_total', 'last_ask_total'
    ]

    # --- 5. 計算 Bar 內的增量成交量 ---
    # (用 .diff() 計算與前一 Bar 的差值，即為此 Bar 的成交量)
    # (fillna 用第一筆的累計值填補 diff 產生的 NaN)
    bars_df['aggressive_buy_volume'] = bars_df['last_bid_total'].diff().fillna(bars_df['last_bid_total'])
    bars_df['aggressive_sell_volume'] = bars_df['last_ask_total'].diff().fillna(bars_df['last_ask_total'])
    
    # --- 6. 清理並排列最終欄位 ---
    bars_df = bars_df.drop(columns=['last_bid_total', 'last_ask_total'])
    
    final_columns = [
        'start_time', 'end_time', 'open', 'high', 'low', 'close', 
        'aggressive_buy_volume', 'aggressive_sell_volume'
    ]

    return bars_df[final_columns].reset_index(drop=True)