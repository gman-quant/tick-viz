# src/processing/volume_bars.py

import pandas as pd

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