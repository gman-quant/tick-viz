# src/processing/metrics.py

from datetime import timedelta

import pandas as pd

def prepare_plot_data(df: pd.DataFrame, txf_prev_close: float, taiex_prev_close: float) -> pd.DataFrame:
    """
    準備繪圖所需的資料，包含計算衍生欄位。
    """
    
    # --- 1. 欄位檢查與複製 ---
    required_initial_cols = [
        "datetime",
        "underlying_price",
        "close",
        "bid_side_total_vol",
        "ask_side_total_vol",
        "total_volume",
        "high",
        "low",
        "avg_price",
        "sma",
        "sma2"
    ]
    
    # 檢查所有必要欄位是否存在
    missing_cols = list(set(required_initial_cols) - set(df.columns))
    if missing_cols:
        raise ValueError(f"輸入的 DataFrame 缺少繪圖所需的原始欄位：{missing_cols}")

    # 只選取需要處理的欄位，並創建一個副本 (避免 SettingWithCopyWarning)
    processed_df = df[required_initial_cols].copy()
    
    # --- 2. 計算衍生指標 ---

    # --- 參數設定 ---
    window_size = 150    # rrp 滾動高低點
    window_size2 = 180   # 買賣力變化窗口

    # --- 衍生指標計算 ---
    processed_df = (
        processed_df
        # RRP 與價差指標
        .assign(
            # SIF_price: spot implied futures price，由現貨價漲跌幅推估的期貨價
            SIF_price=lambda df: df['underlying_price'] / taiex_prev_close * txf_prev_close,
            cumu_vol_delta=lambda df: df['bid_side_total_vol'] - df['ask_side_total_vol'],
            dif=lambda df: df['sma'] - df['sma2']
        )
    )
    
    # SIF Price 滾動高低點（一次計算 max/min）
    processed_df[['SIF_rolling_high','SIF_rolling_low']] = processed_df['SIF_price'].rolling(
        window_size, min_periods=1
    ).agg(['max','min'])

    # 買賣力變化 (動能) 及 OFI
    vol_change = processed_df[['bid_side_total_vol','ask_side_total_vol']].diff(window_size2).rename(
        columns={
            'bid_side_total_vol': 'bid_side_volume_change', 
            'ask_side_total_vol': 'ask_side_volume_change'
        }
    )
    processed_df = processed_df.join(vol_change)

    # 計算 OFI: order flow imbalance
    b = processed_df['bid_side_volume_change']
    a = processed_df['ask_side_volume_change']
    processed_df['OFI'] = (b - a) / (b + a)
    
    return processed_df