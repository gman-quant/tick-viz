# src/processing/metrics.py

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
    if not all(col in df.columns for col in required_initial_cols):
        missing_cols = [col for col in required_initial_cols if col not in df.columns]
        raise ValueError(f"輸入的 DataFrame 缺少繪圖所需的原始欄位：{missing_cols}")

    # 只選取需要處理的欄位，並創建一個副本 (避免 SettingWithCopyWarning)
    processed_df = df[required_initial_cols].copy()
    
    # --- 2. 計算衍生指標 ---
    
    # rrp: Relative Reference Price，由現貨價推估的期貨理論價
    window_size = 150
    window_size2 = 180
    processed_df['rrp_by_taiex'] = processed_df['underlying_price'] / taiex_prev_close * txf_prev_close
    
    # rrp 滾動高低點
    processed_df['rrp_rhigh'] = processed_df['rrp_by_taiex'].rolling(window_size, min_periods=1).max()
    processed_df['rrp_rlow']  = processed_df['rrp_by_taiex'].rolling(window_size, min_periods=1).min()
    
    # 價差指標
    processed_df['fut_premium'] = processed_df['close'] - processed_df['underlying_price']
    processed_df['fut_to_rrp_premium'] = processed_df['close'] - processed_df['rrp_by_taiex']
    processed_df['fut_to_vwap_premium'] = processed_df['close'] - processed_df['avg_price']
    
    # 買賣力指標
    processed_df['cumu_net_agg_vol'] = processed_df['bid_side_total_vol'] - processed_df['ask_side_total_vol']
    
    # 買賣力變化 (動能)
    processed_df['bid_side_volume_change'] = (
        processed_df['bid_side_total_vol'] - processed_df['bid_side_total_vol'].shift(window_size2)
    )
    processed_df['ask_side_volume_change'] = (
        processed_df['ask_side_total_vol'] - processed_df['ask_side_total_vol'].shift(window_size2)
    )
    processed_df['net_agg_vol_change'] = (
        (processed_df['bid_side_volume_change'] - processed_df['ask_side_volume_change']) / 
        (processed_df['bid_side_volume_change'] + processed_df['ask_side_volume_change'])
    )
    
    # rvwap 相關指標
    # processed_df['rvwap_to_vwap_premium'] = processed_df['rvwap'] - processed_df['avg_price']
    # processed_df['rvwap-rrp_rh'] = processed_df['rvwap'] - processed_df['rrp_rhigh']
    # processed_df['rvwap-rrp_rl'] = processed_df['rvwap'] - processed_df['rrp_rlow']
    # processed_df['rrp_rh-rrp_rl'] = processed_df['rrp_rhigh'] - processed_df['rrp_rlow']
    # processed_df['close-rvwap'] = processed_df['close'] - processed_df['rvwap']
    
    return processed_df