# src/processing/kbars.py

# Standard Library Imports
import logging

# Third-Party Imports
import pandas as pd

# Local Application Imports
from config.run_context import RunContext
from config.types import SessionType


def generate_kbars(tick: pd.DataFrame, period: str = '1min', ctx: RunContext = None) -> pd.DataFrame:
    """
    將 tick 資料依時間聚合為 K 線 (Time-based Bars)
    支援常見時間週期：
      - 秒級:  '1s', '15s', '30s'
      - 分級:  '1min', '5min', '15min', '30min'
      - 小時級: '60min' 或 '1H'
    - 自動計算 O/H/L/C、買/賣盤成交量與總成交量
    """

    # --- 1. 處理空資料 ---
    if tick.empty:
        logging.warning("Kbars: 無交易資料，跳過。") 
        return pd.DataFrame()

    # --- 2. 確保 datetime 為索引 ---
    if 'datetime' in tick.columns:
        tick = tick.set_index('datetime')
    if not isinstance(tick.index, pd.DatetimeIndex):
        raise ValueError("DataFrame 必須有 datetime 索引或 datetime 欄位。")

    # --- 3. 定義 K 線聚合方法 ---
    agg_funcs = {
        'close': ['first', 'max', 'min', 'last'],  # O/H/L/C
        'total_volume': 'last',                    # 總成交量 (用 'last' 取得累計值)
    }

    # --- 4. 依時間重採樣 (Resample) ---
    # (根據盤別設定 offset，確保 K 棒時間戳對齊)
    offset = '0h'
    if ctx.session_type == SessionType.DAY:
        offset='8h45min'
    elif ctx.session_type == SessionType.NIGHT:
        offset='15h'
        
    bars_df = tick.resample(period, origin='start_day', offset=offset).agg(agg_funcs)
    bars_df.columns = [
        'open', 'high', 'low', 'close',
        'total_volume'
    ]

    # --- 5. 計算 K 棒增量成交量 ---
    # (total_volume 欄位是累計值，需用 diff 計算每根 K 棒的實際成交量)
    bars_df['volume'] = bars_df['total_volume'].diff().fillna(0)
    
    # (diff() 會使第一筆為 NaN，需手動填入第一筆的累計值)
    first_index = bars_df.index[0]
    bars_df.loc[first_index, 'volume'] = bars_df['total_volume'].iloc[0]

    # --- 6. 清理並重排欄位 ---
    bars_df = bars_df.drop(columns=['total_volume']) # 移除累計欄位
    final_columns = [
        'open', 'high', 'low', 'close',
        'volume'
    ]

    return bars_df[final_columns].reset_index()