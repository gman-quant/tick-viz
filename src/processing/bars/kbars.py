# src/processing/kbars.py (v2, 統一使用 logging)

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

    if tick.empty:
        # logging.warning
        logging.warning("Kbars: 無交易資料，跳過。") 
        return pd.DataFrame()

    # 確保 datetime 為索引
    if 'datetime' in tick.columns:
        tick = tick.set_index('datetime')
    if not isinstance(tick.index, pd.DatetimeIndex):
        raise ValueError("DataFrame 必須有 datetime 索引或 datetime 欄位。")

    # 定義聚合方法
    agg_funcs = {
        'close': ['first', 'max', 'min', 'last'],  # O/H/L/C
        # 'bid_side_total_vol': 'last',              # 外盤總成交量
        # 'ask_side_total_vol': 'last',              # 內盤總成交量
        'total_volume': 'last',                    # 總成交量
    }

    # 依時間聚合
    offset = '0h'
    if ctx.session_type == SessionType.DAY:
        offset='8h45min'
    elif ctx.session_type == SessionType.NIGHT:
        offset='15h'
    bars_df = tick.resample(period, origin='start_day', offset=offset).agg(agg_funcs)
    bars_df.columns = [
        'open', 'high', 'low', 'close',
        # 'bid_side_total_vol', 'ask_side_total_vol', 
        'total_volume'
    ]

    # 計算增量成交量
    # bars_df['aggressive_buy_volume'] = bars_df['bid_side_total_vol'].diff().fillna(0)
    # bars_df['aggressive_sell_volume'] = bars_df['ask_side_total_vol'].diff().fillna(0)
    bars_df['volume'] = bars_df['total_volume'].diff().fillna(0)
    # 第一筆保留原本的數值（因為 diff() 的第一筆 NaN 在這裡被 0 覆蓋了，所以需要重新賦值）
    # bars_df.index[0] 取得第一個索引值 (可能是日期/時間或數字 0)
    first_index = bars_df.index[0]
    # bars_df.loc[first_index, 'aggressive_buy_volume'] = bars_df['bid_side_total_vol'].iloc[0]
    # bars_df.loc[first_index, 'aggressive_sell_volume'] = bars_df['ask_side_total_vol'].iloc[0]
    bars_df.loc[first_index, 'volume'] = bars_df['total_volume'].iloc[0]

    # 清理欄位
    # bars_df = bars_df.drop(columns=['bid_side_total_vol', 'ask_side_total_vol'])

    # 欄位順序
    final_columns = [
        'open', 'high', 'low', 'close',
        # 'aggressive_buy_volume', 'aggressive_sell_volume', 
        'volume'
    ]

    return bars_df[final_columns].reset_index()