# tick-viz/src/data_sourcing/market_data.py

import shioaji as sj
import pandas as pd
from datetime import date, timedelta, time as dt_time

def find_previous_close(api_key: str, secret_key: str, target_date: date, max_lookback: int = 20) -> tuple[float, float]:
    """
    往前回溯最多 max_lookback 天，取得最近一個交易日的
    台指期 (TXF) 和加權指數 (TSE) 的日盤收盤價。
    """
    api = sj.Shioaji(simulation=True)
    try:
        api.login(api_key=api_key, secret_key=secret_key)

        end_time = dt_time(13, 46)
        
        current_date = target_date - timedelta(days=1)

        for _ in range(max_lookback):
            txf_close = _get_last_close(api, api.Contracts.Futures.TXF.TXFR1, current_date, end_time)
            tse_close = _get_last_close(api, api.Contracts.Indexs.TSE.TSE001, current_date, end_time)
            
            if txf_close is not None and tse_close is not None:
                print(f"✅ 成功於 {current_date} 找到前日收盤價: TXF={txf_close}, TAIEX={tse_close}")
                return txf_close, tse_close
                
            current_date -= timedelta(days=1)
            
        raise FileNotFoundError(f"在過去 {max_lookback} 天內找不到 TXF / TSE 的收盤價。")

    finally:
        api.logout()

def _get_last_close(api, contract, query_date: date, session_end: dt_time) -> float | None:
    """輔助函式：擷取某日的日盤收盤價"""
    kbars = api.kbars(contract=contract, start=str(query_date), end=str(query_date))
    df = pd.DataFrame(dict(**kbars))
    if df.empty:
        return None
    df['ts'] = pd.to_datetime(df['ts'])
    day_session_df = df[df['ts'].dt.time < session_end]
    return day_session_df['Close'].iloc[-1] if not day_session_df.empty else None