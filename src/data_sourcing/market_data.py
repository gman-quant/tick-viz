# tick-viz/src/data_sourcing/market_data.py


from datetime import date, timedelta, time as dt_time
from pathlib import Path

import pandas as pd
import shioaji as sj

import config
from src.utils.session_time import is_day_session


def load_or_fetch_kbars(
    api,
    query_date: date,
    symbol: str,
) -> pd.DataFrame:
    """
    嘗試讀取 parquet，若失敗則透過 API 抓取指定合約的 kbars 並快取。
    symbol 例：'txf' 或 'tse'
    """
    output_dir = Path(__file__).resolve().parents[2] / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{symbol.lower()}-kbars_{query_date}.parquet"
    output_file = output_dir / file_name

    try:
        df = pd.read_parquet(output_file)
        print(f"✅ Loaded {symbol.upper()} data from {output_file}")
    except Exception:
        print(f"⚠️ Fetching {symbol.upper()} kbars from API for {query_date}...")
        contract = api.Contracts.Futures.TXF.TXFR1 if symbol == "txf" else api.Contracts.Indexs.TSE.TSE001
        kbars = api.kbars(
            contract=contract,
            start=str(query_date),
            end=str(query_date)
        )

        df = pd.DataFrame({**kbars})
        df['ts'] = pd.to_datetime(df['ts'])
        df.rename(columns={'ts': 'datetime'}, inplace=True)
        if not df.empty:
            df.to_parquet(output_file)
            print(f"💾 Saved {symbol.upper()} kbars to {output_file}")

    return df

def _get_last_close(
    api,
    day_session: bool,
    query_date: date,
    symbol: str,
) -> float | None:
    """從指定合約與日期抓收盤價（若無資料回傳 None）"""
    df = load_or_fetch_kbars(api, query_date, symbol)
    if df.empty:
        return None
    session_end = dt_time(13, 46)
    day_session_df = df[df['datetime'].dt.time < session_end]
    # print(f"{symbol}: {day_session_df}")
    return day_session_df['Close'].iloc[-1] if not day_session_df.empty else None

def find_previous_close(
    max_lookback: int = 20
) -> tuple[float, float]:
    """
    回溯最多 max_lookback 天，尋找最近一個交易日的台指期與加權指數日盤收盤價。
    """
    api = sj.Shioaji(simulation=True)
    api.login(api_key=config.SHIOAJI_API_KEY, secret_key=config.SHIOAJI_SECRET_KEY)
    try:
        current_date = config.START_DATETIME.date()
        current_time = config.START_DATETIME.time()
        day_session = is_day_session(current_time)
        txf_query_date = current_date
        tse_query_date = current_date - timedelta(days=1) if day_session else current_date
        
        for _ in range(max_lookback):
            txf_close = _get_last_close(api, day_session, tse_query_date, symbol="txf")
            tse_close = _get_last_close(api, day_session, tse_query_date, symbol="tse")

            if txf_close is not None and tse_close is not None:
                print(f"✅ 成功找到收盤價: TXF={txf_close}({txf_query_date}), TSE={tse_close}({tse_query_date})")
                return txf_close, tse_close

            txf_query_date -= timedelta(days=1)
            tse_query_date -= timedelta(days=1)

        raise FileNotFoundError(f"❌ 在過去 {max_lookback} 天內找不到 TXF / TSE 收盤價。")
    finally:
        api.logout()