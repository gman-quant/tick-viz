# src/data_sourcing/market_data.py


from datetime import date, timedelta, time as dt_time
from pathlib import Path

import pandas as pd

from config.config import SHIOAJI_API_KEY as api_key, SHIOAJI_SECRET_KEY as secret_key
from config.run_context import RunContext
from config.types import SessionType
from src.utils.session_time import in_which_session
from src.utils.resource_contexts import shioaji_session


def get_contract(api, symbol: str):
    if symbol == "txf":
        return api.Contracts.Futures.TXF.TXFR1
    elif symbol == "tse":
        return api.Contracts.Indexs.TSE.TSE001
    else:
        raise ValueError(f"Unsupported symbol: {symbol}")


def load_or_fetch_kbars(api, query_date: date, symbol: str) -> pd.DataFrame:
    """
    嘗試讀取 parquet，若失敗則透過 API 抓取指定合約的 kbars 並快取。
    symbol 例：'txf' 或 'tse'
    """
    output_dir = Path(__file__).resolve().parents[2] / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{symbol.lower()}-kbars_{query_date}.parquet"
    output_file = output_dir / file_name

    # 嘗試讀取快取檔案
    df = pd.DataFrame()
    try:
        df = pd.read_parquet(output_file)
        print(f"✅ Loaded {symbol.upper()} data from {output_file}")
    except FileNotFoundError:
        print(f"⚠️ 檔案不存在：{output_file}，將跳過載入並回傳空 DataFrame。")
    except pd.errors.EmptyDataError:
        print(f"⚠️ 檔案為空：{output_file}，將回傳空 DataFrame。")
    except Exception as e:
        print(f"⚠️ 讀取 {output_file} 時發生未預期錯誤：{e}，將回傳空 DataFrame。")

    if not df.empty or api is None:
        return df

    # API fallback 抓資料
    print(f"⚠️ Fetching {symbol.upper()} kbars from API for {query_date}...")

    contract = get_contract(api, symbol)
    kbars = api.kbars(
        contract=contract,
        start=str(query_date),
        end=str(query_date)
    )

    df = pd.DataFrame({**kbars})
    if df.empty:
        print(f"❌ API 回傳空資料，無法取得 {symbol.upper()} {query_date} 的 kbars")
        return df

    df['ts'] = pd.to_datetime(df['ts'])
    df.rename(columns={'ts': 'datetime'}, inplace=True)

    df.to_parquet(output_file)
    print(f"💾 Saved {symbol.upper()} kbars to {output_file}")
    return df


def _get_last_close(api, query_date: date, symbol: str) -> float | None:
    """從指定合約與日期抓收盤價（若無資料回傳 None）"""
    df = load_or_fetch_kbars(api, query_date, symbol)
    if df.empty:
        return None
    day_session_df = df[df['datetime'].dt.time < dt_time(13, 46)]
    return day_session_df['Close'].iloc[-1] if not day_session_df.empty else None


def find_previous_close(ctx: RunContext, api=None, max_lookback: int = 10) -> tuple[float, float]:
    """
    回溯最多 max_lookback 天，依序嘗試從本地與 API 查詢，
    尋找最近一個交易日的台指期與加權指數日盤收盤價。
    若 api 為 None，則自動建立 session 後查詢。
    """
    def _try_get_close(query_date: date, api) -> tuple[float, float] | None:
        if query_date.weekday() >= 5:
            return None

        txf_close = _get_last_close(None, query_date, symbol="txf")
        tse_close = _get_last_close(None, query_date, symbol="tse")
        if txf_close is not None and tse_close is not None:
            print(f"📂 本地資料: {query_date} TXF={txf_close}, TSE={tse_close}")
            return txf_close, tse_close

        if api is not None:
            txf_close = _get_last_close(api, query_date, symbol="txf")
            tse_close = _get_last_close(api, query_date, symbol="tse")
            if txf_close is not None and tse_close is not None:
                print(f"🌐 API 資料: {query_date} TXF={txf_close}, TSE={tse_close}")
                return txf_close, tse_close

        return None

    def _lookup_all(api, start_date: date) -> tuple[float, float] | None:
        query_date = start_date
        for _ in range(max_lookback):
            result = _try_get_close(query_date, api)
            if result:
                return result
            query_date -= timedelta(days=1)
        return None
    
    current_date = ctx.start_datetime.date()
    current_time = ctx.start_datetime.time()
    session_type = in_which_session(current_time)

    start_date = current_date - timedelta(days=1) if session_type == SessionType.DAY else current_date

    if api is not None:
        result = _lookup_all(api, start_date)
    else:
        with shioaji_session(api_key, secret_key) as api:
            result = _lookup_all(api, start_date)

    if result:
        return result

    raise FileNotFoundError(f"❌ 在過去 {max_lookback} 天內找不到 TXF / TSE 收盤價。")

        