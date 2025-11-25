# src/data_sourcing/market_data.py

# Standard Library Imports
import logging
from datetime import date, timedelta

# Third-Party Imports
import duckdb

# Local Application Imports
from config.config import CACHE_DIR
from config.run_context import RunContext
from config.types import SessionType
from src.data_sourcing.fetch_ticks import get_or_fetch_contract_ticks
from src.utils.misc import get_contract
from src.utils.resource_contexts import shioaji_session


def _get_last_close(api, query_date: date, symbol: str) -> float | None:
    """(輔助) 從指定合約與日期抓日盤收盤價（若無資料回傳 None）"""
    # --- 0. 設定快取路徑 ---
    file_name = f"{symbol.lower()}-ticks_{query_date}.parquet"
    output_file = CACHE_DIR / file_name

    # --- 1. 確保檔案存在 (若無則下載) ---
    if not output_file.exists():
        # (修正：若無 API Session，無法下載，直接回傳 None)
        if api is None:
            return None
            
        logging.info(f"📥 [MarketData] {symbol} {query_date} 無快取，開始下載補檔...")
        # (這裡假設 get_or_fetch_contract_ticks 會處理下載並回傳 DF)
        # (若下載回來是空的，代表當天休市，也直接回傳 None)
        df = get_or_fetch_contract_ticks(
            api=api, 
            contract=get_contract(api, symbol),
            date=str(query_date), 
            cache_file=output_file
        )
        if df.empty:
            return None

    # --- 2. 使用 DuckDB 查詢 (統一入口) ---
    try:
        # (SQL 邏輯：只讀取 close 欄位，篩選 13:46 以前，取最後一筆)
        query = f"""
            SELECT close 
            FROM '{output_file}'
            WHERE strftime(datetime, '%H:%M:%S') < '13:46:00'
            ORDER BY datetime DESC 
            LIMIT 1
        """
        result = duckdb.query(query).fetchone()
        
        if result:
            close_price = result[0]
            # (優化：改為 debug，避免洗版)
            logging.debug(f"🦆 [DuckDB] 讀取 {symbol} {query_date} 收盤價: {close_price}")
            return close_price
        
        return None 

    except Exception as e:
        logging.error(f"🔥 [DuckDB] 查詢 Parquet 失敗: {e}")
        return None


# ------------------------------------------------------------
# 📦 (Helper) 嘗試取得特定日期的收盤價
# ------------------------------------------------------------
def _attempt_fetch_close_prices(
    api, 
    query_date: date, 
    prefix: str
) -> tuple[float, float] | None:
    """
    (輔助) 嘗試取得指定日期的 TXF 與 TSE 收盤價。
    先試本地快取 (DuckDB)，失敗再試 API。
    """
    if query_date.weekday() >= 5: # 跳過週末
        return None

    # --- 1. 策略 A: 僅嘗試本地快取 (傳入 api=None) ---
    txf_local = _get_last_close(None, query_date, symbol="txf")
    tse_local = _get_last_close(None, query_date, symbol="tse")
    
    if txf_local is not None and tse_local is not None:
        logging.info(f"📂 {prefix} 本地資料: {query_date} TXF={txf_local}, TSE={tse_local}")
        return txf_local, tse_local

    # --- 2. 策略 B: 若快取失敗且有 API，嘗試 API 下載 ---
    if api is not None:
        txf_api = _get_last_close(api, query_date, symbol="txf")
        tse_api = _get_last_close(api, query_date, symbol="tse")
        
        if txf_api is not None and tse_api is not None:
            logging.info(f"🌐 {prefix} API 資料: {query_date} TXF={txf_api}, TSE={tse_api}")
            return txf_api, tse_api

    return None


# ------------------------------------------------------------
# 📦 尋找前日收盤 (主函式)
# ------------------------------------------------------------
def find_previous_close(ctx: RunContext, api=None, max_lookback: int = 15) -> tuple[float, float]:
    """
    回溯最多 max_lookback 天，尋找最近一個交易日的台指期與加權指數日盤收盤價。
    """
    prefix = "[T_Data]" if ctx.real_time_mode else "[Main]"

    # --- (A) 決定回溯起始日期 ---
    current_date = ctx.trade_date
    # (日盤從 T-1 開始找；夜盤從 T 開始找)
    start_date = current_date - timedelta(days=1) if ctx.session_type == SessionType.DAY else current_date

    # --- (B) 準備 API Session ---
    # (如果外部沒給 API，就自己建立一個臨時的 Context Manager)
    # (如果外部有給，就直接用外部的)
    session_manager = shioaji_session() if api is None else None
    local_api = api
    
    # 進入 Context (如果需要)
    if session_manager:
        local_api = session_manager.__enter__()

    try:
        # --- (C) 執行回溯迴圈 ---
        search_date = start_date
        
        for _ in range(max_lookback):
            # 呼叫外部輔助函式
            result = _attempt_fetch_close_prices(local_api, search_date, prefix)
            
            if result:
                return result # 找到了！直接回傳
            
            # 沒找到，往前推一天
            search_date -= timedelta(days=1)

        # --- (D) 若都找不到 ---
        error_msg = f"❌ {prefix} 在過去 {max_lookback} 天内找不到 TXF / TSE 收盤價。"
        logging.error(error_msg)
        raise FileNotFoundError(error_msg)

    finally:
        # --- (E) 資源清理 ---
        # 離開 Context (如果是由我們建立的)
        if session_manager:
            session_manager.__exit__(None, None, None)