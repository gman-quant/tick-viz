# src/data_sourcing/market_data.py

# ------------------------------------------------------------
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

# ------------------------------------------------------------
# 📦 Persistent DuckDB Connection (效能最佳化)
# ------------------------------------------------------------
# (全域連線：避免每次查詢都重新連線，大幅提升回測速度)
_duck = duckdb.connect()


# ------------------------------------------------------------
# 🟦 (Helper) 取得某一天 13:46 前最後一筆 close
# ------------------------------------------------------------
def _get_last_close(api, query_date: date, symbol: str) -> float | None:
    """
    從快取或 API 取出某日某商品的「日盤最後一筆 close」。
    回傳 None 表示該日無資料或查詢失敗。
    """

    # --- 1. 設定快取路徑 ---
    file_name = f"{symbol.lower()}-ticks_{query_date}.parquet"
    output_file = CACHE_DIR / file_name

    # --- 2. 若無 parquet → 嘗試下載 ---
    if not output_file.exists():
        if api is None:
            return None

        logging.info(f"[MarketData] {symbol} {query_date} 無快取 → 下載中...")
        df = get_or_fetch_contract_ticks(
            api=api,
            contract=get_contract(api, symbol),
            date=str(query_date),
            cache_file=output_file
        )

        # (防呆：如果下載回來是空的，代表當天休市)
        if df.empty:
            logging.warning(f"[MarketData] {symbol} {query_date} 無資料，無法取得收盤價。")
            return None
        else:
            logging.info(f"[MarketData] {symbol} {query_date} 下載完成，已快取至 {output_file}。")
    
    # --- 3. 有 parquet 檔案，使用 DuckDB 查詢 ---
    # (SQL 邏輯：轉換時區後篩選 13:46 以前，取最後一筆)
    limit_ts = f"{query_date} 13:46:00+08:00"
    
    try:
        query = f"""
            SELECT close
            FROM read_parquet('{output_file}')
            WHERE (datetime AT TIME ZONE 'Asia/Taipei') < TIMESTAMPTZ '{limit_ts}'
            ORDER BY datetime DESC
            LIMIT 1
        """

        row = _duck.execute(query).fetchone()
        return row[0] if row else None

    except Exception as e:
        logging.error(f"🔥 DuckDB 查詢失敗: {e}")
        return None


# ------------------------------------------------------------
# 🟦 (Helper) 取得 TXF + TSE 收盤組合（快取 → API）
# ------------------------------------------------------------
def _fetch_pair_close(
    api,
    query_date: date,
    prefix: str
) -> tuple[float, float] | None:
    """
    嘗試取得當天 TXF / TSE 日盤收盤價。
    分兩階段：
      1. 優先讀取本地快取 (api=None)
      2. 若缺資料且有 API，則重新下載
    """

    # ---- 1. 先試本地快取 (只查 DB，不下載) ----
    txf = _get_last_close(None, query_date, "txf")
    tse = _get_last_close(None, query_date, "tse")

    if txf is not None and tse is not None:
        logging.info(f"📂 {prefix} 本地: {query_date} TXF={txf}, TSE={tse}")
        return txf, tse

    # ---- 2. 若需要下載且有 API ----
    if api is not None:
        txf = _get_last_close(api, query_date, "txf")
        tse = _get_last_close(api, query_date, "tse")

        if txf is not None and tse is not None:
            logging.info(f"🌐 {prefix} API: {query_date} TXF={txf}, TSE={tse}")
            return txf, tse

    return None


# ------------------------------------------------------------
# 🟥 尋找上一交易日收盤（主函式）
# ------------------------------------------------------------
def find_previous_close(
    ctx: RunContext,
    api=None,
    max_lookback: int = 15
) -> tuple[float, float]:
    """
    回溯最多 max_lookback 天，尋找最近一個交易日的
    台指期 TXF 與加權 TSE 的日盤收盤價。

    - 日盤 → 從前一天 (T-1) 開始找
    - 夜盤 → 從當天 (T) 開始找
    """

    prefix = "[T_Data]" if ctx.real_time_mode else "[Main]"

    # ---- 1. 決定起始搜尋位置 ----
    current = ctx.trade_date
    search_date = (
        current - timedelta(days=1)
        if ctx.session_type == SessionType.DAY
        else current
    )

    # ---- 2. API Session 管理 (若外部無提供則自建) ----
    session_mgr = shioaji_session() if api is None else None
    local_api = api or session_mgr.__enter__()

    try:
        # ---- 3. 回溯 max_lookback 天 ----
        for _ in range(max_lookback):

            # 跳過週末 (簡單過濾，詳細由 _get_last_close 判斷)
            if search_date.weekday() < 5:
                result = _fetch_pair_close(local_api, search_date, prefix)
                if result:
                    return result

            search_date -= timedelta(days=1)

        # ---- 4. 若都找不到 ----
        msg = f"❌ {prefix} 過去 {max_lookback} 天無 TXF / TSE 收盤資料."
        logging.error(msg)
        raise FileNotFoundError(msg)

    finally:
        # 確保自建的 Session 被關閉
        if session_mgr:
            session_mgr.__exit__(None, None, None)