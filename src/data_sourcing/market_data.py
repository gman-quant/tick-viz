# src/data_sourcing/market_data.py

# Standard Library Imports
import logging
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path

# Third-Party Imports
import pandas as pd
from confluent_kafka import TopicPartition

# Local Application Imports
from config.config import KAFKA_TOPIC, TAIWAN_TZ
from config.run_context import RunContext
from config.types import SessionType
from src.data_sourcing import fetch_ticks
from src.utils.resource_contexts import kafka_consumer, shioaji_session


# ------------------------------------------------------------
# 📦 合約物件
# ------------------------------------------------------------
def get_contract(api, symbol: str):
    """取得 Shioaji 合約物件"""
    if symbol == "txf":
        return api.Contracts.Futures.TXF.TXFR1
    elif symbol == "tse":
        return api.Contracts.Indexs.TSE.TSE001
    else:
        raise ValueError(f"Unsupported symbol: {symbol}")

# ------------------------------------------------------------
# 📦 K 線快取 (Parquet) / API 抓取
# ------------------------------------------------------------
def load_or_fetch_kbars(api, query_date: date, symbol: str) -> pd.DataFrame:
    """
    嘗試讀取 parquet，若失敗則透過 API 抓取指定合約的 kbars 並快取。
    symbol 例：'txf' 或 'tse'
    """
    
    # --- 1. 設定快取路徑 ---
    output_dir = Path(__file__).resolve().parents[2] / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{symbol.lower()}-kbars_{query_date}.parquet"
    output_file = output_dir / file_name

    df = pd.DataFrame()
    
    # --- 2. 嘗試從快取 (Parquet) 讀取 ---
    try:
        df = pd.read_parquet(output_file)
        logging.info(f"✅ Loaded {symbol.upper()} data from {output_file}")
    except FileNotFoundError:
        logging.warning(f"⚠️ 檔案不存在：{output_file}，將跳過載入並回傳空 DataFrame。")
    except pd.errors.EmptyDataError:
        logging.warning(f"⚠️ 檔案為空：{output_file}，將回傳空 DataFrame。")
    except Exception as e:
        logging.error(f"⚠️ 讀取 {output_file} 時發生未預期錯誤：{e}，將回傳空 DataFrame。")

    # --- 3. 若快取存在或 API 未提供，則返回 ---
    if not df.empty or api is None:
        return df

    # --- 4. 若快取讀取失敗，從 API 抓取 ---
    logging.info(f"🔔 Fetching {symbol.upper()} kbars from API for {query_date}...")

    contract = get_contract(api, symbol)
    kbars = api.kbars(
        contract=contract,
        start=str(query_date),
        end=str(query_date)
    )

    df = pd.DataFrame({**kbars})
    if df.empty:
        logging.error(f"❌ API 回傳空資料，無法取得 {symbol.upper()} {query_date} 的 kbars")
        return df

    # --- 5. 處理 API 資料並儲存快取 ---
    df['ts'] = pd.to_datetime(df['ts'])
    df.rename(columns={'ts': 'datetime'}, inplace=True)

    df.to_parquet(output_file)
    logging.info(f"💾 Saved {symbol.upper()} kbars to {output_file}")
    return df


def _get_last_close(api, query_date: date, symbol: str) -> float | None:
    """(輔助) 從指定合約與日期抓日盤收盤價（若無資料回傳 None）"""
    
    # --- 1. 讀取 K 線資料 (快取優先) ---
    df = load_or_fetch_kbars(api, query_date, symbol)
    if df.empty:
        return None
        
    # --- 2. 篩選日盤並回傳收盤價 ---
    day_session_df = df[df['datetime'].dt.time < dt_time(13, 46)]
    return day_session_df['Close'].iloc[-1] if not day_session_df.empty else None


# ------------------------------------------------------------
# 📦 從 Kafka 尋找前日收盤
# ------------------------------------------------------------
def find_previous_close_from_kafka(ctx: RunContext, max_lookback: int = 15) -> tuple[float, float] | None:
    """
    嘗試透過 Kafka tick 資料查詢最近 max_lookback 天內的台指期收盤價與標的指數價格。
    回傳 (TXF close, underlying price) 或 None。
    """
    
    # --- 1. 決定回查的起始日期 ---
    if ctx.real_time_mode:
        dt_now = datetime.now(TAIWAN_TZ)
        now_time = dt_now.time()
        now_date = dt_now.date()
        # (若在 13:45 前，基準日 T-1；否則為 T)
        pre_date = now_date - timedelta(days=1) if now_time < dt_time(13, 45) else now_date
    else:
        # (歷史模式)
        pre_date = ctx.trade_date - timedelta(days=1) if ctx.session_type == SessionType.DAY else ctx.trade_date

    # --- 2. 建立 Kafka 連線並設定分區 ---
    with kafka_consumer() as consumer:
        metadata = consumer.list_topics(KAFKA_TOPIC)
        partitions = list(metadata.topics[KAFKA_TOPIC].partitions.keys())

        # --- 3. 進入回溯迴圈 (最多 max_lookback 天) ---
        for _ in range(max_lookback):
            if pre_date.weekday() >= 5: # 跳過週末
                pre_date -= timedelta(days=1)
                continue

            # --- 4. 取得該日 13:29 的 Kafka Offset ---
            start_datetime = datetime.combine(pre_date, dt_time(13, 29)).replace(tzinfo=TAIWAN_TZ)
            end_datetime = datetime.combine(pre_date, dt_time(13, 46)).replace(tzinfo=TAIWAN_TZ)

            timestamp_ms = int(start_datetime.astimezone(timezone.utc).timestamp() * 1000)
            topic_partitions = [TopicPartition(KAFKA_TOPIC, p, timestamp_ms) for p in partitions]
            fixed_offsets = consumer.offsets_for_times(topic_partitions)
            fixed_offsets = [o for o in fixed_offsets if o is not None]
            
            if not fixed_offsets:
                pre_date -= timedelta(days=1)
                continue

            # --- 5. 抓取 Ticks 並尋找最後一筆 ---
            df, _ = fetch_ticks.fetch_ticks_from_kafka(
                consumer=consumer,
                offsets=fixed_offsets,
                end_datetime=end_datetime,
            )

            if df is not None and not df.empty:
                txf_close, tse_close = df.iloc[-1]['close'], df.iloc[-1]['underlying_price']
                logging.info(f"📉 從 Kafka 獲取前收資料: {pre_date} TXF={txf_close}, TSE={tse_close}")
                return txf_close, tse_close

            pre_date -= timedelta(days=1)

    # --- 6. 若所有天數都找不到 ---
    return None

# ------------------------------------------------------------
# 📦 尋找前日收盤 (主函式)
# ------------------------------------------------------------
def find_previous_close(ctx: RunContext, api=None, max_lookback: int = 15) -> tuple[float, float]:
    """
    回溯最多 max_lookback 天，依序嘗試從 Kafka、本地 parquet、API 查詢，
    尋找最近一個交易日的台指期與加權指數日盤收盤價。
    """
    prefix = "[T_Data]" if ctx.real_time_mode else "[Main]"

    # --- (A) 優先嘗試從 Kafka 尋找 (新方法) ---
    if ctx.trade_date >= date(2025, 7, 10):
        result = find_previous_close_from_kafka(ctx, max_lookback)
        if result:
            return result

    # --- (B) Kafka 失敗，定義回溯輔助函式 (舊方法：Parquet / API) ---
    def _try_get_close(query_date: date, api) -> tuple[float, float] | None:
        """(輔助) 嘗試從 Parquet 快取或 API 取得收盤價"""
        if query_date.weekday() >= 5: # 跳過週末
            return None

        # 1. 僅嘗試本地快取 (api=None)
        txf_close = _get_last_close(None, query_date, symbol="txf")
        tse_close = _get_last_close(None, query_date, symbol="tse")
        if txf_close is not None and tse_close is not None:
            logging.info(f"📂 {prefix} 本地資料: {query_date} TXF={txf_close}, TSE={tse_close}")
            return txf_close, tse_close

        # 2. 若快取失敗且有 API，嘗試 API
        if api is not None:
            txf_close = _get_last_close(api, query_date, symbol="txf")
            tse_close = _get_last_close(api, query_date, symbol="tse")
            if txf_close is not None and tse_close is not None:
                logging.info(f"🌐 {prefix} API 資料: {query_date} TXF={txf_close}, TSE={tse_close}")
                return txf_close, tse_close

        return None

    def _lookup_all(api, start_date: date) -> tuple[float, float] | None:
        """(輔助) 執行回溯迴圈"""
        query_date = start_date
        for _ in range(max_lookback):
            result = _try_get_close(query_date, api)
            if result:
                return result
            query_date -= timedelta(days=1)
        return None

    # --- (C) 決定回溯起始日期 ---
    current_date = ctx.trade_date
    session_type = ctx.session_type
    
    # (日盤 T-1；夜盤 T)
    start_date = current_date - timedelta(days=1) if session_type == SessionType.DAY else current_date

    # --- (D) 執行回溯查找 ---
    if api is None:
        with shioaji_session() as api_session:
            result = _lookup_all(api_session, start_date)
    else:
        result = _lookup_all(api, start_date)

    # --- (E) 處理最終結果 ---
    if result:
        return result

    error_msg = f"❌ {prefix} 在過去 {max_lookback} 天内找不到 TXF / TSE 收盤價。"
    logging.error(error_msg)
    raise FileNotFoundError(error_msg)