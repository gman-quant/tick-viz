# src/data_sourcing/fetch_ticks.py

# Standard Library Imports
import logging
import time
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path

# Third-Party Imports
import orjson
import pandas as pd
import shioaji as sj
from confluent_kafka import Consumer, KafkaError, TopicPartition

# Local Application Imports
from config.config import CACHE_DIR, KAFKA_POLL_TIMEOUT, UI_UPDATE_INTERVAL, TAIWAN_TZ
from config.run_context import RunContext
from config.types import SessionType
from src.data_sourcing.market_data import get_contract
from src.utils.resource_contexts import shioaji_session
from src.utils.time_parser import parse_tick_datetime


# ------------------------------------------------------------
# 📦 1. 從 Kafka 抓取 Ticks
# ------------------------------------------------------------
def fetch_ticks_from_kafka(
    consumer: Consumer,
    offsets: list[TopicPartition], 
    end_datetime: datetime
) -> tuple[pd.DataFrame | None, list]:
    """
    從 Kafka 擷取指定時間區間內的 tick 資料。
    
    (此函式由 'process_market_session' 呼叫，
     真正的「收盤」偵測 (Poll Timeout) 是由上層處理的。)
    """
    # --- 初始化：指定讀取位置與啟動計時器 ---
    consumer.assign(offsets)
    new_tick_list = []
    # (記錄開始時間，用於控制抓取時限，確保與 UI 更新同步)
    start_fetch_ts = time.time()

    try:
        # --- 進入 Kafka 輪詢迴圈 ---
        while True:
            # --- 1. 抓取訊息 ---
            msg = consumer.poll(KAFKA_POLL_TIMEOUT)

            # --- 2. 處理閒置 (Poll 超時) ---
            # (即時模式下，無新訊息時的正常出口)
            if msg is None:
                logging.info("✅ [T_Data] 輪詢等待逾時 (無新訊息)。")
                break # (回傳 None 給上層的 process_market_session)

            # --- 3. 處理 Kafka 錯誤 (含 EOF) ---
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # (偵測到 EOF)
                    logging.debug("✅ [T_Data] 讀取到 Kafka Partition 結尾 (EOF)。")
                    
                    # (檢查 Offset 是否真的沒變，若無則 'continue' 以避免日誌洗版)
                    if offsets[0].offset == msg.offset():
                        logging.debug(f"TEST 1: msg offset = {msg.offset()}")
                        continue 
                    break # (若 Offset 有變，代表有新資料，break 以回傳)
                else:
                    logging.warning(f"⚠️ [T_Data] Kafka 訊息錯誤：{msg.error()}")
                    continue

            # --- 4. 處理並過濾訊息 ---
            try:
                record = orjson.loads(msg.value())

                # --- (A) 篩選：(效能) 非模擬單，直接加入 ---
                # (起始時間 'start_datetime' 已由 'offsets_for_times' 控制)
                if not record.get('simtrade', False):
                    new_tick_list.append(record)

                    # (檢查時間限制，與 UI 同步)
                    if (time.time() - start_fetch_ts) > UI_UPDATE_INTERVAL:
                        logging.info(f"⚡ [T_Data] 達到時間限制({UI_UPDATE_INTERVAL}s)，優先回傳以更新 UI。")
                        break

                    continue # (跳過 'simtrade=True' 的時間解析)

                # --- (B) 出口：(安全網) ---
                # (僅 'simtrade=True' 的 Ticks 才會執行到這裡)
                tick_dt_taiwan = parse_tick_datetime(record.get('datetime'))
                if tick_dt_taiwan is not None and tick_dt_taiwan > end_datetime:
                    break

            except Exception as e:
                logging.warning(f"⚠️ [T_Data] JSON 解碼錯誤: {e}")
                continue
            
    except KeyboardInterrupt:
        logging.info("🛑 [T_Data] 使用者手動中止 (in fetch_ticks_from_kafka)。")
        raise
    
    # --- 5. 更新 Offsets ---
    # (嘗試取得最新位置，若無效則維持舊位置，防止 Offset 遺失)
    pos = consumer.position(offsets)
    new_offsets = pos if (pos and pos[0].offset >= 0) else offsets

    # --- 6. 處理返回結果 ---
    result_df = pd.DataFrame(new_tick_list) if new_tick_list else None
    
    return result_df, new_offsets


# ------------------------------------------------------------
# 📦 2. (Shioaji) 抓取或載入快取
# ------------------------------------------------------------
def _get_or_fetch_contract_ticks(
    api: sj.Shioaji, contract: sj.contracts.Contract, date: str, cache_file: Path
) -> pd.DataFrame:
    """
    輔助函式：若快取存在則讀取，否則從 Shioaji API 抓取並儲存快取。
    """
    # --- 1. 檢查快取 ---
    if cache_file.exists():
        return pd.read_parquet(cache_file)

    # --- 2. 若無快取，從 API 抓取 ---
    if not api:
        raise ConnectionError("Shioaji API session not available for fetching data.")

    logging.info(f"💾 [Main] Cache not found for {cache_file.name}. Fetching from API...")
    ticks = api.ticks(contract=contract, date=date)
    if not ticks['ts']:
        raise ValueError(f"No tick data found for {contract.code} on {date}.")

    # --- 3. 處理並儲存快取 ---
    df = pd.DataFrame({**ticks})
    df['ts'] = pd.to_datetime(df['ts']).dt.tz_localize(TAIWAN_TZ)
    df.rename(columns={'ts': 'datetime'}, inplace=True)
    df.to_parquet(cache_file)
    return df


# ------------------------------------------------------------
# 📦 3. (Shioaji) 歷史模式主函式
# ------------------------------------------------------------
def fetch_ticks_from_shioaji(ctx: RunContext, api, tse_prev_close: float) -> pd.DataFrame:
    """
    (歷史模式) 從 Shioaji 抓取 Tick 資料，優先使用本地快取。
    """
    try:
        # --- 1. 設定日期與快取路徑 ---
        # (夜盤 Ticks 歸屬於 T+1)
        target_date_str = str(ctx.trade_date if ctx.session_type == SessionType.DAY else (ctx.trade_date + timedelta(days=1)))
        date_str = str(ctx.trade_date)
        
        txf_file = CACHE_DIR / f"txf-ticks_{target_date_str}.parquet"
        tse_file = CACHE_DIR / f"tse-ticks_{date_str}.parquet"

        # --- 2. 獲取資料 (快取優先) ---
        if not txf_file.exists() or not tse_file.exists():
            if api is None:
                with shioaji_session() as sj_api:
                    txf_contract = get_contract(sj_api, "txf")
                    tse_contract = get_contract(sj_api, "tse")
                    df_txf = _get_or_fetch_contract_ticks(sj_api, txf_contract, target_date_str, txf_file)
                    df_tse = _get_or_fetch_contract_ticks(sj_api, tse_contract, date_str, tse_file)
            else:
                txf_contract = get_contract(api, "txf")
                tse_contract = get_contract(api, "tse")
                df_txf = _get_or_fetch_contract_ticks(api, txf_contract, target_date_str, txf_file)
                df_tse = _get_or_fetch_contract_ticks(api, tse_contract, date_str, tse_file)
        else:
            df_txf = pd.read_parquet(txf_file)
            df_tse = pd.read_parquet(tse_file)

        # --- 3. 處理與合併 (TXF & TSE) ---
        first_row_df = pd.DataFrame([df_tse.iloc[0].copy()])
        first_row_df['datetime'] = first_row_df['datetime'] - timedelta(minutes=30)
        first_row_df['close'] = tse_prev_close

        df_tse_adjusted = pd.concat([first_row_df, df_tse], ignore_index=True)
        df_tse_adjusted = df_tse_adjusted[df_tse_adjusted['datetime'].dt.time < dt_time(13, 46)] # 確保 TSE 資料只到 13:45

        df_txf['datetime'] = pd.to_datetime(df_txf['datetime']).dt.tz_convert(TAIWAN_TZ)
        df_tse_adjusted['datetime'] = pd.to_datetime(df_tse_adjusted['datetime']).dt.tz_convert(TAIWAN_TZ)

        df_merged = pd.merge_asof(
            df_txf,
            df_tse_adjusted[['datetime', 'close']],
            on='datetime',
            direction='backward',
            suffixes=('', '_TSE')
        ).set_index('datetime')

        # --- 4. 篩選盤別時間並計算指標 ---
        df_window = df_merged.loc[ctx.start_datetime : ctx.end_datetime].copy().reset_index()

        window_size = 300
        return df_window.rename(columns={'close_TSE': 'underlying_price'}).assign(
            bid_side_total_vol=lambda x: x['volume'].where(x['tick_type'] == 1, 0).cumsum(),
            ask_side_total_vol=lambda x: x['volume'].where(x['tick_type'] == 2, 0).cumsum(),
            total_volume =lambda x: x['volume'].cumsum(),
            high=lambda x: x['close'].cummax(),
            low=lambda x: x['close'].cummin(),
            avg_price=lambda x: (x['close'] * x['volume']).cumsum() / x['volume'].cumsum(),
            rvwap=lambda x: (x['close'] * x['volume']).rolling(window_size, min_periods=1).sum() /
                          x['volume'].rolling(window_size, min_periods=1).sum()
        )
    
    except Exception as e:
        logging.exception(f"❌ [Main] 歷史模式 {ctx.trade_date} {ctx.session_type.name} 獲取資料失敗: {e}")
        raise # 重新引發錯誤，讓 main_process 知道此任務失敗