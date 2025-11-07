# src/data_sourcing/fetch_ticks.py (v2, 統一使用 logging)

# Standard Library Imports
import logging
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
import time

# Third-Party Imports
import orjson
import pandas as pd
import shioaji as sj
from confluent_kafka import Consumer, KafkaError

# Local Application Imports
from config.config import DATA_DIR, FETCH_INTERVAL, TAIWAN_TZ
from config.run_context import RunContext
from config.types import SessionType
from src.data_sourcing.market_data import get_contract
from src.utils.resource_contexts import shioaji_session
from src.utils.time_parser import parse_tick_datetime


def fetch_ticks_from_kafka(
    consumer: Consumer,
    offsets: list, # [TopicPartition]
    start_datetime: datetime,
    end_datetime: datetime
) -> tuple[pd.DataFrame | None, list]:
    """
    從 Kafka 擷取指定時間區間內的 tick 資料。
    """
    consumer.assign(offsets)

    new_tick_list = [] 

    try:
        while True:
            # === 1. 抓取訊息 ===
            msg = consumer.poll(FETCH_INTERVAL)

            # === 2. 處理閒置 (Poll 超時) - 這是「即時交易」的正常出口 ===
            if msg is None:
                break

            # === 3. 處理 Kafka 錯誤 - 這是「歷史資料」的出口 ===
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # 讀到了 Topic 的結尾，正常退出
                    logging.debug("✅ [T_Data] 讀取到 Kafka Partition 結尾 (EOF)。")
                    break
                else:
                    # 其他 Kafka 錯誤
                    logging.warning(f"⚠️ [T_Data] Kafka 訊息錯誤：{msg.error()}")
                    continue

            # === 4. 處理訊息內容 (已重新排序邏輯) ===
            try:
                record = orjson.loads(msg.value())
            except Exception as e:
                logging.warning(f"⚠️ [T_Data] JSON 解碼錯誤: {e}")
                continue

            tick_dt_taiwan = parse_tick_datetime(record.get('datetime'))

            # 優先排除無效資料
            if tick_dt_taiwan is None:
                continue
            # 篩選我們想要的資料
            if start_datetime <= tick_dt_taiwan <= end_datetime and not record.get('simtrade', False):
                new_tick_list.append(record)
                continue
            # 處理「歷史回測」的出口
            if tick_dt_taiwan > end_datetime:
                break
            
    except KeyboardInterrupt:
        logging.info("🛑 [T_Data] 使用者手動中止 (in fetch_ticks_from_kafka)。")
        raise

    # === 4. 處理返回結果 ===
    if not new_tick_list:
        return None, offsets # 保持 "舊的" offsets，下次重試
    
    # 5. 更新 offsets (單一topic，單一partition)

    # 取得「原始的」 TopicPartition (我們知道只有 1 個)
    original_tp = offsets[0]
    # 取得「最新的」 TopicPartition (我們也知道只有 1 個)
    # (注意：consumer.position() 仍然會回傳一個 list)
    pos = consumer.position(offsets)[0]
    # 檢查「最新的」是否有效
    if pos.offset >= 0:
        # 有效，更新 new_offsets 為「最新的」
        new_offsets = [pos]
    else:
        # 無效，讓 new_offsets 保持為「原始的」
        # (等同於下次重試)
        new_offsets = [original_tp]
    
    return pd.DataFrame(new_tick_list), new_offsets


# --- 抽取輔助函式，處理資料獲取與快取 ---
def _get_or_fetch_contract_ticks(
    api: sj.Shioaji, contract: sj.contracts.Contract, date: str, cache_file: Path
) -> pd.DataFrame:
    """
    Reads tick data from a cache file if it exists, otherwise fetches it
    from the Shioaji API and saves it to the cache.
    """
    if cache_file.exists():
        return pd.read_parquet(cache_file)

    if not api:
        raise ConnectionError("Shioaji API session not available for fetching data.")

    logging.info(f"💾 [Main] Cache not found for {cache_file.name}. Fetching from API...")
    ticks = api.ticks(contract=contract, date=date)
    if not ticks['ts']:
        raise ValueError(f"No tick data found for {contract.code} on {date}.")

    df = pd.DataFrame({**ticks})
    df['ts'] = pd.to_datetime(df['ts']).dt.tz_localize(TAIWAN_TZ)
    df.rename(columns={'ts': 'datetime'}, inplace=True)
    df.to_parquet(cache_file)
    return df


def fetch_ticks_from_shioaji(ctx: RunContext, api, tse_prev_close: float) -> pd.DataFrame:
    """
    Fetches and processes tick data from Shioaji, using local cache if available.
    """
    try:
        # --- 設定日期與檔案路徑 ---
        target_date_str = str(ctx.trade_date if ctx.session_type == SessionType.DAY else (ctx.trade_date + timedelta(days=1)))
        date_str = str(ctx.trade_date)
        
        txf_file = DATA_DIR / f"txf-ticks_{target_date_str}.parquet"
        tse_file = DATA_DIR / f"tse-ticks_{date_str}.parquet"

        # --- 獲取資料 (優先從快取讀取) ---
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

        # --- 資料處理與合併 ---
        first_row_df = pd.DataFrame([df_tse.iloc[0].copy()])
        first_row_df['datetime'] = first_row_df['datetime'] - timedelta(minutes=30)
        first_row_df['close'] = tse_prev_close

        df_tse_adjusted = pd.concat([first_row_df, df_tse], ignore_index=True)
        df_tse_adjusted = df_tse_adjusted[df_tse_adjusted['datetime'].dt.time < dt_time(13, 46)]

        df_txf['datetime'] = pd.to_datetime(df_txf['datetime']).dt.tz_convert(TAIWAN_TZ)
        df_tse_adjusted['datetime'] = pd.to_datetime(df_tse_adjusted['datetime']).dt.tz_convert(TAIWAN_TZ)

        df_merged = pd.merge_asof(
            df_txf,
            df_tse_adjusted[['datetime', 'close']],
            on='datetime',
            direction='backward',
            suffixes=('', '_TSE')
        ).set_index('datetime')

        # --- 篩選與計算 ---
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
        # (修改) 使用 logging.exception 記錄完整錯誤堆疊
        logging.exception(f"❌ [Main] 歷史模式 {ctx.trade_date} {ctx.session_type.name} 獲取資料失敗: {e}")
        # 重新引發錯誤，讓 main_process 知道此任務失敗
        raise

