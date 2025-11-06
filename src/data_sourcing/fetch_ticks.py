# src/data_sourcing/fetch_ticks.py (v2, 統一使用 logging)

# Standard Library Imports
import logging
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path

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
    offsets: list,
    start_datetime: datetime,
    end_datetime: datetime
) -> tuple[pd.DataFrame, list]:
    """
    從 Kafka 擷取指定時間區間內的 tick 資料。
    
    【重構】:
    - 移除 tick_list 參數。
    - 僅返回本次輪詢抓取到的 "新" ticks (new_df)。
    - 移除所有DataFrame的後處理 (to_datetime, rvwap)，交由主流程 (main_process) 統一處理。
    """
    consumer.assign(offsets)

    finished = False
    new_tick_list = [] 

    try:
        while not finished:
            try:
                msg = consumer.poll(FETCH_INTERVAL)
            except Exception as e:
                logging.error(f"⚠️ [T_Data] Kafka polling error: {e}")
                break

            if msg is None:
                finished = True
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    finished = True
                    break
                else:
                    logging.warning(f"⚠️ [T_Data] Kafka 訊息錯誤：{msg.error()}")
                    continue

            # 解析 JSON
            try:
                record = orjson.loads(msg.value())
            except Exception as e:
                logging.warning(f"⚠️ [T_Data] JSON 解碼錯誤: {e}")
                continue

            tick_dt_taiwan = parse_tick_datetime(record.get('datetime'))
            if tick_dt_taiwan is None:
                continue

            if tick_dt_taiwan > end_datetime:
                finished = True
                break

            if start_datetime <= tick_dt_taiwan <= end_datetime and not record.get('simtrade', False):
                new_tick_list.append(record) 

    except KeyboardInterrupt:
        logging.info("🛑 [T_Data] 使用者手動中止 (in fetch_ticks_from_kafka)。")
        raise

    # 只轉換本次抓到的 "新" Ticks
    df = pd.DataFrame(new_tick_list)

    if not df.empty:
        # (這個訊息可能會洗版，如果您覺得太吵，可以註解掉)
        logging.info(f"✅ [T_Data] 本次輪詢取得 {len(df)} 筆新資料")

    # 更新 offsets，推進到下一個 offset
    positions = consumer.position(offsets)
    new_offsets = []
    for pos in positions:
        if pos.offset >= 0:
            new_offsets.append(pos)
        else:
            original_tp = next(
                (tp for tp in offsets if tp.topic == pos.topic and tp.partition == pos.partition),
                None
            )
            if original_tp:
                new_offsets.append(original_tp)

    return df, new_offsets


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

