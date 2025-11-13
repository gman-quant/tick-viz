# src/data_sourcing/fetch_ticks.py

# Standard Library Imports
import logging
from time import time
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path

# Third-Party Imports
from orjson import loads as json_loads
import pandas as pd
import shioaji as sj
from confluent_kafka import Consumer, TopicPartition

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
    """
    # --- 1. 初始化與設定 ---
    consumer.assign(offsets)
    new_tick_list = []

    # (優化: 區域變數綁定，加速迴圈內查找)
    _parse = json_loads 
    _time = time
    
    # (優化: 預先計算「截止時間」，迴圈內只需比大小，省去減法運算)
    fetch_deadline = _time() + UI_UPDATE_INTERVAL

    try:
        while True:
            
            # --- 主動切斷機制 (與 UI 同步) ---
            # (防止資料流太快導致卡死，時間一到強制回傳資料以更新 UI)
            if _time() > fetch_deadline:
                logging.debug(f"⚡ [T_Data] 累積逾 {UI_UPDATE_INTERVAL} 秒，優先回傳資料。")
                break

            # --- 1. 抓取訊息 ---
            msg = consumer.poll(KAFKA_POLL_TIMEOUT)

            # --- 2. 處理閒置 ---
            if msg is None:
                logging.info(f"💤 [T_Data] {KAFKA_POLL_TIMEOUT} 秒內無新資料，本次無回傳。")
                break 

            # --- 3. 資料處理 (Happy Path) ---
            # (極致優化：直接解析，不先檢查 error。如果 msg 有錯，這裡會爆開進入 except)
            try:
                record = _parse(msg.value())

                # (A) 篩選：非模擬單 (Hot Path)
                # (直接 append 並 continue，跳過昂貴的時間解析)
                if not record['simtrade']:
                    new_tick_list.append(record)
                    continue 

                # (B) 模擬單 (Signal Path)
                # (只有在收到模擬單時，才花費 CPU 解析時間以判斷收盤)
                else:
                    t_dt = parse_tick_datetime(record['datetime'])
                    if t_dt is not None and t_dt > end_datetime:
                        logging.debug(f"ℹ️ [T_Data] 偵測到次盤試撮 ({t_dt})，確認已收盤。")
                        break
                    continue

            except Exception as e:
                # --- 4. 錯誤處理 (Unhappy Path) ---
                # (程式執行到這裡，代表出錯了：可能是 Kafka 錯，也可能是 JSON 錯)
                
                # (A) 檢查是否為 Kafka 系統錯誤
                kafka_err = msg.error()
                if kafka_err:
                    logging.error(f"🔥 [T_Data] Kafka 錯誤：{kafka_err}")
                    time.sleep(1) # (冷靜一下，防止 log 塞爆)
                    continue
                
                # (B) 如果不是 Kafka 錯，那就是 JSON 解析失敗
                logging.warning(f"⚠️ [T_Data] 資料解析失敗: {e}")
                continue

    except KeyboardInterrupt:
        logging.info("🛑 [T_Data] 使用者手動中止。")
        raise
    
    # 5. 更新 Offsets
    # (無論是否有資料，只要 Consumer 有移動，就必須更新 Offset)
    pos = consumer.position(offsets)
    new_offsets = pos if pos and pos[0].offset >= 0 else offsets

    # 6. 回傳
    if not new_tick_list:
        return None, new_offsets 
    return pd.DataFrame(new_tick_list), new_offsets


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