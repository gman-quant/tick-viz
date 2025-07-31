# src/data_sourcing/fetch_ticks.py


from datetime import datetime, timedelta, time as dt_time
from pathlib import Path

import orjson
import pandas as pd
import shioaji as sj
from confluent_kafka import Consumer, KafkaError

from config.config import DATA_DIR, TAIWAN_TZ
from config.run_context import RunContext
from config.types import SessionType
from src.data_sourcing.market_data import get_contract
from src.utils.time_parser import parse_tick_datetime
from src.utils.resource_contexts import ensure_api_session

def fetch_ticks_from_kafka(consumer: Consumer, offsets: list, start_datetime: datetime, end_datetime: datetime, tick_list: list) -> tuple[pd.DataFrame, list]:
    """
    從 Kafka 擷取指定時間區間內的 tick 資料。
    """
    consumer.assign(offsets)
    print(f"🔄 從 {start_datetime} (Asia/Taipei) 開始讀取資料...")

    try:
        while True:
            try:
                msg = consumer.poll(1.0)
            except Exception as e:
                print(f"⚠️ Kafka polling error: {e}")
                break
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                else:
                    print("⚠️ Kafka 錯誤：", msg.error())
                    continue

            try:
                record = orjson.loads(msg.value())
            except Exception as e:
                print(f"⚠️ JSON 解碼錯誤: {e}")
                continue

            tick_dt_taiwan = parse_tick_datetime(record['datetime'])
            if tick_dt_taiwan is None:
                continue

            if tick_dt_taiwan > end_datetime:
                print("⏹️ 已達目標時間，停止讀取。")
                break

            if start_datetime <= tick_dt_taiwan <= end_datetime and not record.get('simtrade', False):
                tick_list.append(record)

    except KeyboardInterrupt:
        print("🛑 使用者手動中止。")

    df = pd.DataFrame(tick_list)
    if not df.empty:
        df['datetime'] = pd.to_datetime(df['datetime'], format='ISO8601')
        df.drop_duplicates(inplace=True)
        window_size = 300  # 你可以自由調整這個數字
        df['rvwap'] = (
            (df['close'] * df['volume']).rolling(window_size, min_periods=1).sum() /
            df['volume'].rolling(window_size, min_periods=1).sum()
        )
        

    print(f"✅ 共取得 {len(df)} 筆資料")
    
    # 取得最新 consumer 位置 offsets，準備下一輪拉取用
    positions = consumer.position(offsets)
    
    new_offsets = []
    for pos in positions:
        if pos.offset >= 0:
            new_offsets.append(pos)
        else:
            # 如果 offset 無效，維持原本 offsets
            original_tp = next((tp for tp in offsets if tp.topic == pos.topic and tp.partition == pos.partition), None)
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

    print(f"Cache not found for {cache_file.name}. Fetching from API...")
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
    
    Logic:
    1. Fetch daily tick data for both TXF and TSE (from cache or API).
    2. Merge the datasets.
    3. Filter the merged data for the specific time window.
    4. Calculate cumulative metrics on the filtered window.
    """
    try:
        # --- 設定日期與檔案路徑 ---
        target_date_str = str(ctx.trade_date if ctx.session_type == SessionType.DAY else (ctx.trade_date + timedelta(days=1)))
        date_str = str(ctx.trade_date)
        
        txf_file = DATA_DIR / f"txf-ticks_{target_date_str}.parquet"
        tse_file = DATA_DIR / f"tse-ticks_{date_str}.parquet"

        # --- 獲取資料 (優先從快取讀取) ---
        # 只有當檔案不存在時，才需要建立 API 連線
        if not txf_file.exists() or not tse_file.exists():
            with ensure_api_session(api) as sj_api:
                txf_contract = get_contract(sj_api, "txf")
                tse_contract = get_contract(sj_api, "tse")
                df_txf = _get_or_fetch_contract_ticks(sj_api, txf_contract, target_date_str, txf_file)
                df_tse = _get_or_fetch_contract_ticks(sj_api, tse_contract, date_str, tse_file)
        else:
            df_txf = pd.read_parquet(txf_file)
            df_tse = pd.read_parquet(tse_file)

        # --- 資料處理與合併 ---
        # 複製第一列，並將 close 設為 tse_prev_close，時間提前 30 分鐘，確保 merge_asof 有對應值
        first_row_df = pd.DataFrame([df_tse.iloc[0].copy()])
        first_row_df['datetime'] = first_row_df['datetime'] - timedelta(minutes=30)
        first_row_df['close'] = tse_prev_close

        # 合併調整後的 df_tse 和原始 df_tse，並過濾時間早於 13:46
        df_tse_adjusted = pd.concat([first_row_df, df_tse], ignore_index=True)
        df_tse_adjusted = df_tse_adjusted[df_tse_adjusted['datetime'].dt.time < dt_time(13, 46)]

        # 統一時區並排序，準備合併
        df_txf['datetime'] = pd.to_datetime(df_txf['datetime']).dt.tz_convert(TAIWAN_TZ)
        df_tse_adjusted['datetime'] = pd.to_datetime(df_tse_adjusted['datetime']).dt.tz_convert(TAIWAN_TZ)

        # 使用 merge_asof 對齊 datetime，取最近的先前 TSE close 價
        df_merged = pd.merge_asof(
            df_txf,
            df_tse_adjusted[['datetime', 'close']],
            on='datetime',
            direction='backward',
            suffixes=('', '_TSE')
        ).set_index('datetime')

        # --- 篩選與計算 ---
        # 1. 先篩選出指定時間窗口
        df_window = df_merged.loc[ctx.start_datetime : ctx.end_datetime].copy().reset_index()

        # 2. 僅對此窗口內的資料計算累計指標
        window_size = 300  # 你可以自由調整這個數字
        return df_window.rename(columns={'close_TSE': 'underlying_price'}).assign(
            bid_side_total_vol=lambda x: x['volume'].where(x['tick_type'] == 1, 0).cumsum(),
            ask_side_total_vol=lambda x: x['volume'].where(x['tick_type'] == 2, 0).cumsum(),
            high=lambda x: x['close'].cummax(),
            low=lambda x: x['close'].cummin(),
            avg_price=lambda x: (x['close'] * x['volume']).cumsum() / x['volume'].cumsum(),
            rvwap=lambda x: (x['close'] * x['volume']).rolling(window_size, min_periods=1).sum() /
                          x['volume'].rolling(window_size, min_periods=1).sum()
        )
    
    except Exception as e:
        # 將所有可能的錯誤包裝成一個統一的 Runtime 錯誤
        raise RuntimeError(f"Failed to fetch tick data from Shioaji: {e}") from e
