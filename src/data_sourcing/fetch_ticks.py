# tick-viz/src/data_sourcing/fetch_ticks.py


from contextlib import contextmanager
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path

import orjson
import pandas as pd
import shioaji as sj
from confluent_kafka import Consumer, KafkaError

import config
from src.utils.time_parser import parse_tick_datetime

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
        df.sort_values(by='datetime', inplace=True)
        df.drop_duplicates(inplace=True)

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


# --- 1. 使用內容管理器，確保 API 安全登入與登出 ---
@contextmanager
def shioaji_session(api_key: str, secret_key: str):
    """A context manager to safely handle Shioaji API login and logout."""
    api = None
    try:
        api = sj.Shioaji(simulation=True)
        api.login(api_key=api_key, secret_key=secret_key)
        yield api
    finally:
        if api:
            api.logout()

# --- 2. 抽取輔助函式，處理資料獲取與快取 ---
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
    df['datetime'] = pd.to_datetime(df.pop('ts')).dt.tz_localize(config.TAIWAN_TZ)
    df = df.sort_values(by='datetime').reset_index(drop=True)
    df.to_parquet(cache_file)
    return df

# --- 3. 重構後的主函式 ---
def fetch_ticks_from_shioaji(api_key: str, secret_key: str) -> pd.DataFrame:
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
        target_date_str = str(config.DATE if config.DAY_SESSION else (config.DATE + timedelta(days=1)))
        date_str = str(config.DATE)
        
        output_dir = Path(__file__).resolve().parents[2] / "data"
        output_dir.mkdir(parents=True, exist_ok=True)
        txf_file = output_dir / f"txf-ticks_{target_date_str}.parquet"
        tse_file = output_dir / f"tse-ticks_{date_str}.parquet"

        # --- 獲取資料 (優先從快取讀取) ---
        # 只有當檔案不存在時，才需要建立 API 連線
        if not txf_file.exists() or not tse_file.exists():
            with shioaji_session(api_key, secret_key) as api:
                # 建立 Shioaji Contracts 物件
                txf_contract = api.Contracts.Futures.TXF.TXFR1
                tse_contract = api.Contracts.Indexs.TSE.TSE001
                
                df_txf = _get_or_fetch_contract_ticks(api, txf_contract, target_date_str, txf_file)
                df_tse = _get_or_fetch_contract_ticks(api, tse_contract, date_str, tse_file)
        else:
            df_txf = pd.read_parquet(txf_file)
            df_tse = pd.read_parquet(tse_file)

        # --- 資料處理與合併 ---
        # 為了 merge_asof，確保 TSE 資料在起始時間有對應值
        first_row = df_tse.iloc[[0]].copy()
        first_row['datetime'] = config.START_DATETIME
        df_tse_adjusted = pd.concat([first_row, df_tse], ignore_index=True)
        
        # 統一時區並排序，準備合併
        df_txf['datetime'] = pd.to_datetime(df_txf['datetime']).dt.tz_convert(config.TAIWAN_TZ)
        df_tse_adjusted['datetime'] = pd.to_datetime(df_tse_adjusted['datetime']).dt.tz_convert(config.TAIWAN_TZ)
        
        df_merged = pd.merge_asof(
            df_txf.sort_values(by='datetime'),
            df_tse_adjusted[['datetime', 'close']].sort_values(by='datetime'),
            on='datetime',
            direction='backward',
            suffixes=('', '_TSE')
        ).set_index('datetime')
        
        # --- 篩選與計算 ---
        # 1. 先篩選出指定時間窗口
        df_window = df_merged.loc[config.START_DATETIME : config.END_DATETIME].copy().reset_index()

        # 2. 僅對此窗口內的資料計算累計指標
        return df_window.rename(columns={'close_TSE': 'underlying_price'}).assign(
            bid_side_total_vol=lambda x: x['volume'].where(x['tick_type'] == 1, 0).cumsum(),
            ask_side_total_vol=lambda x: x['volume'].where(x['tick_type'] == 2, 0).cumsum(),
            high=lambda x: x['close'].cummax(),
            low=lambda x: x['close'].cummin(),
            avg_price=lambda x: (x['close'] * x['volume']).cumsum() / x['volume'].cumsum()
        )
    
    except Exception as e:
        # 將所有可能的錯誤包裝成一個統一的 Runtime 錯誤
        raise RuntimeError(f"Failed to fetch tick data from Shioaji: {e}") from e
