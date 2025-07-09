# tick-viz/src/data_sourcing/fetch_ticks.py

from datetime import datetime, timedelta, time as dt_time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError
import orjson
import pandas as pd
import shioaji as sj

import config
from src.utils.time_parser import parse_tick_datetime

def fetch_ticks_from_kafka(consumer: Consumer, offsets: list, start_datetime: datetime, end_datetime: datetime, tick_dict: dict) -> tuple[pd.DataFrame, list]:
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
                tick_dict[tick_dt_taiwan] = record

    except KeyboardInterrupt:
        print("🛑 使用者手動中止。")

    df = pd.DataFrame(tick_dict.values())
    if not df.empty:
        df['datetime'] = pd.to_datetime(df['datetime'], format='ISO8601')
        df.sort_values(by='datetime', inplace=True)

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

def fetch_ticks_from_shioaji(api_key: str, secret_key: str) -> pd.DataFrame:
    """
    從 Shioaji 獲取並處理 Tick 資料。
    
    此版本邏輯：
    1. 獲取整日的 Tick 資料。
    2. 立刻篩選出 `start_datetime` 到 `end_datetime` 的區間。
    3. 僅對此區間內的資料計算累計指標 (high, low, cumsum_vol, vwap)。
    """
    target_date = config.DATE if config.DAY_SESSION else (config.DATE + timedelta(days=1))
    
    output_dir = Path(__file__).resolve().parents[2] / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    txf_file = output_dir / f"txf-ticks_{target_date}.parquet"
    tse_file = output_dir / f"tse-ticks_{config.DATE}.parquet"

    api = None
    try:
        if txf_file.exists() and tse_file.exists():
            df = pd.read_parquet(txf_file)
            df2 = pd.read_parquet(tse_file)
        else:
            api = sj.Shioaji(simulation=True)
            api.login(api_key=api_key, secret_key=secret_key)

            if txf_file.exists():
                df = pd.read_parquet(txf_file)
            else:
                ticks = api.ticks(
                    contract=api.Contracts.Futures.TXF.TXFR1,
                    date=str(target_date)
                )
                if not ticks['ts']:
                    raise ValueError("未找到指定日期的 tick 資料。請確認交易日是否正確。")
                
                df = pd.DataFrame({**ticks})
                df.ts = pd.to_datetime(df['ts']).dt.tz_localize(config.TAIWAN_TZ)
                df.rename(columns={'ts': 'datetime'}, inplace=True)
                df = df.sort_values(by='datetime')
                df.to_parquet(txf_file)

            if tse_file.exists():
                df2 = pd.read_parquet(tse_file)
            else:
                ticks = api.ticks(
                    contract=api.Contracts.Indexs.TSE.TSE001, 
                    date=str(config.DATE)
                )
                if not ticks['ts']:
                    raise ValueError("未找到指定日期的 tick 資料。請確認交易日是否正確。")
                
                df2 = pd.DataFrame({**ticks})
                df2.ts = pd.to_datetime(df2['ts']).dt.tz_localize(config.TAIWAN_TZ)
                df2.rename(columns={'ts': 'datetime'}, inplace=True)
                df2 = df2.sort_values(by='datetime')
                df2.to_parquet(tse_file)

        first_row = df2.iloc[[0]].copy()
        first_row.loc[first_row.index[0], 'datetime'] = config.START_DATETIME
        df2 = pd.concat([first_row, df2], ignore_index=True)
        # 合併
        df['datetime'] = pd.to_datetime(df['datetime']).dt.tz_convert(config.TAIWAN_TZ)
        df2['datetime'] = pd.to_datetime(df2['datetime']).dt.tz_convert(config.TAIWAN_TZ)
        df = df.sort_values(by='datetime')
        df2 = df2.sort_values(by='datetime')
        df2 = df2[df2['datetime'].dt.time < dt_time(13, 46)]
        # print(f"txf: {df}")
        # print(f"tse: {df2}")
        df = pd.merge_asof(df, df2[['datetime', 'close']], on='datetime', direction='backward', suffixes=('', '_TSE')).set_index('datetime')
        
        # 1. 先篩選：設定時間戳為索引，並使用 .loc 精確篩選出您指定的時間窗口
        df = df.loc[config.START_DATETIME : config.END_DATETIME].copy().reset_index()

        # 2. 後計算：對篩選後的 df_window 進行指標計算
        #    這樣所有 cumsum, cummax 等都會從 df 的第一筆資料（即 start_datetime 的資料）開始
        return df.rename(columns={'close_TSE': 'underlying_price'}).assign(
            bid_side_total_vol = lambda x: x['volume'].where(x['tick_type'] == 1, 0).cumsum(),
            ask_side_total_vol = lambda x: x['volume'].where(x['tick_type'] == 2, 0).cumsum(),
            high = lambda x: x['close'].cummax(),
            low = lambda x: x['close'].cummin(),
            avg_price = lambda x: (x['close'] * x['volume']).cumsum() / x['volume'].cumsum()
        )
    
    except Exception as e:
        raise RuntimeError(f"無法從 Shioaji 獲取 Tick 資料: {e}")
    
    finally:
        if api:
            api.logout()

