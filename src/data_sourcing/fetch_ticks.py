# tick-viz/src/data_sourcing/fetch_ticks.py

import orjson
import pandas as pd
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from src.utils.time_parser import parse_tick_datetime

def fetch_ticks_from_kafka(consumer: Consumer, offsets: list, start_datetime: datetime, end_datetime: datetime, tick_dict: dict) -> tuple[pd.DataFrame, list]:
    """
    從 Kafka 擷取指定時間區間內的 tick 資料。
    """
    consumer.assign(offsets)
    print(f"🔄 從 {start_datetime} (Asia/Taipei) 開始讀取資料...")

    try:
        while True:
            msg = consumer.poll(1.0)
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
            except orjson.JSONDecodeError as e:
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

    print(f"✅ 共取得 {len(df)} 筆資料（從 {start_datetime} 到 {end_datetime}）")
    
    positions = consumer.position(offsets)
    new_offsets = [pos for pos in positions if pos.offset >= 0]
    
    return df, new_offsets