# src/utils/resource_contexts.py

# Standard Library Imports
from contextlib import contextmanager

# Third-Party Imports
from confluent_kafka import Consumer
import shioaji as sj

# Local Application Imports
import config.config as config

# ------------------------------------------------------------
# 📦 Shioaji API 連線管理
# ------------------------------------------------------------
@contextmanager
def shioaji_session():
    """
    使用 Context Manager 安全地處理 Shioaji API 登入與登出。
    """
    api = sj.Shioaji(simulation=True)
    try:
        api.login(api_key=config.SHIOAJI_API_KEY, secret_key=config.SHIOAJI_SECRET_KEY)
        yield api
    finally:
        api.logout()

# ------------------------------------------------------------
# 📦 Kafka 消費者連線管理 (針對 Producer 優化)
# ------------------------------------------------------------
@contextmanager
def kafka_consumer():
    """
    使用 Context Manager 安全地處理 Kafka Consumer 建立與關閉。
    
    [Consumer Optimization Strategy]:
    Matching the high-throughput Producer settings.
    """
    consumer = Consumer({
        'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': config.KAFKA_GROUP_ID,
        'enable.auto.commit': False,
        'enable.partition.eof': False,
        
        # --- 1. 擴大單次抓取上限 (配合 Producer 的 batch.size=256KB + zstd) ---
        # Producer 傳送的是高壓縮的大封包，解壓後數據量巨大。
        # 將上限提至 50MB，確保 Consumer 能一次網路請求就抓回大量累積數據，
        # 避免在開盤暴量時因為封包太碎而發生網路阻塞。
        'fetch.message.max.bytes': 52428800, # 50 MB
        
        # --- 2. 擴大內部緩衝區 (配合 Producer 的 queue...=128MB) ---
        # Producer 有 128MB 的緩衝來吸收瞬間賣壓。
        # Consumer 也開啟 128MB 的 "預讀緩衝區"，讓底層 C 語言執行緒
        # 能在 Python 處理數據時，持續從網路拉取後續資料堆在記憶體中。
        'queued.max.messages.kbytes': 131072, # 128 MB
        
        # --- 3. (選擇性) 最小抓取位元組 (維持預設) ---
        # 雖然 Producer 有 linger.ms=100，但 Consumer 端我們維持預設 1 byte。
        # 確保只要 Producer 一吐出資料，Consumer 看到多少就抓多少，
        # 不人為增加額外延遲。
        'fetch.min.bytes': 1,
    })
    try:
        yield consumer
    finally:
        consumer.close()