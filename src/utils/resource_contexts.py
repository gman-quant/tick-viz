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
# 📦 Kafka 消費者連線管理
# ------------------------------------------------------------
@contextmanager
def kafka_consumer():
    """
    使用 Context Manager 安全地處理 Kafka Consumer 建立與關閉。
    """
    consumer = Consumer({
        'bootstrap.servers': config.KAFKA_BROKER,
        'group.id': config.KAFKA_GROUP_ID,
        'enable.auto.commit': False,
        'enable.partition.eof': True
    })
    try:
        yield consumer
    finally:
        consumer.close()