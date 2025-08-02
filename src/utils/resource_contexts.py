# src/utils/resource_contexts.py
# 使用內容管理器，確保 API 安全登入與登出


from contextlib import contextmanager
from confluent_kafka import Consumer
import shioaji as sj

import config.config as config


@contextmanager
def shioaji_session():
    """A context manager to safely handle Shioaji API login and logout."""
    api = sj.Shioaji(simulation=True)
    try:
        api.login(api_key=config.SHIOAJI_API_KEY, secret_key=config.SHIOAJI_SECRET_KEY)
        yield api
    finally:
        api.logout()
        
@contextmanager
def kafka_consumer():
    """Context manager for Kafka Consumer with safe teardown."""
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

