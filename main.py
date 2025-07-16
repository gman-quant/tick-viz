# main.py


import asyncio
from datetime import date, datetime, timedelta, timezone

from aiohttp import web
from confluent_kafka import TopicPartition

import config.config as config
from config.run_context import RunContext
from config.types import SessionType, DataSource
from src.utils.session_time import in_which_session, get_session_range
from src.utils.resource_contexts import kafka_consumer, shioaji_session
from src.web.app_factory import init_app
from main_process import process_market_session



async def data_loop(ctx: RunContext, api=None):

    if ctx.real_time_mode or ctx.data_source == DataSource.KAFKA:
        with kafka_consumer() as consumer:
            # Kafka offset 初始化
            start_dt_utc = ctx.start_datetime.astimezone(timezone.utc)
            timestamp_ms = int(start_dt_utc.timestamp() * 1000)
            metadata = consumer.list_topics(config.KAFKA_TOPIC)
            partitions = list(metadata.topics[config.KAFKA_TOPIC].partitions.keys())
            topic_partitions = [TopicPartition(config.KAFKA_TOPIC, p, timestamp_ms) for p in partitions]
            fixed_offsets = consumer.offsets_for_times(topic_partitions)
            current_offsets = fixed_offsets.copy()

            await process_market_session(consumer, current_offsets, ctx)
    else:
        await process_market_session(None, None, ctx, api)


async def main(real_time_mode: bool = 1):
    ctx = RunContext(real_time_mode=real_time_mode)

    if real_time_mode:
        app = await init_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', 8080)
        await site.start()

        now_time = datetime.now(tz=config.TAIWAN_TZ).time()
        ctx = ctx.with_updated(
            trade_date=date.today(),
            session_type=in_which_session(now_time),
        )
        await data_loop(ctx)

    else:
        with shioaji_session() as api: 
            start_date = date(2025, 5, 1)
            end_date   = date(2025, 7, 16)
            pick       = 'whole' # 可選 'day'（日盤）、'night'（夜盤）、或 'whole'（日+夜）
            
            current = start_date
            one_day = timedelta(days=1)
            st, ed = get_session_range(pick)
            while current <= end_date:
                if current.weekday() >= 5:
                    print(f"⏩ 跳過週末：{current}")
                    current += one_day
                    continue

                for day_session in range(st, ed - 1, -1):
                    print(f"\n📅 處理日期：{current} - {'日盤' if day_session else '夜盤'}")
                    ctx = ctx.with_updated(
                        trade_date=current,
                        session_type=SessionType.DAY if day_session else SessionType.NIGHT,
                        data_source=DataSource.SHIOAJI
                    )
    
                    await data_loop(ctx, api)

                current += one_day


if __name__ == "__main__":
    asyncio.run(main())
