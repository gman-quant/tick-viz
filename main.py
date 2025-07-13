# # main.py

# # === Standard Library ===
# import asyncio
# import os
# from datetime import date, datetime, timezone, timedelta

# # === Third-Party Libraries ===
# from aiohttp import web
# from confluent_kafka import Consumer, TopicPartition

# # === Local Application Imports ===
# import config
# from src.data_sourcing import fetch_ticks, market_data
# from src.processing import volume_bars
# from src.utils.resource_contexts import kafka_consumer
# from src.utils.session_time import get_trading_session, is_day_session
# from src.visualization import main_chart, candlestick_chart, report_generator, stats_table
# from src.web.app_factory import init_app
# from src.web.websocket_handler import websocket_handler, notify_clients


# # clients = set()

# # # WebSocket handler
# # async def websocket_handler(request):
# #     ws = web.WebSocketResponse()
# #     await ws.prepare(request)
# #     clients.add(ws)
# #     print("📡 WebSocket client connected")
# #     try:
# #         async for msg in ws:
# #             pass  # 這裡目前沒用到前端訊息
# #     finally:
# #         clients.remove(ws)
# #         print("📡 WebSocket client disconnected")
# #     return ws

# # async def notify_clients():
# #     if clients:
# #         await asyncio.gather(*[ws.send_str("reload") for ws in clients])

# # 資料抓取與報告生成的主迴圈
# async def data_loop():
#     real_time_mode = config.IS_REALTIME_MODE
#     date_source = config.DATA_SOURCE

#     consumer = None
#     current_offsets = None

#     if real_time_mode or date_source == "kafka":
#         # Kafka Consumer 建立
#         consumer = Consumer({
#             'bootstrap.servers': config.KAFKA_BROKER,
#             'group.id': config.KAFKA_GROUP_ID,
#             'enable.auto.commit': False,
#             'enable.partition.eof': True
#         })

#     start_dt_utc = config.START_DATETIME.astimezone(timezone.utc)
#     timestamp_ms = int(start_dt_utc.timestamp() * 1000)
#     metadata = consumer.list_topics(config.KAFKA_TOPIC)
#     partitions = list(metadata.topics[config.KAFKA_TOPIC].partitions.keys())
#     topic_partitions = [TopicPartition(config.KAFKA_TOPIC, p, timestamp_ms) for p in partitions]
#     fixed_offsets = consumer.offsets_for_times(topic_partitions)
#     current_offsets = fixed_offsets.copy()
#     if real_time_mode:
#         dt_now = datetime.now(tz=config.TAIWAN_TZ)
#         trade_date = dt_now.date()
#         day_session = is_day_session(dt_now.time())
#         tz = config.TAIWAN_TZ
#         start_dt, end_dt = get_trading_session(trade_date, day_session, real_time_mode, tz)
#         config.DATE = trade_date
#         config.DAY_SESSION = day_session
#         config.START_DATETIME = start_dt
#         config.END_DATETIME = end_dt
#         config.VOLUME_PER_BAR = volume_bars.get_volume_per_bar(day_session)

#     mode_info       = f"模式: {'即時動態' if real_time_mode else '歷史回顧'}"
#     session_info    = f"🕒 交易日期: {config.START_DATETIME.date()}({'日盤' if config.DAY_SESSION else '夜盤'})"
#     time_range_info = f"🔄 資料區間: {config.START_DATETIME} ~ {config.END_DATETIME}"

#     txf_prev_close, taiex_prev_close = market_data.find_previous_close()

#     df = None
#     tick_list = []

#     print("✅ 初始化完成。\n")

#     try:
#         while True:
#             if config.CLEAR_SCREEN_EACH_CYCLE:
#                 os.system('cls' if os.name == 'nt' else 'clear')
            
#             # Print Information
#             print(mode_info)
#             print(session_info)
#             print(time_range_info)

#             try:
#                 if real_time_mode or date_source == "kafka":
#                     df, current_offsets = fetch_ticks.fetch_ticks_from_kafka(
#                         consumer=consumer, 
#                         offsets=current_offsets, 
#                         start_datetime=config.START_DATETIME, 
#                         end_datetime=config.END_DATETIME, 
#                         tick_list=tick_list
#                     )
#                 else:
#                     df = fetch_ticks.fetch_ticks_from_shioaji(
#                         api_key=config.SHIOAJI_API_KEY, 
#                         secret_key=config.SHIOAJI_SECRET_KEY,
#                     )

#                 if not df.empty:
#                     print("📊 資料獲取完畢，準備處理與繪圖...\n")

#                     df_vol_kbars = volume_bars.generate_volume_bars(df, volume_per_bar=config.VOLUME_PER_BAR)
#                     stats_html = stats_table.generate_stats_html(df)
#                     fig_candlestick = candlestick_chart.plot_candlestick_with_volume_delta(df_vol_kbars)
#                     fig_main_analysis = main_chart.create_tick_analysis_figure(df, txf_prev_close, taiex_prev_close)

#                     output_file = config.OUTPUT_DIR / f"{config.REPORT_TITLE}.html"
#                     report_generator.generate_html_report(
#                         figures=[fig_candlestick, fig_main_analysis],
#                         stats_html=stats_html,
#                         output_path=output_file,
#                         report_title=config.REPORT_TITLE,
#                     )
#                     # 通知前端自動更新
#                     if config.IS_REALTIME_MODE:
#                         await notify_clients()
#                 else:
#                     print("⚠️ 沒有新資料，請確認時間或來源。")

#             except Exception as e:
#                 print(f"❌ 發生錯誤: {e}")

#             if not real_time_mode or dt_now >= config.END_DATETIME:
#                 break

#             await asyncio.sleep(config.UPDATE_INTERVAL)
#     finally:
#         if consumer:
#             consumer.close()

# # async def init_app():
# #     app = web.Application()
# #     app.router.add_get('/ws', websocket_handler)
# #     # 靜態檔案路徑，對應 output 資料夾
# #     app.router.add_static('/', path=str(config.OUTPUT_DIR.resolve()), name='static')
# #     return app

# async def main(real_time_mode: bool = 0):
#     config.IS_REALTIME_MODE = real_time_mode

#     if config.IS_REALTIME_MODE:
#         # ✅ 即時模式：啟動 Web + 單日報告
#         app = await init_app()
#         runner = web.AppRunner(app)
#         await runner.setup()
#         site = web.TCPSite(runner, 'localhost', 8080)
#         await site.start()
#         await data_loop()
#     else:
#         # ✅ 歷史模式：跑一段時間區間
#         start_date = date(2025, 6, 23)
#         end_date   = date(2025, 6, 27)
#         # day_session = 0
#         delta = timedelta(days=1)
#         tz = config.TAIWAN_TZ

#         current = start_date
#         while current <= end_date:
#             if current.weekday() >= 5:
#                 print(f"⏩ 跳過週末：{current}")
#                 current += delta
#                 continue

#             print(f"\n📅 處理日期：{current}")
#             # 跑全盤
#             for day_session in range(2):

#                 # 動態切換 config
#                 start_dt, end_dt = get_trading_session(current, day_session, False, tz)

#                 config.DATE = current
#                 config.DAY_SESSION = day_session
#                 config.START_DATETIME = start_dt
#                 config.END_DATETIME = end_dt
#                 config.VOLUME_PER_BAR = volume_bars.get_volume_per_bar(day_session)

#                 # ✅ 動態設定報告名稱
#                 session_flag = "1" if day_session else "2"
#                 config.REPORT_TITLE = f"TXF-Charts_{session_flag}_{current.strftime('%Y-%m-%d')}_{config.DATA_SOURCE}"

#                 await data_loop()

#             current += delta

# if __name__ == "__main__":
#     asyncio.run(main())



# main.py
import asyncio
from datetime import date, datetime, timedelta, timezone

from aiohttp import web
from confluent_kafka import TopicPartition

import config
from src.utils.session_time import get_trading_session, is_day_session
from src.utils.resource_contexts import kafka_consumer
from src.web.app_factory import init_app
from src.processing.main_process import process_market_session
from src.processing.volume_bars import get_volume_per_bar


async def data_loop():
    real_time_mode = config.IS_REALTIME_MODE
    date_source = config.DATA_SOURCE

    if real_time_mode or date_source == "kafka":
        with kafka_consumer() as consumer:
            # Kafka offset 初始化
            start_dt_utc = config.START_DATETIME.astimezone(timezone.utc)
            timestamp_ms = int(start_dt_utc.timestamp() * 1000)
            metadata = consumer.list_topics(config.KAFKA_TOPIC)
            partitions = list(metadata.topics[config.KAFKA_TOPIC].partitions.keys())
            topic_partitions = [TopicPartition(config.KAFKA_TOPIC, p, timestamp_ms) for p in partitions]
            fixed_offsets = consumer.offsets_for_times(topic_partitions)
            current_offsets = fixed_offsets.copy()

            await process_market_session(consumer, current_offsets, real_time_mode, date_source)
    else:
        await process_market_session(None, None, real_time_mode, date_source)


async def main(real_time_mode: bool = 0):
    config.IS_REALTIME_MODE = real_time_mode

    if config.IS_REALTIME_MODE:
        config.DATA_SOURCE = "kafka"

        app = await init_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', 8080)
        await site.start()

        dt_now = datetime.now(tz=config.TAIWAN_TZ)
        trade_date = dt_now.date()
        day_session = is_day_session(dt_now.time())
        start_dt, end_dt = get_trading_session(trade_date, day_session, True, config.TAIWAN_TZ)

        config.DATE = trade_date
        config.DAY_SESSION = day_session
        config.START_DATETIME = start_dt
        config.END_DATETIME = end_dt
        config.VOLUME_PER_BAR = get_volume_per_bar(day_session)

        await data_loop()

    else:
        config.DATA_SOURCE = "kafka"

        start_date = date(2025, 7, 11)
        end_date = date(2025, 7, 11)
        delta = timedelta(days=1)
        tz = config.TAIWAN_TZ

        current = start_date
        while current <= end_date:
            if current.weekday() >= 5:
                print(f"⏩ 跳過週末：{current}")
                current += delta
                continue

            print(f"\n📅 處理日期：{current}")
            for day_session in range(0, 1+1):
                start_dt, end_dt = get_trading_session(current, day_session, False, tz)

                config.DATE = current
                config.DAY_SESSION = day_session
                config.START_DATETIME = start_dt
                config.END_DATETIME = end_dt
                config.VOLUME_PER_BAR = get_volume_per_bar(day_session)

                session_flag = "1" if day_session else "2"
                config.REPORT_TITLE = f"TXF-Charts_{session_flag}_{current.strftime('%Y-%m-%d')}_{config.DATA_SOURCE}"

                await data_loop()

            current += delta


if __name__ == "__main__":
    asyncio.run(main())
