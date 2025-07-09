# main.py

# === Standard Library ===
import asyncio
import os
from datetime import datetime, timezone, timedelta

# === Third-Party Libraries ===
from aiohttp import web
from confluent_kafka import Consumer, TopicPartition

# === Local Application Imports ===
import config
from src.data_sourcing import fetch_ticks, market_data
from src.processing import volume_bars
from src.visualization import main_chart, candlestick_chart, report_generator, stats_table
from src.utils.session_time import get_trading_session, is_day_session

clients = set()

# WebSocket handler
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    clients.add(ws)
    print("📡 WebSocket client connected")
    try:
        async for msg in ws:
            pass  # 這裡目前沒用到前端訊息
    finally:
        clients.remove(ws)
        print("📡 WebSocket client disconnected")
    return ws

async def notify_clients():
    if clients:
        await asyncio.gather(*[ws.send_str("reload") for ws in clients])

# 資料抓取與報告生成的主迴圈
async def data_loop():
    # Kafka Consumer 建立
    consumer = Consumer({
        'bootstrap.servers': config.KAFKA_BROKER,
        'group.id': config.KAFKA_GROUP_ID,
        'enable.auto.commit': False,
        'enable.partition.eof': True
    })

    start_dt_utc = config.START_DATETIME.astimezone(timezone.utc)
    timestamp_ms = int(start_dt_utc.timestamp() * 1000)
    metadata = consumer.list_topics(config.KAFKA_TOPIC)
    partitions = list(metadata.topics[config.KAFKA_TOPIC].partitions.keys())
    topic_partitions = [TopicPartition(config.KAFKA_TOPIC, p, timestamp_ms) for p in partitions]
    fixed_offsets = consumer.offsets_for_times(topic_partitions)
    current_offsets = fixed_offsets.copy()

    txf_prev_close, taiex_prev_close = market_data.find_previous_close()

    df = None
    tick_list = []

    print("✅ 初始化完成。\n")

    while True:
        if config.CLEAR_SCREEN_EACH_CYCLE:
            os.system('cls' if os.name == 'nt' else 'clear')
        
        day_session = config.DAY_SESSION

        print(f"模式: {'當盤即時動態' if config.IS_REALTIME_MODE else '歷史資料'}")

        if config.IS_REALTIME_MODE:
            dt_now = datetime.now(tz=config.TAIWAN_TZ)
            now_date, now_time = dt_now.date(), dt_now.time()
            day_session = is_day_session(now_time)
            
            start_dt, end_dt = get_trading_session(now_date, day_session, config.IS_REALTIME_MODE, config.TAIWAN_TZ)
            config.START_DATETIME = start_dt
            config.END_DATETIME = end_dt

        if day_session is not None:
            print(f"🕒 時段判斷: {'日盤' if day_session else '夜盤'}")
            print(f"🔄 資料區間: {config.START_DATETIME} ~ {config.END_DATETIME}")

        try:
            if True:#config.IS_REALTIME_MODE:
                df, current_offsets = fetch_ticks.fetch_ticks_from_kafka(
                    consumer=consumer, 
                    offsets=current_offsets, 
                    start_datetime=config.START_DATETIME, 
                    end_datetime=config.END_DATETIME, 
                    tick_list=tick_list
                )
            else:
                df = fetch_ticks.fetch_ticks_from_shioaji(
                    api_key=config.SHIOAJI_API_KEY, 
                    secret_key=config.SHIOAJI_SECRET_KEY,
                )

            if not df.empty:
                print("📊 資料獲取完畢，準備處理與繪圖...\n")

                df_vol_kbars = volume_bars.generate_volume_bars(df, volume_per_bar=config.VOLUME_PER_BAR)
                stats_html = stats_table.generate_stats_html(df)
                fig_candlestick = candlestick_chart.plot_candlestick_with_volume_delta(df_vol_kbars)
                fig_main_analysis = main_chart.plot_tick_analysis(df, txf_prev_close, taiex_prev_close)

                output_file = config.OUTPUT_DIR / f"{config.REPORT_TITLE}.html"
                report_generator.generate_html_report(
                    figures=[fig_candlestick, fig_main_analysis],
                    stats_html=stats_html,
                    output_path=output_file,
                    report_title=config.REPORT_TITLE,
                )
                # 通知前端自動更新
                if config.IS_REALTIME_MODE:
                    await notify_clients()
            else:
                print("⚠️ 沒有新資料，請確認時間或來源。")

        except Exception as e:
            print(f"❌ 發生錯誤: {e}")

        if not config.IS_REALTIME_MODE or dt_now >= config.END_DATETIME:
            break

        await asyncio.sleep(config.UPDATE_INTERVAL)

async def init_app():
    app = web.Application()
    app.router.add_get('/ws', websocket_handler)
    # 靜態檔案路徑，對應 output 資料夾
    app.router.add_static('/', path=str(config.OUTPUT_DIR.resolve()), name='static')
    return app

async def main():
    if config.IS_REALTIME_MODE:
        app = await init_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', 8080)
        await site.start()
    # 同時執行資料迴圈
    await data_loop()

if __name__ == "__main__":
    asyncio.run(main())
