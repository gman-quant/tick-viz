# main.py


import argparse
import asyncio
from datetime import date, datetime, timedelta, timezone

from aiohttp import web
from confluent_kafka import TopicPartition

import config.config as config
from config.run_context import RunContext
from config.types import SessionType, DataSource
from src.utils.session_time import get_session_range, in_which_session
from src.utils.time_parser import parse_date
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


async def main(
        real_time_mode: bool = 1, 
        auto_refresh: bool = 1,
        date_start: str = None, 
        date_end: str = None, 
        session: str = None
):
    ctx = RunContext(real_time_mode=real_time_mode, auto_refresh=auto_refresh)

    if not ctx.real_time_mode:
        with shioaji_session() as api:
            one_day = timedelta(days=1)
            dt_st   = date_start or date.today() - one_day # 2024, 3, 14
            dt_ed   = date_end or date.today() # 2024, 3, 28
            pick    = session or 'whole' # 可選 'day'（日盤）、'night'（夜盤）、或 'whole'（日+夜）
            
            current = dt_st
            st, ed  = get_session_range(pick)
            while current <= dt_ed:
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
    else:
        if ctx.auto_refresh:
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
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="台指期 Tick 資料處理與繪圖")
    
    parser.add_argument(
        "--real-time-mode", 
        type=int, 
        choices=[0, 1], 
        default=1,
        help="即時模式 (1=啟用, 0=停用)"
    )
    parser.add_argument(
        "--auto-refresh", 
        type=int, 
        choices=[0, 1], 
        default=1,
        help="自動重新整理 (1=啟用, 0=停用)"
    )
    parser.add_argument(
        "--date-start", 
        type=parse_date, 
        help="資料開始日期 (格式: YYYY-MM-DD)"
    )
    parser.add_argument(
        "--date-end", 
        type=parse_date, 
        help="資料結束日期 (格式: YYYY-MM-DD)"
    )
    parser.add_argument(
        "--session", 
        type=str, 
        choices=["day", "night", "whole"], 
        help="交易時段: day=日盤, night=夜盤, whole=全部"
    )

    args = parser.parse_args()

    # 呼叫 async 的 main
    asyncio.run(main(
        real_time_mode=bool(args.real_time_mode),
        auto_refresh=bool(args.auto_refresh),
        date_start=args.date_start,
        date_end=args.date_end,
        session=args.session
    ))

''' 📊 tick-viz 專案常用指令

🟢 即時更新模式
cd Projects/tick-viz && source venv/bin/activate
python main.py --auto-refresh 1 --real-time-mode 1

🟡 靜態即時模式
cd Projects/tick-viz && source venv/bin/activate
python main.py --auto-refresh 0 --real-time-mode 1

🔵 歷史回顧模式
cd Projects/tick-viz && source venv/bin/activate
python main.py --auto-refresh 0 --real-time-mode 0 --date-start 2025-08-12 --date-end 2025-08-12 --session day

 📅 日線圖更新
source venv/bin/activate && python -m src.processing.kbar.process_all_ticks_to_daily_csv
python plot_txf_kbar.py

📦 生成 requirements.txt
pip freeze > requirements.txt
'''


''' macOS launchd 代理程式管理與除錯指令

這份精簡指南涵蓋了常用的 launchctl 指令與相關檔案操作，方便你快速掌握自動啟動代理程式的狀態與問題排查。

📂 代理程式檔案位置
| Agent Label          | Script Name             | Schedule Time          | Description       
| -------------------- | ----------------------- | ---------------------- | ----------------- 
| com.garrett.tickviz  | update_daily_chart.sh   | MON - FRI 13:46        | 自動執行歷史回顧模式  
| com.garrett.tickviz2 | monitor_realtime_txf.sh | MON - FRI 08:44, 14:59 | 自動執行即時更新模式  


📜 launchctl 指令
launchctl print gui/$(id -u)/com.garrett.tickviz	                查看代理程式的狀態、日誌與退出碼。
launchctl load ~/Library/LaunchAgents/com.garrett.tickviz.plist	    啟動並載入代理程式。
launchctl unload ~/Library/LaunchAgents/com.garrett.tickviz.plist   停止並卸載代理程式。
launchctl list com.garrett.tickviz	                                列出該代理程式的狀態。
launchctl list | grep tickviz	                                    查看所有與 tickviz 相關的代理程式。
launchctl kickstart -k gui/$(id -u)/com.garrett.tickviz	            立即啟動或重新啟動代理程式。
launchctl stop gui/$(id -u)/com.garrett.tickviz	                    停止正在執行的代理程式。
launchctl start gui/$(id -u)/com.garrett.tickviz	                啟動代理程式（如果尚未啟動）。
launchctl remove com.garrett.tickviz	                            從 launchctl 中移除該代理程式（卸載前請先 unload）。

📂 .plist 檔案除錯
plutil -lint ~/Library/LaunchAgents/com.garrett.tickviz.plist   檢查 .plist 檔案的 XML 語法是否正確。
nano ~/Library/LaunchAgents/com.garrett.tickviz.plist	        編輯 .plist 檔案。

📂 腳本與日誌
nano ~/Library/Scripts/startup.sh   編輯 startup.sh 腳本的內容。
cat /tmp/tickviz.log	            查看 腳本的標準輸出日誌。
cat /tmp/tickviz.err	            查看 腳本的錯誤日誌。
'''