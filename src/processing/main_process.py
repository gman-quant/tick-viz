# src/processing/main_process.py
from datetime import datetime
from confluent_kafka import Consumer, TopicPartition
import asyncio

import config
from src.data_sourcing import fetch_ticks, market_data
from src.processing import volume_bars
from src.visualization import candlestick_chart, main_chart, report_generator, stats_table
from src.utils.misc import clear_console
from src.web.websocket_handler import notify_clients


async def process_market_session(
    consumer: Consumer | None,
    current_offsets: list[TopicPartition] | None,
    real_time_mode: bool,
    date_source: str,
):
    df = None
    tick_list = []

    mode_info       = f"模式: {'即時動態' if real_time_mode else '歷史回顧'}"
    session_info    = f"🕒 交易日期: {config.START_DATETIME.date()}({'日盤' if config.DAY_SESSION else '夜盤'})"
    time_range_info = f"🔄 資料區間: {config.START_DATETIME} ~ {config.END_DATETIME}"

    txf_prev_close, taiex_prev_close = market_data.find_previous_close()

    print("✅ 初始化完成。\n")

    while True:
        if config.CLEAR_SCREEN_EACH_CYCLE:
            clear_console()

        print(mode_info)
        print(session_info)
        print(time_range_info)

        try:
            if real_time_mode or date_source == "kafka":
                assert consumer is not None and current_offsets is not None
                df, current_offsets = fetch_ticks.fetch_ticks_from_kafka(
                    consumer=consumer,
                    offsets=current_offsets,
                    start_datetime=config.START_DATETIME,
                    end_datetime=config.END_DATETIME,
                    tick_list=tick_list,
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
                fig_main_analysis = main_chart.create_tick_analysis_figure(df, txf_prev_close, taiex_prev_close)

                output_file = config.OUTPUT_DIR / f"{config.REPORT_TITLE}.html"
                report_generator.generate_html_report(
                    figures=[fig_candlestick, fig_main_analysis],
                    stats_html=stats_html,
                    output_path=output_file,
                    report_title=config.REPORT_TITLE,
                )

                if config.IS_REALTIME_MODE:
                    await notify_clients()
            else:
                print("⚠️ 沒有新資料，請確認時間或來源。")

        except Exception as e:
            print(f"❌ 發生錯誤: {e}")

        now = datetime.now(tz=config.TAIWAN_TZ)
        if not real_time_mode or now >= config.END_DATETIME:
            break

        await asyncio.sleep(config.UPDATE_INTERVAL)
