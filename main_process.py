# src/processing/main_process.py
from datetime import datetime
from confluent_kafka import Consumer, TopicPartition
import asyncio

import config.config as config
from config.run_context import RunContext
from config.types import SessionType, DataSource
from src.data_sourcing import fetch_ticks, market_data
from src.processing import volume_bars, kbars
from src.visualization import candlestick_chart, main_chart, report_generator, stats_table
from src.utils.misc import clear_console
from src.utils.session_time import get_observation_window
from src.web.websocket_handler import notify_clients


async def process_market_session(
    consumer: Consumer | None,
    current_offsets: list[TopicPartition] | None,
    ctx: RunContext,
    api=None
):
    df = None
    tick_list = []

    mode_info       = f"模式: {'即時動態' if ctx.real_time_mode else '歷史回顧'}"
    session_info    = f"🕒 交易日期: {ctx.start_datetime.date()}({'日盤' if ctx.session_type == SessionType.DAY else '夜盤'})"
    time_range_info = f"🔄 資料區間: {ctx.start_datetime} ~ {ctx.end_datetime}"

    txf_prev_close, taiex_prev_close = market_data.find_previous_close(ctx, api)

    print("✅ 初始化完成。\n")

    while True:
        if config.CLEAR_SCREEN_EACH_CYCLE:
            clear_console()

        print(mode_info)
        print(session_info)
        print(time_range_info)

        try:
            if ctx.real_time_mode or ctx.data_source == DataSource.KAFKA:
                df, current_offsets = fetch_ticks.fetch_ticks_from_kafka(
                    consumer=consumer,
                    offsets=current_offsets,
                    start_datetime=ctx.start_datetime,
                    end_datetime=ctx.end_datetime,
                    tick_list=tick_list,
                )
            else:
                df = fetch_ticks.fetch_ticks_from_shioaji(
                    ctx=ctx,
                    api=api,
                    tse_prev_close=taiex_prev_close
                )

            if not df.empty:
                print("📊 資料獲取完畢，準備處理與繪圖...\n")

                #df_vol_kbars = volume_bars.generate_volume_bars(df, volume_per_bar=ctx.volume_per_bar)
                #fig_candlestick = candlestick_chart.plot_candlestick_with_volume_delta(df_vol_kbars, ctx)
                stats_html = stats_table.generate_stats_html(df, txf_prev_close)
                fig_main_analysis = main_chart.create_tick_analysis_figure(df, txf_prev_close, taiex_prev_close, ctx)
                df_kbars = kbars.generate_kbars(df, period='1min', ctx=ctx)
                fig_candlestick = candlestick_chart.plot_candlestick(df_kbars, ctx)

                figures = [fig_main_analysis, fig_candlestick]
                if not ctx.real_time_mode:
                    for period in ['5min', '10min']:
                        df_kbars = kbars.generate_kbars(df, period=period, ctx=ctx)
                        fig_candlestick = candlestick_chart.plot_candlestick(df_kbars, ctx)
                        figures.append(fig_candlestick)
                
                report_generator.generate_html_report(
                    figures=figures,
                    stats_html=stats_html,
                    ctx=ctx
                )
                # 額外另外生成一份靜態報告
                if ctx.real_time_mode and ctx.auto_refresh:
                    ctx2 = ctx.with_updated(auto_refresh=False)
                    st_dt, ed_dt = get_observation_window(ctx2.start_datetime, ctx2.end_datetime, config.TAIWAN_TZ)
                    fig_candlestick.update_xaxes(range=[st_dt, ed_dt])
                    fig_main_analysis.update_xaxes(range=[st_dt, ed_dt])
                    report_generator.generate_html_report(
                        figures=[fig_main_analysis, fig_candlestick],
                        stats_html=stats_html,
                        ctx=ctx2
                    )

                if ctx.real_time_mode:
                    await notify_clients()
            else:
                print("⚠️ 沒有新資料，請確認時間或來源。")

        except Exception as e:
            print(f"❌ 發生錯誤: {e}")

        now = datetime.now(tz=config.TAIWAN_TZ)
        if not ctx.real_time_mode or now >= ctx.end_datetime:
            break

        await asyncio.sleep(config.UPDATE_INTERVAL)
