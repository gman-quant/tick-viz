# src/processing/main_process.py

from datetime import datetime
from confluent_kafka import Consumer, TopicPartition

import config.config as config
from config.run_context import RunContext
from config.types import SessionType, DataSource
from src.data_sourcing import fetch_ticks, market_data
from src.processing import kbars
from src.visualization import candlestick_chart, main_chart, report_generator, stats_table
from src.utils.misc import clear_console
from src.web.shared_state import shared_state


def generate_figures(df, ctx, txf_prev_close, taiex_prev_close):
    """
    生成主分析圖與各 K 線圖，返回圖表列表
    """
    figures = [
        main_chart.create_tick_analysis_figure(df, txf_prev_close, taiex_prev_close, ctx)
    ]

    for period in ['1min', '3min', '5min', '10min']:
        df_kbars = kbars.generate_kbars(df, period=period, ctx=ctx)
        figures.append(candlestick_chart.plot_candlestick(df_kbars, ctx))

    return figures


async def process_market_session(
    consumer: Consumer | None,
    current_offsets: list[TopicPartition] | None,
    ctx: RunContext,
    api=None
):
    """
    即時或歷史資料處理主流程
    """
    df = None
    tick_list = []

    # 基本資訊
    mode_info = f"模式: {'即時動態' if ctx.real_time_mode else '歷史回顧'}"
    session_info = (
        f"🕒 交易日期: {ctx.start_datetime.date()}"
        f"({'日盤' if ctx.session_type == SessionType.DAY else '夜盤'})"
    )

    txf_prev_close, taiex_prev_close = market_data.find_previous_close(ctx, api)

    print("✅ 初始化完成。\n")

    while True:
        # 清理畫面
        if config.CLEAR_SCREEN_EACH_CYCLE:
            clear_console()

        # 當前時間
        time_now = f"🕒 當前時間: {datetime.now(tz=config.TAIWAN_TZ).strftime('%H:%M:%S')}"

        # 顯示狀態
        print(mode_info)
        print(session_info)
        print(time_now)

        try:
            # 取得資料
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

            # 處理資料
            if df.empty:
                print("⚠️ 沒有新資料，請確認時間或來源。")
            else:
                print("📊 資料獲取完畢，準備處理與繪圖...\n")

                if ctx.real_time_mode:
                    # 更新共享狀態
                    with shared_state.lock:
                        shared_state.latest_df = df
                        shared_state.txf_prev_close = txf_prev_close
                        shared_state.taiex_prev_close = taiex_prev_close
                else:
                    # 生成報告
                    stats_html = stats_table.generate_stats_html(
                        stats_table.compute_stats(df, txf_prev_close)
                    )
                    figures = generate_figures(df, ctx, txf_prev_close, taiex_prev_close)
                    report_generator.generate_html_report(
                        figures=figures,
                        stats_html=stats_html,
                        ctx=ctx
                    )

        except Exception as e:
            print(f"❌ 發生錯誤: {e}")

        # 判斷是否結束
        now = datetime.now(tz=config.TAIWAN_TZ)
        if not ctx.real_time_mode or now >= ctx.end_datetime:
            break
