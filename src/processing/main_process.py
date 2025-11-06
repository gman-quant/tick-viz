# src/processing/main_process.py (v2, 統一使用 logging)

# Standard Library Imports
import logging
import time
from datetime import datetime

# Third-Party Imports
import pandas as pd 
from confluent_kafka import Consumer, TopicPartition

# Local Application Imports
import config.config as config
from config.run_context import RunContext
from config.types import DataSource, SessionType
from src.data_sourcing import fetch_ticks, market_data
from src.processing import kbars
from src.processing.metrics import prepare_plot_data 
from src.utils.misc import clear_console
from src.visualization import candlestick_chart, main_chart, report_generator, stats_table
from src.web.shared_state import shared_state


def generate_figures(df, ctx, txf_prev_close, taiex_prev_close):
    """
    生成主分析圖與各 K 線 圖，返回圖表列表
    """
    plot_df = prepare_plot_data(df, txf_prev_close, taiex_prev_close)
    
    figures = [
        main_chart.create_tick_analysis_figure(plot_df, txf_prev_close, taiex_prev_close, ctx)
    ]

    for period in ['1min', '3min', '5min', '10min']:
        df_kbars = kbars.generate_kbars(df, period=period, ctx=ctx)
        figures.append(candlestick_chart.plot_candlestick(df_kbars, ctx))

    return figures


def process_market_session(
    consumer: Consumer | None,
    current_offsets: list[TopicPartition] | None,
    ctx: RunContext,
    api=None
):
    """
    即時或歷史資料處理主流程
    """
    # (注意: main_df 在即時模式中會持續增長)
    main_df = pd.DataFrame() 

    # 基本資訊
    mode_info = f"模式: {'即時動態' if ctx.real_time_mode else '歷史回顧'}"
    session_info = (
        f"🕒 交易日期: {ctx.start_datetime.date()}"
        f"({'日盤' if ctx.session_type == SessionType.DAY else '夜盤'})"
    )

    # 2. (修改)
    logging.info(f"🔁 [T_Data] 正在 (重新) 取得 {ctx.session_type.name} 的前日收盤價...")
    txf_prev_close, taiex_prev_close = market_data.find_previous_close(ctx, api)
    
    # 3. (修改)
    logging.info("✅ [T_Data] process_market_session 初始化完成。")

    while True:
        try:
            # 取得資料
            if ctx.real_time_mode or ctx.data_source == DataSource.KAFKA:
                
                # 1. 先 poll 資料 (包含 2 秒等待)
                new_df, current_offsets = fetch_ticks.fetch_ticks_from_kafka(
                    consumer=consumer,
                    offsets=current_offsets,
                    start_datetime=ctx.start_datetime,
                    end_datetime=ctx.end_datetime,
                )
                # --- 若 new_df 為空，則跳過本次迴圈剩餘步驟 ---
                if new_df.empty:
                    continue 

                # --- (只有 new_df 不為空時才會執行到這裡) ---

                # (注意: clear_console() 在日誌導向檔案時會失效或產生亂碼)
                if config.CLEAR_SCREEN_EACH_CYCLE:
                    clear_console()

                # 3. 顯示狀態 (修改)
                time_now = f"🕒 當前時間: {datetime.now(tz=config.TAIWAN_TZ).strftime('%H:%M:%S')}"
                logging.info(f"--- {mode_info} | {session_info} | {time_now} ---")
                
                # --- 【增量處理核心】 ---
                new_df['datetime'] = pd.to_datetime(new_df['datetime'], format='ISO8601')
                main_df = pd.concat([main_df, new_df], ignore_index=True)
                main_df.drop_duplicates(inplace=True) 
                
                window_size = 300
                main_df['rvwap'] = (
                    (main_df['close'] * main_df['volume']).rolling(window_size, min_periods=1).sum()
                    / main_df['volume'].rolling(window_size, min_periods=1).sum()
                )
                
                df = main_df 
                
                # 4. (修改)
                logging.info(f"📈 資料總筆數: {len(df)} 筆。 正在更新共享狀態 (shared_state)...")
                
                plot_df = prepare_plot_data(df, txf_prev_close, taiex_prev_close)
                df_kbars_1min = kbars.generate_kbars(df, period="1min", ctx=ctx)

                # 更新共享狀態
                with shared_state.lock:
                    shared_state.latest_df = df
                    shared_state.plot_df = plot_df
                    shared_state.kbars_1min = df_kbars_1min
                    shared_state.txf_prev_close = txf_prev_close
                    shared_state.taiex_prev_close = taiex_prev_close
            
            else:
                # --------------------
                # 📘 歷史回顧模式
                # --------------------
                if config.CLEAR_SCREEN_EACH_CYCLE:
                    clear_console()

                # 5. (修改)
                time_now = f"🕒 當前時間: {datetime.now(tz=config.TAIWAN_TZ).strftime('%H:%M:%S')}"
                logging.info(f"--- {mode_info} | {session_info} | {time_now} ---")
                
                df = fetch_ticks.fetch_ticks_from_shioaji(
                    ctx=ctx,
                    api=api,
                    tse_prev_close=taiex_prev_close
                )
                
                if df is None or df.empty:
                     # 6. (修改)
                     logging.warning("⚠️ 沒有新資料，請確認時間或來源。")
                else:
                    # 7. (修改)
                    logging.info(f"📈 資料總筆數: {len(df)} 筆")
                    logging.info("📊 資料獲取完畢，準備生成靜態報告...")
                    stats_html = stats_table.generate_stats_html(
                        stats_table.compute_stats(df, txf_prev_close)
                    )
                    figures = generate_figures(df, ctx, txf_prev_close, taiex_prev_close)
                    report_generator.generate_html_report(
                        figures=figures,
                        stats_html=stats_html,
                        ctx=ctx
                    )

        except KeyboardInterrupt:
            # 8. (修改)
            logging.info("\n🛑 收到使用者中斷 (Ctrl+C)，正在結束 process_market_session...")
            break # <-- 關鍵：跳出 while True 迴圈
        
        except Exception as e:
            # 9. (修改) *** 這是最重要的修改 ***
            # logging.exception() 會自動包含完整的錯誤堆疊 (Traceback)
            logging.exception(f"❌ [T_Data] process_market_session 迴圈發生未預期錯誤: {e}")
            # (在即時模式下，我們通常不希望 break，而是 sleep 後重試)
            if ctx.real_time_mode:
                logging.info("     10 秒後嘗試繼續迴圈...")
                time.sleep(10)
            else:
                # 歷史模式發生錯誤，應中斷
                logging.error("     歷史模式發生錯誤，中斷此任務。")
                break

        # 判斷是否結束
        now = datetime.now(tz=config.TAIWAN_TZ)
        if not ctx.real_time_mode or now >= ctx.end_datetime:
            if ctx.real_time_mode:
                 logging.info(f"ℹ️ [T_Data] 已達 {ctx.session_type.name} 收盤時間 ({ctx.end_datetime})，結束任務。")
            break