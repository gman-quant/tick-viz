# src/core/session_processor.py

# Standard Library Imports
from datetime import datetime
import logging
import time

# Third-Party Imports
import pandas as pd 
from confluent_kafka import Consumer, TopicPartition

# Local Application Imports
from config.config import TAIWAN_TZ
from config.run_context import RunContext
from config.types import DataSource
from src.data_sourcing.fetch_ticks import fetch_ticks_from_kafka, fetch_ticks_from_shioaji
from src.data_sourcing.market_data import find_previous_close
from src.processing.bars.kbars import generate_kbars
from src.processing.metrics import prepare_plot_data 
from src.visualization import stats_table
from src.visualization.report_generator import generate_html_report
from src.web.shared_state import shared_state

def process_market_session(
    consumer: Consumer | None,
    current_offsets: list[TopicPartition] | None,
    ctx: RunContext,
    api=None
):
    """
    即時或歷史資料處理主流程。
    - 即時模式 (real_time_mode=True): 進入 Kafka 輪詢迴圈。
    - 歷史模式 (real_time_mode=False): 執行一次 Shioaji/Kafka 資料抓取與報告生成。
    """
    main_df = pd.DataFrame() 

    # --- 1. 初始化：取得前日收盤價 ---
    logging.info(f"🔁 [T_Data] 正在取得 {ctx.session_type.name} 的前日收盤價...")
    txf_prev_close, taiex_prev_close = find_previous_close(ctx, api)

    with shared_state.lock:
        shared_state.txf_prev_close = txf_prev_close
        shared_state.taiex_prev_close = taiex_prev_close
    
    logging.info("✅ [T_Data] process_market_session 初始化完成。")

    # --- 2. 進入資料處理迴圈 ---
    # (即時模式下，此迴圈會持續執行直到收盤；歷史模式下，執行一次後 break)
    while True:
        new_count = 0 # (初始化本輪新增筆數)

        try:
            # --- (A) 即時模式 / Kafka 歷史模式 ---
            if ctx.real_time_mode or ctx.data_source == DataSource.KAFKA:
                
                # --- 抓取新 Ticks ---
                new_df, current_offsets = fetch_ticks_from_kafka(
                    consumer=consumer,
                    offsets=current_offsets,
                    end_datetime=ctx.end_datetime,
                    real_time_mode=ctx.real_time_mode
                )
                
                # --- 檢查收盤或無資料 ---
                if new_df is None:
                    if datetime.now(TAIWAN_TZ) >= ctx.end_datetime:
                        logging.info(f"🔔 [T_Data] {ctx.session_type.name} 收盤時間已到，結束任務。")
                        break  # 收盤時間到，結束任務
                    continue   # 無新資料，繼續輪詢

                new_count = len(new_df)

                # --- 即時增量處理 ---
                new_df['datetime'] = pd.to_datetime(new_df['datetime'], format='ISO8601')
                main_df = pd.concat([main_df, new_df], ignore_index=True)
                # main_df.drop_duplicates(inplace=True) # 確保唯一性
                
                # --- 計算指標 ---
                tick_sma_window = shared_state.param_tick_window
                time_sma_window = shared_state.param_time_window
                # main_df['rvwap'] = (
                #     (main_df['close'] * main_df['volume']).rolling(tick_sma_window, min_periods=1).sum()
                #     / main_df['volume'].rolling(tick_sma_window, min_periods=1).sum()
                # )
                main_df['sma'] = main_df.rolling(
                    tick_sma_window, 
                    on='datetime', 
                    min_periods=1
                )['close'].mean()
                main_df['sma2'] = main_df.rolling(
                    time_sma_window, 
                    on='datetime', 
                    min_periods=1
                )['close'].mean()
                
                
                df = main_df # 將處理完的 main_df 指派給 df
                
                # --- 更新共用狀態 (Web/Dash) ---
                plot_df = prepare_plot_data(df, txf_prev_close, taiex_prev_close)
                df_kbars_1min = generate_kbars(df, period="1min", ctx=ctx)

                with shared_state.lock:
                    shared_state.latest_df = df
                    shared_state.plot_df = plot_df
                    shared_state.kbars_1min = df_kbars_1min
            
            # --- (B) Shioaji 歷史模式 ---
            else:
                df = fetch_ticks_from_shioaji(
                    ctx=ctx,
                    api=api,
                    tse_prev_close=taiex_prev_close
                )
            
            logging.info(f"📈 累計：{len(df):>6,} ｜ 新增：{new_count:>6,}")

            # --- 3. 靜態報告生成 (僅歷史模式) ---
            if not ctx.real_time_mode:
                if df is None or df.empty:
                     logging.warning("⚠️ 沒有新資料，請確認時間或來源。")
                else:
                    logging.info("📊 資料獲取完畢，準備生成靜態報告...")
                    stats_html = stats_table.generate_stats_html(
                        stats_table.compute_stats(df, txf_prev_close)
                    )
                    generate_html_report(
                        df=df,
                        stats_html=stats_html,
                        ctx=ctx,
                        txf_prev_close=txf_prev_close,
                        taiex_prev_close=taiex_prev_close
                    )
                break # 歷史模式只跑一次

        except KeyboardInterrupt:
            logging.info("\n🛑 收到使用者中斷 (Ctrl+C)，正在結束 process_market_session...")
            break 
        
        except Exception as e:
            logging.exception(f"❌ [T_Data] process_market_session 迴圈發生未預期錯誤: {e}")
            
            if ctx.real_time_mode:
                logging.info("     10 秒後嘗試繼續迴圈...")
                time.sleep(10) 
            else:
                logging.error("     歷史模式發生錯誤，中斷此任務。")
                break

