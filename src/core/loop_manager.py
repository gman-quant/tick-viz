# src/service.py

# Standard Library Imports
import logging
import time
from datetime import datetime, timezone

# Third-Party Imports
from confluent_kafka import TopicPartition

# Local Application Imports
from config.config import KAFKA_TOPIC, TAIWAN_TZ
from config.run_context import RunContext
from config.types import DataSource, SessionType
from src.core.session_processor import process_market_session
from src.utils.resource_contexts import kafka_consumer
from src.utils.session_time import in_which_session
from src.web.shared_state import shared_state


# ------------------------------------------------------------
# 📦 單一盤別的資料處理任務
# ------------------------------------------------------------
def run_single_session_task(ctx: RunContext, api=None):
    """
    執行「單一盤別」的資料處理 (Kafka 或 Shioaji)。
    (此版本包含 Kafka 啟動重試機制)
    """
    
    # --- 歷史模式 (Shioaji) ---
    if not ctx.real_time_mode and ctx.data_source == DataSource.SHIOAJI:
        process_market_session(None, None, ctx, api)
        return

    # --- 即時模式 (Kafka) 或 歷史模式 (Kafka) ---
    if ctx.real_time_mode or ctx.data_source == DataSource.KAFKA:
        logging.info(f"📊 [T_Data] 資料處理任務 (run_single_session_task) 已啟動 ({ctx.session_type.name})。")
        
        # --- 重設共用狀態 ---
        logging.info(f"🧹 [T_Data] 正在清除舊資料，準備 {ctx.session_type.name} SESSION...")
        with shared_state.lock:
            shared_state.latest_df = None
            shared_state.txf_prev_close = None
            shared_state.taiex_prev_close = None
            shared_state.plot_df = None
            shared_state.kbars_1min = None
        
        try:
            # --- Kafka 消費者初始化 (含重試) ---
            with kafka_consumer() as consumer:
                current_offsets = None
                
                # --- 迴圈：直到成功取得 Kafka Offset ---
                while current_offsets is None:
                    try:
                        logging.info(f"⏳ [T_Data] 正在取得 {ctx.session_type.name} 的 Kafka offsets...")
                        start_dt_utc = ctx.start_datetime.astimezone(timezone.utc)
                        timestamp_ms = int(start_dt_utc.timestamp() * 1000)

                        # 根據 Context 的起始時間去 Kafka 尋找對應的 offset
                        metadata = consumer.list_topics(KAFKA_TOPIC)
                        partitions = list(metadata.topics[KAFKA_TOPIC].partitions.keys())
                        topic_partitions = [
                            TopicPartition(KAFKA_TOPIC, p, timestamp_ms) for p in partitions
                        ]
                        fixed_offsets = consumer.offsets_for_times(topic_partitions)
                        
                        if fixed_offsets and all(p.offset >= 0 for p in fixed_offsets):
                            current_offsets = fixed_offsets.copy()
                            logging.info(f"✅ [T_Data] 成功取得 Offsets: {[p.offset for p in current_offsets]}")
                        else:
                            logging.info(f"⏳ [T_Data] 尚未取得有效 offset (可能 {ctx.session_type.name} 尚未開盤)，10 秒後重試...")
                            time.sleep(10)
                            
                    except Exception as e:
                        logging.error(f"🔥 [T_Data] Kafka 初始化時發生錯誤: {e}")
                        logging.info("     10 秒後自動重試...")
                        time.sleep(10)

                # --- 進入資料處理迴圈 ---
                logging.info("✅ [T_Data] Offsets 已鎖定，進入 process_market_session...")
                process_market_session(consumer, current_offsets, ctx)
                logging.info(f"✅ [T_Data] {ctx.session_type.name} 任務執行完畢。\n")

        except Exception as e:
            logging.exception(f"🔥 [T_Data] run_single_session_task 發生致命錯誤: {e}")


# ------------------------------------------------------------
# 📦 24/7 運作的資料迴圈管理器
# ------------------------------------------------------------
def data_loop_manager():
    """
    這是在 T_Data 背景執行緒中「永遠執行」的管理器。
    """
    logging.info(f"✅ [T_Data] 24/7 資料迴圈管理器 (data_loop_manager) 已啟動。")
    
    current_running_session_key = None
    
    # --- 24/7 監控迴圈 ---
    while True:
        try:
            current_session_type = in_which_session()

            # --- (A) 休市期間 ---
            if current_session_type == SessionType.CLOSED:
                if current_running_session_key is not None:
                    # 剛收盤
                    logging.info(f"ℹ️ [T_Data] {current_running_session_key} 已收盤。")
                    logging.info("     畫面將保留最後狀態。等待下一交易時段...")
                    current_running_session_key = None 
                else:
                    # 處於休市
                    logging.info(f"💤 [T_Data] 休市中... 每 60 秒檢查一次。")
                    time.sleep(60)
                continue 

            # --- (B) 偵測到開盤 ---
            today = datetime.now(tz=TAIWAN_TZ).date()
            new_session_key = f"{today}-{current_session_type.name}"

            # 如果是新的盤 (例如 08:45 日盤開始, 15:00 夜盤開始)
            if new_session_key != current_running_session_key:
                
                # --- 建立新盤別的 Context ---
                logging.info(f"🚀 [T_Data] 偵測到新交易時段: {new_session_key} session")
                current_running_session_key = new_session_key 
                
                with shared_state.lock:
                    shared_state.context = RunContext(
                        trade_date=today,
                        session_type=current_session_type
                    )
                
                # --- 啟動單一盤別處理任務 ---
                # (這個任務會持續運行直到收盤)
                run_single_session_task(shared_state.context, api=None)
            
        except Exception as e:
            # --- 管理器例外處理 ---
            logging.exception(f"🔥 [T_Data] data_loop_manager 發生嚴重錯誤: {e}")
            logging.info("     60 秒後自動重試管理器...")
            time.sleep(60)

