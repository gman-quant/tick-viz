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
    if not ctx.real_time_mode and ctx.data_source == DataSource.SHIOAJI:
        # Shioaji 歷史模式
        process_market_session(None, None, ctx, api)
        return
    if ctx.real_time_mode or ctx.data_source == DataSource.KAFKA:
        # Kafka 即時模式 or 歷史模式(data from Kafka)
        logging.info(f"📊 [T_Data] 資料處理任務 (run_single_session_task) 已啟動 ({ctx.session_type.name})。")
        
        logging.info(f"🧹 [T_Data] 正在清除舊資料，準備 {ctx.session_type.name} SESSION...")
        with shared_state.lock:
            shared_state.latest_df = None
            shared_state.txf_prev_close = None
            shared_state.taiex_prev_close = None
            shared_state.plot_df = None
            shared_state.kbars_1min = None
        
        try:
            with kafka_consumer() as consumer:
                current_offsets = None
                
                while current_offsets is None:
                    try:
                        logging.info(f"⏳ [T_Data] GNN {ctx.session_type.name} 的 Kafka offsets...")
                        start_dt_utc = ctx.start_datetime.astimezone(timezone.utc)
                        timestamp_ms = int(start_dt_utc.timestamp() * 1000)

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
    
    while True:
        try:
            current_session_type = in_which_session()

            if current_session_type == SessionType.CLOSED:
                # ... (休市邏輯不變) ...
                if current_running_session_key is not None:
                    logging.info(f"ℹ️ [T_Data] {current_running_session_key} 已收盤。")
                    logging.info("畫面將保留最後狀態。等待下一交易時段...")
                    current_running_session_key = None 
                else:
                    logging.info(f"💤 [T_Data] 休市中... 每 60 秒檢查一次。")
                    time.sleep(60)
                continue 
            
            today = datetime.now(tz=TAIWAN_TZ).date()
            new_session_key = f"{today}-{current_session_type.name}"

            # --- (換盤/新盤偵測，只在這裡計算一次時間) ---
            logging.info(f"🚀 [T_Data] 偵測到新交易時段: {new_session_key} session")
            current_running_session_key = new_session_key 
            
            # 建立完整的 RunContext
            with shared_state.lock:
                shared_state.context = RunContext(
                    trade_date=today,
                    session_type=current_session_type
                )

            # --- (一次性計算結束) ---
            
            # 啟動處理該盤資料的任務 (這個任務會持續運行直到收盤)
            run_single_session_task(shared_state.context, api=None)
            
        except Exception as e:
            logging.exception(f"🔥 [T_Data] data_loop_manager 發生嚴重錯誤: {e}")
            logging.info("     60 秒後自動重試管理器...")
            time.sleep(60)

