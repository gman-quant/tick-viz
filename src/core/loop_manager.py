# src/service.py

# Standard Library Imports
import logging
import time
from datetime import datetime, timedelta

# Third-Party Imports
from confluent_kafka import TopicPartition

# Local Application Imports
from config.config import KAFKA_TOPIC, TAIWAN_TZ
from config.run_context import RunContext
from config.types import DataSource, SessionType
from src.core.session_processor import process_market_session
from src.utils.resource_contexts import kafka_consumer
from src.utils.session_time import in_which_session, is_am_night_session, get_next_valid_day_session_start
from src.web.shared_state import shared_state


# ------------------------------------------------------------
# 📦 1. 單一盤別的資料處理任務
# ------------------------------------------------------------
def run_single_session_task(ctx: RunContext, api=None) -> bool:
    """
    執行「單一盤別」的資料處理。
    
    回傳值:
    - True  = 成功取得 Offset 並進入資料迴圈。
    - False = 偵測到休市 (例如假日) 或發生致命錯誤而終止。
    """
    
    # --- (A) 歷史模式 (Shioaji) ---
    if not ctx.real_time_mode and ctx.data_source == DataSource.SHIOAJI:
        process_market_session(None, None, ctx, api)
        return True 

    # --- (B) 即時模式 / Kafka 歷史模式 ---
    if ctx.real_time_mode or ctx.data_source == DataSource.KAFKA:
        logging.info(f"📊 [T_Data] 資料處理任務 (run_single_session_task) 已啟動 ({ctx.session_type.name})。")
        
        # --- (B.1) 重設共用狀態 ---
        logging.info(f"🧹 [T_Data] 正在清除舊資料，準備 {ctx.session_type.name} SESSION...")
        with shared_state.lock:
            shared_state.latest_df = None
            shared_state.txf_prev_close = None
            shared_state.taiex_prev_close = None
            shared_state.plot_df = None
            shared_state.kbars_1min = None
        
        try:
            # --- (B.2) Kafka 消費者初始化 (含重試與休市偵測) ---
            with kafka_consumer() as consumer:
                valid_start_offsets = None
                grace_period_end = ctx.start_datetime + timedelta(minutes=20) # (20 分鐘開盤寬限期)
                
                # --- (迴圈：直到成功取得 Kafka Offset 或 偵測到休市) ---
                while True:

                    try:
                        # --- (嘗試取得 Offset) ---
                        # [極致簡化] 假設只有 Partition 0 (效能優化)
                        # 1. 設定目標時間 (毫秒)
                        target_ts_ms = int(ctx.start_datetime.timestamp() * 1000)
                        # 2. 建立搜尋請求 (指定 Partition 0)
                        search_partition = TopicPartition(KAFKA_TOPIC, 0, target_ts_ms)
                        # 3. 發送查詢，取得結果
                        found_offsets = consumer.offsets_for_times([search_partition])

                        if found_offsets and all(p.offset >= 0 for p in found_offsets):
                            # --- (成功取得 Offset) ---
                            valid_start_offsets = found_offsets
                            logging.info(f"✅ [T_Data] 成功取得 Offsets: {[p.offset for p in valid_start_offsets]}")
                            break # 進入B.3

                        else:
                            # --- (取得 Offset 失敗) ---
                            logging.info(f"⏳ [T_Data] 尚未取得有效 offset...")
                            
                    except Exception as e:
                        # --- (Kafka 連線例外) ---
                        logging.error(f"🔥 [T_Data] Kafka 初始化時發生錯誤: {e}")
                        
                    # (只有在 'try' 區塊中「沒有 break」時，才會執行到這裡)  
                    # --- (檢查: 假日/休市 "Fail Fast") ---
                    if datetime.now(TAIWAN_TZ) > grace_period_end:
                        logging.warning(f"⚠️ [T_Data] 已超過開盤寬限期 {grace_period_end} 且仍無 offset。")
                        logging.warning("     判斷為假日休市，自動終止此任務。")
                        return False # (回傳 False (失敗訊號))
                    # --- (在寬限期內，重試) ---
                    else:
                        logging.info(f"     在開盤寬限期 {grace_period_end} 內，10 秒後重試...")
                        time.sleep(10)

                # --- (B.3) 進入資料處理迴圈 ---
                logging.info("✅ [T_Data] Offsets 已鎖定，進入 process_market_session...")
                process_market_session(consumer, valid_start_offsets, ctx)
                logging.info(f"✅ [T_Data] {ctx.session_type.name} 任務執行完畢。\n")
                return True # (回傳 True (成功))

        except Exception as e:
            logging.exception(f"🔥 [T_Data] run_single_session_task 發生致命錯誤: {e}")
            return False # (回傳 False (失敗訊號))


# ------------------------------------------------------------
# 📦 2. 24/7 運作的資料迴圈管理器
# ------------------------------------------------------------
def data_loop_manager():
    """
    這是在 T_Data 背景執行緒中「永遠執行」的管理器。
    """
    logging.info(f"✅ [T_Data] 24/7 資料迴圈管理器 (data_loop_manager) 已啟動。")
    
    # --- 狀態管理 ---
    
    # "當前運行的任務" ID (e.g., "2025-11-10-DAY")
    current_running_session_key = None
    
    # "假日休眠" 模式的「解除時間」
    # (如果偵測到假日，設定此時間戳，系統將休眠直到下一個日盤開盤，完美跳過所有累贅的夜盤檢查。)
    holiday_sleep_until: datetime | None = None
    
    # --- 24/7 監控迴圈 ---
    while True:
        try:
            # --- (A) 取得「單一」時間點 ---
            now_dt = datetime.now(TAIWAN_TZ) 
            today  = now_dt.date()
            
            # --- (B) 檢查 1: 是否處於「假日休眠」模式 ---
            if holiday_sleep_until is not None:
                if now_dt < holiday_sleep_until:
                    # (仍在休眠期)
                    logging.info(f"💤 [T_Data] 處於假日休市模式，直到 {holiday_sleep_until}。60 秒後檢查。")
                    time.sleep(60)
                    continue
                else:
                    # (休眠期結束)
                    logging.info(f"ℹ️ [T_Data] 假日休市模式結束，恢復正常檢查。")
                    holiday_sleep_until = None # (解除休眠)
            
            # --- (C) 檢查 2: 是否為正常休市 ---
            current_session_type = in_which_session(now_dt)
            if current_session_type == SessionType.CLOSED:
                if current_running_session_key is not None:
                    # (剛收盤)
                    logging.info(f"ℹ️ [T_Data] {current_running_session_key} 已收盤。")
                    logging.info("     畫面將保留最後狀態。等待下一交易時段...")
                    current_running_session_key = None # (重設「運行中」標記)
                    with shared_state.lock:
                        shared_state.context = RunContext(
                            trade_date=today,
                            session_type=SessionType.CLOSED
                        )
                else:
                    # (處於休市)
                    logging.info(f"💤 [T_Data] 休市中... 每 60 秒檢查一次。")
                
                time.sleep(60) # (正常休市 sleep)
                continue 

            # --- (D) 檢查 3: 是否為「新」的任務 ---

            # 1. 判斷是否為 AM 盤 (凌晨 00:00 - 05:00)
            is_am = is_am_night_session(now_dt.time())
            # 2. 取得此 Session 應歸屬的「交易日」
            session_trade_date = today - timedelta(days=1) if is_am else today
            # 3. 組合出標準化的 Key
            new_session_key = f"{session_trade_date}-{current_session_type.name}"
            
            if new_session_key != current_running_session_key:
                
                # --- (D.1) 建立新盤別的 Context ---
                logging.info(f"🚀 [T_Data] 偵測到新交易時段: {new_session_key} SESSION")
                current_running_session_key = new_session_key 
                
                with shared_state.lock:
                    shared_state.context = RunContext(
                        trade_date=today,
                        session_type=current_session_type
                    )
                
                # --- (D.2) 啟動任務 (並檢查回傳值) ---
                task_success = run_single_session_task(shared_state.context, api=None)
                
                # --- (D.3) 處理假日/休市終止 ---
                if not task_success:
                    # (任務回傳 False，判斷為休市)
                    logging.warning(f"⚠️ [T_Data] {new_session_key} 任務回報失敗 (判斷為休市)。")
                    
                    # (1. 更新儀表板為 CLOSED)
                    with shared_state.lock:
                        shared_state.context = RunContext(
                            trade_date=today,
                            session_type=SessionType.CLOSED
                        )
                    
                    # (2. 計算「下一個日盤」開盤時間，並設為「休眠直到」的時間戳)
                    holiday_sleep_until = get_next_valid_day_session_start(now_dt)
                    logging.warning(f"     啟動「假日休市」模式，將休眠直到 {holiday_sleep_until}")
                    
                    # (3. 重設 "運行中" 標記)
                    current_running_session_key = None
            
        except Exception as e:
            # --- (E) 管理器例外處理 ---
            logging.exception(f"🔥 [T_Data] data_loop_manager 發生嚴重錯誤: {e}")
            logging.info("     60 秒後自動重試管理器...")
            time.sleep(60)

