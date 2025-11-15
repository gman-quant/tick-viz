# src/core/orchestrator.py

# Standard Library Imports
import logging
import threading
from datetime import date, timedelta

# Local Application Imports
from config.run_context import RunContext
from config.types import DataSource, SessionType
from src.core.loop_manager import data_loop_manager, run_single_session_task
from src.utils.misc import clear_console
from src.utils.resource_contexts import shioaji_session
from src.utils.session_time import get_session_range
from src.web.dash_app import create_dash_app
from src.web.shared_state import shared_state

# ------------------------------------------------------------
# 模式一：歷史回顧 (Backfill)
# ------------------------------------------------------------
def run_backfill_mode(
    date_start: date | None,
    date_end:   date | None,
    session:     str | None,
    data_source: str
):
    """
    執行歷史回顧模式的完整邏輯。
    (此函數即為您原本 main.py 中的 _run_backfill_loop 及其外層邏輯)
    """
    
    # 1. 決定資料來源
    if data_source == "shioaji":
        selected_data_source = DataSource.SHIOAJI
    else:
        selected_data_source = DataSource.KAFKA
    
    logging.info(f"💾 資料來源設定為: {selected_data_source.name}")
    
    # 2. 定義迴圈邏輯 (內部函數)
    def _run_loop(api_instance):
        one_day = timedelta(days=1)
        dt_st = date_start or (date.today() - one_day)
        dt_ed = date_end or date.today()
        pick = session or "whole"

        st, ed = get_session_range(pick)
        current = dt_st

        while current <= dt_ed:
            if current.weekday() >= 5:
                logging.info(f"⏩ 跳過週末：{current}")
                current += one_day
                continue

            for day_session in range(st, ed - 1, -1):
                logging.info(f"📅 處理日期：{current} - {'日盤' if day_session else '夜盤'}")

                ctx = RunContext(
                    trade_date=current,
                    session_type=SessionType.DAY if day_session else SessionType.NIGHT,
                    real_time_mode=False,
                    data_source=selected_data_source
                )
                # (*** 呼叫 loop_manager.py 中的函數 ***)
                run_single_session_task(ctx, api_instance) 

            current += one_day

    # 3. 根據資料來源決定是否登入
    clear_console()
    if selected_data_source == DataSource.SHIOAJI:
        logging.info("🔑 需要 API 存取，正在登入 Shioaji...")
        with shioaji_session() as api:
            _run_loop(api)
    else:
        logging.info("🚀 無需 API 存取 (使用 Kafka)，直接執行。")
        _run_loop(None)


# ------------------------------------------------------------
# 模式二：即時伺服器 (Real-time)
# ------------------------------------------------------------
def run_realtime_mode():
    """
    執行 24/7 即時伺服器模式的完整邏輯。
    (此函數即為您原本 main.py 中的 else 區塊)
    """
    
    # --- 啟動背景資料處理執行緒 ---
    logging.info("📊 [Main] 啟動 24/7 背景資料管理器 (T_Data)...")
    data_thread = threading.Thread(
        target=data_loop_manager, # (*** 呼叫 loop_manager.py 中的函數 ***)
        args=(),
        daemon=True
    )
    data_thread.start()

    # --- 啟動前景 Web Server (主執行緒) ---
    logging.info(f"🚀 [Main] 啟動 Web Server 於 http://localhost:8080 ...")
    app = create_dash_app(shared_state) 
    
    try:
        app.run(host="0.0.0.0", port=8080, debug=False, use_reloader=False)
    
    except KeyboardInterrupt:
        logging.info("\n👋 [Main] 收到使用者關閉訊號 (Ctrl+C)...")
    except Exception as e:
        logging.exception(f"⚠️ [Main] Dash Server 啟動失敗: {e}")
    
    logging.info("✅ [Main] Dash Server 已關閉。")