# main.py

# Standard Library Imports
import argparse
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
from src.utils.time_parser import parse_date
from src.web.dash_app import create_dash_app
from src.web.shared_state import shared_state


# ------------------------------------------------------------
# 主流程
# ------------------------------------------------------------
def main(
    real_time_mode:    bool = True,
    date_start: date | None = None,
    date_end:   date | None = None,
    session:     str | None = None,
):
    """
    主執行流程，支援即時模式 (24/7 Server) 與歷史模式（多日迭代）。
    """

    if not real_time_mode:
        # --- 歷史回顧模式 ---
        clear_console()
        logging.info("📘 執行歷史回顧模式...")
        with shioaji_session() as api:
            one_day = timedelta(days=1)
            dt_st = date_start or (date.today() - one_day)
            dt_ed = date_end or date.today()
            pick = session or "whole"

            st, ed = get_session_range(pick)
            current = dt_st

            while current <= dt_ed:
                # 跳過週末
                if current.weekday() >= 5:
                    logging.info(f"⏩ 跳過週末：{current}")
                    current += one_day
                    continue

                # 迭代處理日盤與夜盤
                for day_session in range(st, ed - 1, -1):
                    logging.info(f"📅 處理日期：{current} - {'日盤' if day_session else '夜盤'}")

                    ctx = RunContext(
                        trade_date=current,
                        session_type=SessionType.DAY if day_session else SessionType.NIGHT,
                        real_time_mode=False,
                        data_source=DataSource.SHIOAJI # 亦可用 DataSource.KAFKA 
                    )
                    run_single_session_task(ctx, api)

                current += one_day
                
        logging.info("✅ 歷史回顧模式執行完畢。")

    else:
        # --- 即時 24/7 伺服器模式 ---
        logging.info("⚡ [Main] 執行 24/7 即時伺服器模式...")
        
        # --- 啟動背景資料處理執行緒 ---
        logging.info("📊 [Main] 啟動 24/7 背景資料管理器 (T_Data)...")
        data_thread = threading.Thread(
            target=data_loop_manager,
            args=(),
            daemon=True
        )
        data_thread.start()

        # --- 啟動前景 Web Server (主執行緒) ---
        logging.info(f"🚀 [Main] 啟動 Web Server (MainThread) 於 http://localhost:8080 ...")
        app = create_dash_app(shared_state) 
        
        try:
            app.run(host="0.0.0.0", port=8080, debug=False, use_reloader=False)
        
        except KeyboardInterrupt:
            logging.info("\n👋 [Main] 收到使用者關閉訊號 (Ctrl+C)...")
        except Exception as e:
            logging.exception(f"⚠️ [Main] Dash Server 啟動失敗: {e}")
        
        logging.info("✅ [Main] Dash Server 已關閉，程式結束。")


# ------------------------------------------------------------
# 程式進入點
# ------------------------------------------------------------
if __name__ == "__main__":

    # --- 1. 設定全域日誌格式 ---
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # --- 2. 靜音 Dash/Werkzeug 伺服器日誌 ---
    # (避免 'POST /_dash-update-component... 200 -' 洗版)
    werkzeug_logger = logging.getLogger('werkzeug')
    werkzeug_logger.setLevel(logging.ERROR)

    # --- 3. 建立命令列參數解析器 ---
    parser = argparse.ArgumentParser(description="台指期 Tick 資料處理與繪圖")

    # --- 4. 定義 CLI 參數 ---
    parser.add_argument("--real-time-mode", type=int, choices=[0, 1], default=1,
                        help="即時模式 (1=啟用, 0=停用)")
    
    parser.add_argument("--date-start", type=parse_date,
                        help="資料開始日期 (格式: YYYY-MM-DD)")
    
    parser.add_argument("--date-end", type=parse_date,
                        help="資料結束日期 (格式: YYYY-MM-DD)")
    
    parser.add_argument("--session", type=str, choices=["day", "night", "whole"],
                        help="交易時段: day=日盤, night=夜盤, whole=全部")

    # --- 5. 解析傳入參數 ---
    args = parser.parse_args()

    # --- 6. 執行主程式 ---
    main(
        real_time_mode=bool(args.real_time_mode),
        date_start=args.date_start,
        date_end=args.date_end,
        session=args.session,
    )


''' 📊 tick-viz 專案常用指令

🟢 即時更新模式
cd Projects/tick-viz && source venv/bin/activate
python main.py --real-time-mode 1

🔵 歷史回顧模式
cd Projects/tick-viz && source venv/bin/activate
python main.py --real-time-mode 0 --date-start 2025-11-06 --date-end 2025-11-07 --session whole

 📅 日線圖更新
cd Projects/tick-viz && source venv/bin/activate
python -m scripts.generate_daily_csv
python -m scripts.plot_txf_kbar

📦 生成 requirements.txt
pip freeze > requirements.txt
'''


''' macOS launchd 代理程式管理與除錯指令

這份精簡指南涵蓋了常用的 launchctl 指令與相關檔案操作，方便你快速掌握自動啟動代理程式的狀態與問題排查。

📂 代理程式檔案位置
| Agent Label          | Script Name             | Schedule Time          | Description       
| -------------------- | ----------------------- | ---------------------- | ----------------- 
| com.garrett.tickviz  | update_daily_chart.sh   | MON - FRI 13:46        | 自動執行歷史回顧模式  
| com.garrett.tickviz2 | monitor_realtime_txf.sh | MON - FRI 08:40, 14:55 | 自動執行即時更新模式  


📜 launchctl 指令
launchctl list | grep tickviz                                                       # 查看所有與 tickviz 相關的代理程式
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.garrett.tickviz.plist     # 卸載舊代理程式（若有）
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.garrett.tickviz2.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.garrett.tickviz.plist   # 載入新代理程式
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.garrett.tickviz2.plist
launchctl kickstart -k gui/$(id -u)/com.garrett.tickviz                             # 立即測試執行（強制重啟）
launchctl print gui/$(id -u)/com.garrett.tickviz                                    # 查看代理程式的狀態、日誌與退出碼
launchctl list com.garrett.tickviz                                                  # 列出該代理程式的狀態
launchctl start gui/$(id -u)/com.garrett.tickviz                                    # 啟動代理程式（若尚未運行）
launchctl stop gui/$(id -u)/com.garrett.tickviz                                     # 停止正在執行的代理程式
launchctl remove com.garrett.tickviz                                                # 從 launchctl 清單中移除代理程式（卸載前請先 bootout）

📂 .plist 檔案除錯
plutil -lint ~/Library/LaunchAgents/com.garrett.tickviz.plist   # 檢查 .plist 檔案的 XML 語法是否正確。
nano ~/Library/LaunchAgents/com.garrett.tickviz.plist	        # 編輯 .plist 檔案。
nano ~/Library/LaunchAgents/com.garrett.tickviz2.plist	        # 編輯 .plist 檔案。

📂 腳本與日誌
nano ~/Library/Scripts/update_daily_chart.sh    # 編輯 startup.sh 腳本的內容。
nano ~/Library/Scripts/monitor_realtime_txf.sh
cat /tmp/tickviz.log	                        # 查看 腳本的標準輸出日誌。
cat /tmp/tickviz2.out	                        # 查看 腳本的標準輸出日誌。
cat /tmp/tickviz.err	                        # 查看 腳本的錯誤日誌。
tail -f /tmp/tickviz2.out /tmp/tickviz2.err     # 實時查看腳本的標準輸出與錯誤日誌
'''

