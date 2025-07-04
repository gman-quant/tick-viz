# tick-viz/config.py
# 不再追蹤: git update-index --assume-unchanged config.py
# 恢復追蹤: git update-index --no-assume-unchanged config.py


import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from pathlib import Path
from dotenv import load_dotenv

# ==== 載入環境變數 ====
load_dotenv()

# ==== Shioaji API 金鑰 ====
SHIOAJI_API_KEY    = os.environ.get('SHIOAJI_API_KEY')
SHIOAJI_SECRET_KEY = os.environ.get('SHIOAJI_SECRET_KEY')

# ==== Kafka 參數設定 ====
KAFKA_BROKER   = os.environ.get('KAFKA_BROKER')
KAFKA_TOPIC    = os.environ.get('KAFKA_TOPIC')
KAFKA_GROUP_ID = 'tick-consumer-group'

# ==== 時區與時間區間設定 ====
TAIWAN_TZ = ZoneInfo("Asia/Taipei")
# 日盤
# START_DATETIME = datetime(2025, 7, 4,  8, 45, 0, 0, tzinfo=TAIWAN_TZ)
# END_DATETIME   = datetime(2025, 7, 4, 13, 45, 0, 0, tzinfo=TAIWAN_TZ)
# 夜盤
START_DATETIME = datetime(2025, 7, 4, 15, 0, 0, 0, tzinfo=TAIWAN_TZ)
END_DATETIME   = datetime(2025, 7, 5,  5,  0, 0, 0, tzinfo=TAIWAN_TZ)

# ==== 繪圖與輸出設定 ====
# True: 使用上面設定的固定結束時間; False: 使用當前時間作為結束時間。
USE_FIXED_END_TIME = False 
# Volume Bar 成交量基準 (例如: 每 450 口產生一根 K棒)
VOLUME_PER_BAR = 450
# HTML 報告自動刷新秒數
REFRESH_INTERVAL_SECONDS = 999999 if USE_FIXED_END_TIME else 15

# ==== 報告生成設定 ====
# 報告標題
Report_TITLE = f"TXF-Charts_{START_DATETIME.strftime('%Y-%m-%d_%H%M')}" if USE_FIXED_END_TIME else "TXF-Charts-Live"
# HTML 報告輸出路徑
OUTPUT_DIR = Path(r"G:\我的雲端硬碟\Trading\Dashboard_snapshot") # Path(__file__).parent / "output"
