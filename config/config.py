# config/config.py

# Standard Library Imports
import os
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import time as dt_time

# Third-Party Imports
from dotenv import load_dotenv

# ------------------------------------------------------------
# 1. 載入環境變數
# ------------------------------------------------------------
# (從 .env 檔案載入設定值)
load_dotenv(override=True)

# ------------------------------------------------------------
# 2. Shioaji API 憑證
# ------------------------------------------------------------
SHIOAJI_API_KEY    = os.environ.get("SHIOAJI_API_KEY")
SHIOAJI_SECRET_KEY = os.environ.get("SHIOAJI_SECRET_KEY")

# ------------------------------------------------------------
# 3. Kafka 連線設定
# ------------------------------------------------------------
KAFKA_BROKER       = os.environ.get("KAFKA_BROKER")
KAFKA_TOPIC        = os.environ.get("KAFKA_TOPIC")
KAFKA_GROUP_ID     = "tick-consumer-group"

# ------------------------------------------------------------
# 4. 時區與交易時段
# ------------------------------------------------------------
TAIWAN_TZ = ZoneInfo("Asia/Taipei")

# (日盤)
DAY_START   = dt_time( 8, 30)
DAY_END     = dt_time(13, 45, 15) # (15 秒緩衝)

# (夜盤)
NIGHT_START = dt_time(14, 50)
NIGHT_END   = dt_time( 5,  0, 15) # (15 秒緩衝)

# ------------------------------------------------------------
# 5. 應用程式時序設定 (單位：秒)
# ------------------------------------------------------------
# (後端：Kafka 沒資料時的最大等待時間)
KAFKA_POLL_TIMEOUT = 10

# (前端：Dash UI 的畫面刷新週期)
UI_UPDATE_INTERVAL =  5

# ------------------------------------------------------------
# 6. 檔案路徑設定 (輸出與快取)
# ------------------------------------------------------------
# (報告輸出路徑：存放 HTML)
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# (資料快取路徑：存放 Parquet)
CACHE_DIR = Path(__file__).resolve().parents[1] / "data"
CACHE_DIR.mkdir(exist_ok=True)