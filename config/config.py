# config/config.py

# Standard Library Imports
import os
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import time as dt_time

# Third-Party Imports
from dotenv import load_dotenv

# ------------------------------------------------------------
# 1. 專案基礎設定 (路徑)
# ------------------------------------------------------------
# (config.py 檔案的上一層目錄)
BASE_DIR = Path(__file__).resolve().parents[1]

# (報告輸出路徑：存放 HTML)
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# (資料快取路徑：存放 Parquet)
CACHE_DIR = BASE_DIR / "data"
CACHE_DIR.mkdir(exist_ok=True)

# ------------------------------------------------------------
# 2. 環境變數載入
# ------------------------------------------------------------
# (從 .env 檔案載入設定值)
load_dotenv(override=True)

# ------------------------------------------------------------
# 3. 外部服務連線 (讀取環境變數)
# ------------------------------------------------------------
# (Shioaji API 憑證)
SHIOAJI_API_KEY         = os.environ.get("SHIOAJI_API_KEY")
SHIOAJI_SECRET_KEY      = os.environ.get("SHIOAJI_SECRET_KEY")

# (Kafka 連線設定)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS") 
KAFKA_TOPIC             = os.environ.get("KAFKA_TOPIC")
KAFKA_GROUP_ID          = "tick-consumer-group"

# ------------------------------------------------------------
# 4. 交易邏輯設定 (時區與時段)
# ------------------------------------------------------------
TAIWAN_TZ = ZoneInfo("Asia/Taipei")

# (日盤)
DAY_SESSION_START_TIME   = dt_time( 8, 30)
DAY_SESSION_END_TIME     = dt_time(13, 45, 15) # (15 秒緩衝)

# (夜盤)
NIGHT_SESSION_START_TIME = dt_time(14, 50)
NIGHT_SESSION_END_TIME   = dt_time( 5,  0, 15) # (15 秒緩衝)

# ------------------------------------------------------------
# 5. 應用程式行為
# ------------------------------------------------------------
# (後端：Kafka 沒資料時的最大等待時間)
KAFKA_POLL_TIMEOUT_SECONDS  = 10
# (前端：Dash UI 的畫面刷新週期)
UI_REFRESH_INTERVAL_SECONDS = 5
# (預設時間窗口，可於 UI 動態調整)
DEFAULT_LOOKBACK_MINUTES    = 120