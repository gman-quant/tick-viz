# config/config.py
# 不再追蹤: git update-index --assume-unchanged config.py
# 恢復追蹤: git update-index --no-assume-unchanged config.py


# ==== 靜態設定區（環境變數、固定常數）====

import os
from pathlib import Path
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from datetime import time as dt_time

load_dotenv(override=True)

# === Shioaji API credentials ===
SHIOAJI_API_KEY    = os.environ.get("SHIOAJI_API_KEY")
SHIOAJI_SECRET_KEY = os.environ.get("SHIOAJI_SECRET_KEY")

# === Kafka connection settings ===
KAFKA_BROKER       = os.environ.get("KAFKA_BROKER")
KAFKA_TOPIC        = os.environ.get("KAFKA_TOPIC")
KAFKA_GROUP_ID     = "tick-consumer-group"

# === Timezone setting ===
TAIWAN_TZ = ZoneInfo("Asia/Taipei")

# === Trading Session Times ===
DAY_START   = dt_time( 8, 30)
DAY_END     = dt_time(13, 45, 15) # 15 秒緩衝
NIGHT_START = dt_time(14, 50)
NIGHT_END   = dt_time( 5,  0, 15) # 15 秒緩衝

# === Report and chart settings ===
FETCH_INTERVAL  = 2    # [秒] consumer.poll() 的最長等待時間
UPDATE_INTERVAL = 2    # [秒] UI 更新週期
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"
OUTPUT_DIR.mkdir(exist_ok=True)  # 確保目錄存在

# === Data cache settings ===
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DATA_DIR.mkdir(exist_ok=True)  # 確保目錄存在

