# config/config.py
# 不再追蹤: git update-index --assume-unchanged config.py
# 恢復追蹤: git update-index --no-assume-unchanged config.py


# ==== 靜態設定區（環境變數、固定常數）====

import os
from pathlib import Path
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

load_dotenv(override=True)

# === Shioaji API credentials ===
SHIOAJI_API_KEY    = os.environ.get("SHIOAJI_API_KEY")
SHIOAJI_SECRET_KEY = os.environ.get("SHIOAJI_SECRET_KEY")

# === Kafka connection settings ===
KAFKA_BROKER   = os.environ.get("KAFKA_BROKER")
KAFKA_TOPIC    = os.environ.get("KAFKA_TOPIC")
KAFKA_GROUP_ID = "tick-consumer-group"

# === Timezone setting ===
TAIWAN_TZ = ZoneInfo("Asia/Taipei")

# === Report and chart settings ===
CLEAR_SCREEN_EACH_CYCLE = True
FETCH_INTERVAL  = 2    # [秒] Consumer 輪詢間隔；控制每次 poll() 等待新 tick 的最長時間
UPDATE_INTERVAL = 2    # [秒] UI 更新週期；控制每隔多久重新計算與刷新統計資訊
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"
OUTPUT_DIR.mkdir(exist_ok=True)  # 確保目錄存在

# === Data cache settings ===
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DATA_DIR.mkdir(exist_ok=True)  # 確保目錄存在


