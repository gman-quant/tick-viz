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
# 5. 應用程式設定 (更新週期 / 輸出路徑)
# ------------------------------------------------------------
FETCH_INTERVAL  = 2    # [秒] consumer.poll() 的最長等待時間
UPDATE_INTERVAL = 2    # [秒] UI (Dash) 更新週期

# (報告輸出路徑)
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"
OUTPUT_DIR.mkdir(exist_ok=True)  # 確保目錄存在

# ------------------------------------------------------------
# 6. 資料快取路徑
# ------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DATA_DIR.mkdir(exist_ok=True)  # 確保目錄存在


'''
[Git 開發者筆記]

若需在「本地」暫時修改此 config.py (例如切換 Kafka 位址) 
且「不想」將這些修改 commit 出去時，可使用以下指令：

(1) 暫時忽略此檔案的變更 (讓 git status 看不見):
git update-index --assume-unchanged config.py

(2) 恢復追蹤此檔案的變更 (當您真的要 commit 變更時):
git update-index --no-assume-unchanged config.py
'''