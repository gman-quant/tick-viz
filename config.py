# tick-viz/config.py
# 不再追蹤: git update-index --assume-unchanged config.py
# 恢復追蹤: git update-index --no-assume-unchanged config.py

import os
from datetime import date
from pathlib import Path

from zoneinfo import ZoneInfo
from dotenv import load_dotenv

from src.utils.session_time import get_trading_session
from src.processing.volume_bars import get_volume_per_bar

# === Load environment variables ===
load_dotenv(override=True)

# === Shioaji API credentials ===
SHIOAJI_API_KEY    = os.environ.get('SHIOAJI_API_KEY')
SHIOAJI_SECRET_KEY = os.environ.get('SHIOAJI_SECRET_KEY')

# === Kafka connection settings ===
KAFKA_BROKER   = os.environ.get('KAFKA_BROKER')
KAFKA_TOPIC    = os.environ.get('KAFKA_TOPIC')
KAFKA_GROUP_ID = 'tick-consumer-group'

# === Time and session configuration ===
TAIWAN_TZ = ZoneInfo("Asia/Taipei")

# Toggle between real-time mode and historical mode
# True  → constantly update report using current time
# False → generate one-time report for a fixed time window
IS_REALTIME_MODE = 1

# Base date and session type for historical mode
DATE         = date(2025, 7, 7)
DAY_SESSION  = 1  # True = 08:30–13:45, False = 14:50–05:00
START_DATETIME, END_DATETIME = get_trading_session(DATE, DAY_SESSION, IS_REALTIME_MODE, TAIWAN_TZ)

# === Chart and output settings ===

# Whether to clear terminal screen after each update cycle
CLEAR_SCREEN_EACH_CYCLE = True

# Number of contracts per volume-based bar
VOLUME_PER_BAR = get_volume_per_bar(DAY_SESSION)

# Interval (in seconds) to update data and trigger frontend auto-refresh
UPDATE_INTERVAL = 12

# === Report generation settings ===
# Data Source
DATA_SOURCE = "kafka" # "kafka" if you have

# Report title (also used as output file name)
REPORT_TITLE = (
    "TXF-Charts-Live"
    if IS_REALTIME_MODE
    else 
    f"TXF-Charts_1_{START_DATETIME.strftime('%Y-%m-%d')}_{DATA_SOURCE}" 
    if DAY_SESSION
    else
    f"TXF-Charts_2_{START_DATETIME.strftime('%Y-%m-%d')}_{DATA_SOURCE}"
)

# Output directory for HTML report
OUTPUT_DIR = Path(__file__).parent / "output"
