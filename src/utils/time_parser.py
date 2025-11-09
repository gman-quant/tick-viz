# src/utils/time_parser.py

# Standard Library Imports
import logging
from datetime import date, datetime

# Third-Party Imports
from dateutil import parser

# Local Application Imports
from config.config import TAIWAN_TZ

# ------------------------------------------------------------
# 📦 Tick 時間字串解析
# ------------------------------------------------------------
def parse_tick_datetime(raw_dt: str) -> datetime | None:
    """
    (高效能版) 
    將 Kafka 傳入的 ISO 格式時間字串，解析為帶有 TAIWAN_TZ 時區的 datetime 物件。
    
    假設:
    - raw_dt 100% 是由 Producer 產生、
    - 且帶有時區 (+08:00) 的 ISO 字串。
    """
    try:
        # --- 1. 使用內建函式高速解析 ---
        # (假設格式固定，使用 fromisoformat 效能最佳)
        dt = datetime.fromisoformat(raw_dt) 
        
        # --- 2. 標準化時區至 TAIWAN_TZ ---
        # (確保即使收到 UTC 或其他時區也能正確轉換)
        return dt.astimezone(TAIWAN_TZ) 
        
    except Exception as e:
        # --- 3. 處理預期外的格式錯誤 ---
        logging.error(f"❌ 嚴重：Tick datetime 格式與預期不符: {raw_dt}，錯誤: {e}")
        return None

# ------------------------------------------------------------
# 📦 CLI 日期字串解析
# ------------------------------------------------------------
def parse_date(raw_date: str) -> date | None:
    """
    (argparse 用) 把 YYYY-MM-DD 格式的字串轉成 date 物件
    """
    return datetime.strptime(raw_date, "%Y-%m-%d").date() if raw_date else None