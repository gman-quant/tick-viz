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
    將原始 tick 的字串 datetime (ISO 格式) 轉為帶時區的 datetime 物件（Asia/Taipei）
    """
    try:
        dt = parser.isoparse(raw_dt)
        # 確保 datetime 物件最終帶有 TAIWAN_TZ 時區資訊
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TAIWAN_TZ)
        else:
            dt = dt.astimezone(TAIWAN_TZ)
        return dt
    except Exception as e:
        logging.warning(f"⚠️ 無法解析 datetime: {raw_dt}，錯誤: {e}")
        return None

# ------------------------------------------------------------
# 📦 CLI 日期字串解析
# ------------------------------------------------------------
def parse_date(raw_date: str) -> date | None:
    """
    (argparse 用) 把 YYYY-MM-DD 格式的字串轉成 date 物件
    """
    return datetime.strptime(raw_date, "%Y-%m-%d").date() if raw_date else None