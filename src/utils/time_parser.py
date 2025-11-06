# src/utils/time_parser.py (v2, 統一使用 logging)

# Standard Library Imports
import logging
from datetime import date, datetime

# Third-Party Imports
from dateutil import parser

# Local Application Imports
from config.config import TAIWAN_TZ

combine = datetime.combine

def parse_tick_datetime(raw_dt: str) -> datetime | None:
    """
    將原始 tick 的字串 datetime 轉為帶時區的 datetime 物件（Asia/Taipei）
    """
    try:
        dt = parser.isoparse(raw_dt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TAIWAN_TZ)
        else:
            dt = dt.astimezone(TAIWAN_TZ)
        return dt
    except Exception as e:
        # (修改) 改用 logging.warning
        logging.warning(f"⚠️ 無法解析 datetime: {raw_dt}，錯誤: {e}")
        return None
    
# 把日期字串轉成 date 物件
def parse_date(raw_date: str) -> date | None:
    return datetime.strptime(raw_date, "%Y-%m-%d").date() if raw_date else None