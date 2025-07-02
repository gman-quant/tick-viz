# tick-viz/src/utils/time_parser.py

from datetime import datetime
from dateutil import parser
from config import TAIWAN_TZ

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
        print(f"⚠️ 無法解析 datetime: {raw_dt}，錯誤: {e}")
        return None