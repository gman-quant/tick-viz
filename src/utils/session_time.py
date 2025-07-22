# src/utils/session_time.py

from datetime import datetime, time as dt_time, timedelta, date
from zoneinfo import ZoneInfo

from config.config import TAIWAN_TZ
from config.types import SessionType

def get_trading_session(
    trade_date: date,
    session_type: SessionType = SessionType.UNKNOWN,
    real_time_mode: bool = True,
    tz: ZoneInfo = TAIWAN_TZ
) -> tuple[datetime, datetime]:
    """
    根據交易日、盤別與即時模式，計算出正確的 session 開始與結束時間
    """
    now = datetime.now(tz)
    now_time = now.time()

    if real_time_mode or session_type == SessionType.UNKNOWN:
        session_type = in_which_session(now_time)

    start_date = end_date = trade_date

    if session_type == SessionType.DAY:
        start_time, end_time = dt_time(8, 30), dt_time(13, 46)
    else:
        start_time, end_time = dt_time(14, 50), dt_time(5, 1)
        one_day = timedelta(days=1)

        if not real_time_mode:
            end_date += one_day
        elif now_time < start_time:
            start_date -= one_day
        else:
            end_date += one_day

    start_dt = datetime.combine(start_date, start_time).replace(tzinfo=tz)
    end_dt   = datetime.combine(end_date, end_time).replace(tzinfo=tz)
    return start_dt, end_dt

def in_which_session(now_time: dt_time) -> SessionType:
    """
    根據當下時間判斷是日盤還是夜盤
    """
    return SessionType.DAY if dt_time(8, 30) <= now_time < dt_time(14, 50) else SessionType.NIGHT

def get_session_range(pick: str) -> tuple[int, int]:
    """
    根據輸入的 pick 字串，回傳對應的 session 範圍。
    - 'day'   → (1, 1)
    - 'night' → (0, 0)
    - 'whole' → (1, 0)

    若輸入無效，預設回傳 (0, 1)（即 whole）。

    Args:
        pick (str): 指定要處理的時段類型。

    Returns:
        tuple[int, int]: session 起迄 index
    """
    mapping = {
        'day': (1, 1),
        'night': (0, 0),
        'whole': (1, 0),
    }
    return mapping.get(pick.lower(), (0, 1))  # 忽略大小寫

def get_observation_window(start: datetime, end: datetime, tz: ZoneInfo) -> tuple[datetime, datetime]:
    """
    根據起始時間自動調整觀察區間，排除開盤雜訊或初期波動。
    """
    now = datetime.now(tz=tz)
    adjustment = timedelta(minutes=15 if start.time() == dt_time(8, 30) else 10)
    adjusted_start = start + adjustment
    adjusted_end = min(end, now + timedelta(hours=1))
    return adjusted_start, adjusted_end

def get_sliding_window(
    start: datetime,
    end: datetime,
    tz: ZoneInfo,
    lookback_minutes: int = 120,
    lookahead_minutes: int = 60
) -> tuple[datetime, datetime]:
    """
    動態滑動時間窗：根據現在時間往前 / 往後推移，限制在 [start, end] 區間。
    """
    now = datetime.now(tz=tz)
    adjustment = timedelta(minutes=15 if start.time() == dt_time(8, 30) else 10)
    adjusted_start = start + adjustment
    window_start = max(adjusted_start, now - timedelta(minutes=lookback_minutes))
    window_end = min(end, now + timedelta(minutes=lookahead_minutes))
    return window_start, window_end
