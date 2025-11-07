# src/utils/session_time.py (v2, 重構版)

from datetime import datetime, time as dt_time, timedelta, date
from zoneinfo import ZoneInfo

from config.config import (
    TAIWAN_TZ, 
    DAY_START, 
    DAY_END, 
    NIGHT_START, 
    NIGHT_END
)
from config.types import SessionType

def get_trading_session(
    trade_date: date,
    session_type: SessionType = None,
    real_time_mode: bool = True,
    tz: ZoneInfo = TAIWAN_TZ
) -> tuple[datetime, datetime]:
    """
    根據交易日、盤別與即時模式，計算出正確的 session 開始與結束時間
    """
    now = datetime.now(tz)
    now_time = now.time()

    if real_time_mode and session_type is None:
        session_type = in_which_session(now_time)

    start_date = end_date = trade_date

    if session_type == SessionType.DAY or (session_type == SessionType.CLOSED and DAY_START <= now_time < NIGHT_START):
        start_time, end_time = DAY_START, DAY_END
    else:
        start_time, end_time = NIGHT_START, NIGHT_END
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

def in_which_session(now_time: dt_time | None = None) -> SessionType:
    """
    根據當下時間判斷是日盤、夜盤、或休市
    (已加入 SessionType.CLOSED 邏輯)
    """
    if now_time is None:
        now_time = datetime.now(tz=TAIWAN_TZ).time()

    if DAY_START <= now_time < DAY_END:
        return SessionType.DAY
    elif NIGHT_START <= now_time or now_time < NIGHT_END:
        return SessionType.NIGHT
    else:
        return SessionType.CLOSED

def get_session_range(pick: str) -> tuple[int, int]:
    """
    (此函式不變)
    """
    mapping = {
        'day': (1, 1),
        'night': (0, 0),
        'whole': (1, 0),
    }
    return mapping.get(pick.lower(), (0, 1))

def get_observation_window(start: datetime, end: datetime, tz: ZoneInfo) -> tuple[datetime, datetime]:
    """
    完整的開始與結束時間，固定在 start 與 end
    """
    now = datetime.now(tz=tz)
    adjustment = timedelta(minutes=15 if start.time() == DAY_START else 10)
    adjusted_start = start + adjustment
    adjusted_end = min(end, now + timedelta(minutes=0))
    return adjusted_start, adjusted_end


def get_sliding_window(
    start: datetime,
    end: datetime,
    tz: ZoneInfo,
    lookback_minutes: int = 30,
) -> tuple[datetime, datetime]:
    """
    根據當前時間計算滑動視窗：
    - 若尚未收盤，依 now 做滑動
    - 若已收盤，視窗直接固定在 start 與 end
    """
    now = datetime.now(tz=tz)
    adjusted_start = start + timedelta(minutes=15 if start.time() == DAY_START else 10)
    if now > end:
        # 已收盤 → 固定範圍
        return adjusted_start, end
    # 尚未收盤 → 正常滑動邏輯
    window_start = max(adjusted_start, now - timedelta(minutes=lookback_minutes))
    return window_start, now


