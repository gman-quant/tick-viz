# tick-viz/src/utils/session_time.py

from datetime import datetime, time as dt_time, timedelta, date
from zoneinfo import ZoneInfo


def get_trading_session(trade_date: date, day_session: bool = None, is_real_time_mode: bool = True, tz: ZoneInfo = None) -> tuple[datetime, datetime]:
    start_date = end_date = trade_date
    start_time = end_time = None
    now_time = datetime.now().time()

    if is_real_time_mode or day_session is None:
        day_session = is_day_session(now_time)

    if day_session:
        start_time, end_time = dt_time(8, 30), dt_time(13, 46)
    else:
        start_time, end_time = dt_time(14, 50), dt_time(5, 1)
        one_day = timedelta(days=1)

        if not is_real_time_mode:
            end_date += one_day
        else:
            if now_time < start_time:
                start_date -= one_day
            else:
                end_date += one_day

    start_dt = datetime.combine(start_date, start_time).replace(tzinfo=tz)
    end_dt   = datetime.combine(end_date, end_time).replace(tzinfo=tz)

    return start_dt, end_dt


def is_day_session(now_time: dt_time) -> bool:
    """
    Check whether the given time is in the day session.

    Returns:
        True if between 08:30 and 14:50, else False.
    """
    return dt_time(8, 30) <= now_time < dt_time(14, 50)

def get_observation_window(start: datetime, end: datetime, tz: ZoneInfo) -> tuple[datetime, datetime]:
    """
    根據觀察邏輯調整時間範圍：
    - 若起始時間為 08:30，跳過開盤高雜訊，往後推 15 分鐘
    - 其餘情況則推 10 分鐘
    - 結束時間為「現在時間 + 1 小時」與原始 end 的最小值
    """
    now = datetime.now(tz=tz)
    if start.time() == dt_time(8, 30):
        adjusted_start = start + timedelta(minutes=15)
    else:
        adjusted_start = start + timedelta(minutes=10)
    
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
    取得以現在時間為中心的區間：
    - 往前推 `lookback_minutes` 分鐘作為起點
    - 往後推 `lookahead_minutes` 分鐘作為終點
    - 並限制在 [start, end] 範圍內
    """
    now = datetime.now(tz=tz)
    window_start = max(start, now - timedelta(minutes=lookback_minutes))
    window_end = min(end, now + timedelta(minutes=lookahead_minutes))
    return window_start, window_end

