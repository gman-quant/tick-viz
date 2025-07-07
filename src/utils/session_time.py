# tick-viz/src/utils/session_time.py

from datetime import datetime, time as dt_time, timedelta, date
from zoneinfo import ZoneInfo

def get_datetimes(trade_date: date, is_day_session: bool, tz: ZoneInfo) -> tuple[datetime, datetime]:
    """
    Return start and end datetime for the given trading session.

    Args:
        trade_date: The trading date.
        is_day_session: True for day session, False for night session.
        tz: The timezone to assign.

    Returns:
        A tuple of (start_datetime, end_datetime) in the given timezone.
    """
    if is_day_session:
        end_date = trade_date
        start_time = dt_time(8, 30)
        end_time = dt_time(13, 45)
    else:
        end_date = trade_date + timedelta(days=1)
        start_time = dt_time(14, 50)
        end_time = dt_time(5, 0)

    start_dt = datetime.combine(trade_date, start_time).replace(tzinfo=tz)
    end_dt = datetime.combine(end_date, end_time).replace(tzinfo=tz)

    return start_dt, end_dt

def is_day_session(now_time: dt_time) -> bool:
    """
    Check whether the given time is in the day session.

    Returns:
        True if between 08:30 and 14:50, else False.
    """
    return dt_time(8, 30) <= now_time < dt_time(14, 50)

