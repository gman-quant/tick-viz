# tick-viz/src/utils/session_time.py

from datetime import datetime, time as dt_time, timedelta, date
from zoneinfo import ZoneInfo


def get_datetimes(trade_date: date, is_day_session: bool, is_real_time_mode: bool, tz: ZoneInfo) -> tuple[datetime, datetime]:
    """
    Generate the start and end datetime for a specific trading session.

    Args:
        trade_date: The base trading date.
        is_day_session: True for day session (08:30–13:45), False for night session (14:50–05:00 next day).
        is_real_time_mode: Determines how to adjust dates in night sessions.
            - If False: Always assume night session spans to the next calendar day.
            - If True: Adjust session based on current time (for real-time alignment).
        tz: Timezone to localize the returned datetimes.

    Returns:
        A tuple (start_datetime, end_datetime) localized to the given timezone.
    """
    start_date = end_date = trade_date
    start_time = end_time = None

    if is_day_session:
        # Day session: Same-day range from 08:30 to 13:45
        start_time, end_time = dt_time(8, 30), dt_time(13, 45)
    else:
        # Night session: From 14:50 to 05:00 (spans two calendar days)
        start_time, end_time = dt_time(14, 50), dt_time(5, 0)
        one_day = timedelta(days=1)

        if not is_real_time_mode:
            # Static mode: Always treat night session as spanning two days
            end_date += one_day
        else:
            # Real-time mode: Adjust date boundary depending on current time
            now_time = datetime.now().time()
            if now_time < start_time:
                # Before night session starts (early morning): previous day's night session
                start_date -= one_day
            else:
                # After night session starts: end date rolls into next day
                end_date += one_day

    # Combine date and time, and apply timezone
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

def get_range(st_dt: datetime, ed_dt: datetime, tz: ZoneInfo) -> tuple[datetime, datetime]:
    new_st_dt = st_dt
    new_ed_dt = datetime.now(tz=tz) + timedelta(hours=1)
    if st_dt.time() == dt_time(8, 30):
        new_st_dt += timedelta(minutes=15)
    else:
        new_st_dt += timedelta(minutes=10)

    if new_ed_dt > ed_dt:
        new_ed_dt = ed_dt

    return new_st_dt, new_ed_dt

