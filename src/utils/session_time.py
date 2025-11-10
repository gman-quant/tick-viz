# src/utils/session_time.py

# Standard Library Imports
from datetime import datetime, time as dt_time, timedelta, date
from zoneinfo import ZoneInfo

# Third-Party Imports
import pandas as pd

# Local Application Imports
from config.config import (
    TAIWAN_TZ, 
    DAY_START, 
    DAY_END, 
    NIGHT_START, 
    NIGHT_END
)
from config.types import SessionType

# ------------------------------------------------------------
# 📦 交易時段 (Session) 計算
# ------------------------------------------------------------
def get_session_datetime_range(
    trade_date: date,
    session_type: SessionType = None,
    real_time_mode: bool = True,
    tz: ZoneInfo = TAIWAN_TZ
) -> tuple[datetime, datetime]:
    """
    根據交易日、盤別與即時模式，計算出正確的 session 開始與結束時間
    """
    
    # --- 1. 取得當前時間 (即時模式用) ---
    now_dt   = datetime.now(tz)
    now_time = now_dt.time()

    if real_time_mode and session_type is None:
        session_type = in_which_session(now_dt)

    start_date = end_date = trade_date

    # --- 2. 判斷日盤時段 ---
    if session_type == SessionType.DAY or (session_type == SessionType.CLOSED and DAY_START <= now_time < NIGHT_START):
        start_time, end_time = DAY_START, DAY_END
    
    # --- 3. 判斷夜盤時段 (需處理日期變換) ---
    else:
        start_time, end_time = NIGHT_START, NIGHT_END
        one_day = timedelta(days=1)

        if not real_time_mode:
            # 歷史模式：夜盤結束日在 T+1
            end_date += one_day
        elif now_time < start_time:
            # 即時模式 (午夜後)：夜盤開始日在 T-1
            start_date -= one_day
        else:
            # 即時模式 (午夜前)：夜盤結束日在 T+1
            end_date += one_day

    # --- 4. 組合回傳 ---
    start_dt = datetime.combine(start_date, start_time).replace(tzinfo=tz)
    end_dt   = datetime.combine(end_date, end_time).replace(tzinfo=tz)
    return start_dt, end_dt

# ------------------------------------------------------------
# 📦 盤別判斷 (v4, 邏輯重構版)
# ------------------------------------------------------------
def in_which_session(now_dt: datetime | None = None) -> SessionType:
    """
    根據當下「日期」和「時間」判斷是日盤、夜盤、或休市
    (已加入 "週" 的判斷，修復週日夜盤誤判問題)
    """
    if now_dt is None:
        now_dt = datetime.now(tz=TAIWAN_TZ)

    now_time    = now_dt.time()
    now_weekday = now_dt.weekday() # 0=Mon, 1=Tue, ..., 5=Sat, 6=Sun

    # --- 1. 處理週一至週五 (Mon-Fri) ---
    if 0 <= now_weekday <= 4: 
        # (A) 日盤 (08:30 - 13:45)
        if DAY_START <= now_time < DAY_END:
            return SessionType.DAY
        # (B) 夜盤 (下午時段) (14:50 - 23:59)
        if now_time >= NIGHT_START:
            return SessionType.NIGHT
    
    # --- 2. 處理凌晨時段 (週二 ~ 週六) (Tue-Sat) ---
    # (這屬於前一個交易日的夜盤: 00:00 - 05:00)
    if 1 <= now_weekday <= 5: 
        if now_time < NIGHT_END: 
            return SessionType.NIGHT

    # --- 3. 其他所有時間 (休市) ---
    # (含週六 05:00 後、所有週日、交易日的 5:00-8:30, 13:45-14:50)
    return SessionType.CLOSED

# ------------------------------------------------------------
# 📦 (歷史回測) 盤別範圍
# ------------------------------------------------------------
def get_session_range(pick: str) -> tuple[int, int]:
    """
    (歷史回測用) 根據 'day', 'night', 'whole' 回傳迭代範圍
    """
    mapping = {
        'day': (1, 1),
        'night': (0, 0),
        'whole': (1, 0),
    }
    return mapping.get(pick.lower(), (0, 1))

# ------------------------------------------------------------
# 📦 繪圖時間視窗計算
# ------------------------------------------------------------
def get_observation_window(df: pd.DataFrame, start: datetime) -> tuple[datetime, datetime]:
    """
    (繪圖用) 計算完整的「固定」觀察視窗
    (從開盤後 N 分鐘，到最後一筆 tick)
    """
    adjusted_start = start + timedelta(minutes=15 if start.time() == DAY_START else 10)
    end = df['datetime'].iloc[-1] if not df.empty else adjusted_start
    return adjusted_start, end


def get_sliding_window(
    df: pd.DataFrame,
    start: datetime,
    lookback_minutes: int = 30,
) -> tuple[datetime, datetime]:
    """
    (繪圖用) 計算「滑動」時間視窗
    (從最後一筆 tick 往前推 N 分鐘)
    """
    adjusted_start = start + timedelta(minutes=15 if start.time() == DAY_START else 10)
    end = df['datetime'].iloc[-1] if not df.empty else adjusted_start
    
    # 確保視窗起點不會早於 adjusted_start
    window_start = max(adjusted_start, end - timedelta(minutes=lookback_minutes))
    return window_start, end