# src/utils/run_context.py


from datetime import datetime, date
from src.utils.session_time import get_trading_session
from src.processing.volume_bars import get_volume_per_bar

from config import TAIWAN_TZ

class RunContext:
    def __init__(self, is_real_time_mode: bool, trade_date: date, day_session: int, data_source: str):
        self.is_real_time_mode = is_real_time_mode
        self.trade_date = trade_date
        self.day_session = day_session
        self.data_source = data_source

    @property
    def trading_session(self) -> tuple[datetime, datetime]:
        """回傳 (start_datetime, end_datetime)"""
        return get_trading_session(self.trade_date, self.day_session, self.is_real_time_mode, TAIWAN_TZ)

    @property
    def start_datetime(self) -> datetime:
        return self.trading_session[0]

    @property
    def end_datetime(self) -> datetime:
        return self.trading_session[1]

    @property
    def volume_per_bar(self) -> int:
        return get_volume_per_bar(self.day_session)

    @property
    def report_title(self) -> str:
        session_flag = "1" if self.day_session else "2"
        date_str = self.start_datetime.strftime("%Y-%m-%d")
        return f"TXF-Charts_{session_flag}_{date_str}_{self.data_source}"
