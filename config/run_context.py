# config/run_context.py


from dataclasses import dataclass, field
from datetime import date, datetime
from zoneinfo import ZoneInfo
from typing import Optional

from config.config import TAIWAN_TZ
from config.types import SessionType, DataSource
from src.processing.volume_bars import get_volume_per_bar
from src.utils.session_time import get_trading_session

@dataclass(frozen=True)
class RunContext:
    auto_refresh: bool = True
    real_time_mode: bool = True
    trade_date: date = field(default_factory=date.today)
    session_type: SessionType = SessionType.UNKNOWN
    data_source: DataSource = DataSource.KAFKA
    tz: ZoneInfo = TAIWAN_TZ

    # 使用 Optional，允許使用者手動輸入，也可留空
    start_datetime: Optional[datetime] = None
    end_datetime: Optional[datetime] = None

    def __post_init__(self):
        if self.start_datetime is None or self.end_datetime is None:
            start_dt, end_dt = get_trading_session(
                self.trade_date, self.session_type, self.real_time_mode, self.tz
            )
            object.__setattr__(self, 'start_datetime', start_dt)
            object.__setattr__(self, 'end_datetime', end_dt)

    @property
    def volume_per_bar(self) -> int:
        return get_volume_per_bar(self.session_type)

    @property
    def report_title(self) -> str:
        if self.real_time_mode and self.auto_refresh:
            return "TXF-Charts-Live"
        elif self.real_time_mode:
            return "TXF-Charts-Live-Static"
        else:
            session_flag = "1" if self.session_type == SessionType.DAY else "2"
            return f"TXF-Charts_{self.trade_date.strftime('%Y-%m-%d')}_{session_flag}_{self.data_source.value}"

    def with_updated(self, **kwargs) -> "RunContext":
        new_auto_refresh = kwargs.get("auto_refresh", self.auto_refresh)
        new_real_time_mode = kwargs.get("real_time_mode", self.real_time_mode)
        new_trade_date = kwargs.get("trade_date", self.trade_date)
        new_session_type = kwargs.get("session_type", self.session_type)
        new_data_source=kwargs.get("data_source", self.data_source)
        new_tz = kwargs.get("tz", self.tz)

        # 判斷是否需要重算 start/end datetime
        need_recompute_time = any([
            "trade_date" in kwargs,
            "session_type" in kwargs,
            "real_time_mode" in kwargs,
            "tz" in kwargs,
        ])

        if need_recompute_time:
            start_dt, end_dt = get_trading_session(
                new_trade_date, new_session_type, new_real_time_mode, new_tz
            )
        else:
            start_dt = self.start_datetime
            end_dt = self.end_datetime

        return RunContext(
            auto_refresh=new_auto_refresh,
            real_time_mode=new_real_time_mode,
            trade_date=new_trade_date,
            session_type=new_session_type,
            data_source=new_data_source,
            tz=new_tz,
            start_datetime=start_dt,
            end_datetime=end_dt
        )

