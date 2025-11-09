# config/run_context.py

# Standard Library Imports
from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

# Local Application Imports
from config.config import TAIWAN_TZ
from config.types import SessionType, DataSource
from src.processing.bars.volume_bars import get_volume_per_bar
from src.utils.session_time import get_trading_session, in_which_session

# ------------------------------------------------------------
# 📦 執行環境 (Context)
# ------------------------------------------------------------
@dataclass(frozen=True)
class RunContext:
    """
    一個「不可變」(Immutable) 的資料類別，用於儲存當前執行的所有環境參數。
    (e.g., 執行日期, 盤別, 模式...)
    
    'frozen=True' 確保實例在建立後不會被意外修改。
    """
    real_time_mode:    bool = True
    data_source: DataSource = DataSource.KAFKA
    tz:            ZoneInfo = TAIWAN_TZ
    trade_date:          date | None = None
    session_type: SessionType | None = None 
    start_datetime:  datetime | None = None
    end_datetime:    datetime | None = None

    # ------------------------------------------------------------
    # 📦 (Dataclass) 初始化後自動填值
    # ------------------------------------------------------------
    def __post_init__(self):
        """
        在 Dataclass 初始化後自動執行，
        用於自動填補缺失的欄位 (e.g., trade_date, session_type, start/end times)。
        
        (使用 object.__setattr__ 是在 'frozen' dataclass 中設定欄位的正確做法)
        """
        
        # --- 1. 自動填入 trade_date (若為 None) ---
        if self.trade_date is None:
            today = datetime.now(self.tz).date()
            object.__setattr__(self, 'trade_date', today)
            
        # --- 2. 自動填入 session_type (若為 None 且為即時模式) ---
        if self.real_time_mode and self.session_type is None:
            session_type = in_which_session()
            object.__setattr__(self, 'session_type', session_type)
            
        # --- 3. 自動計算 start/end datetime (若為 None) ---
        if self.start_datetime is None or self.end_datetime is None:
            start_dt, end_dt = get_trading_session(
                self.trade_date, self.session_type, self.real_time_mode, self.tz
            )
            object.__setattr__(self, 'start_datetime', start_dt)
            object.__setattr__(self, 'end_datetime', end_dt)

    # ------------------------------------------------------------
    # 📦 (Property) 衍生屬性
    # ------------------------------------------------------------
    @property
    def volume_per_bar(self) -> int:
        """(衍生) 根據盤別回傳 Volume Bar 的基準量"""
        return get_volume_per_bar(self.session_type)

    @property
    def report_title(self) -> str:
        """(衍生) 根據 context (即時/歷史/日期) 生成標準化的報告標題。"""
        if self.real_time_mode:
            return "TXF-Charts-Live-Static"
        else:
            session_flag = "1" if self.session_type == SessionType.DAY else "2"
            return f"TXF-Charts_{self.trade_date.strftime('%Y-%m-%d')}_{session_flag}_{self.data_source.value}"

    # ------------------------------------------------------------
    # 📦 (Method) 不可變更新 (Immutable Update)
    # ------------------------------------------------------------
    def with_updated(self, **kwargs) -> "RunContext":
        """
        (Immutable) 
        建立一個「新的」 RunContext 實例，並更新指定的值。
        
        這是 'frozen' class 必要的更新模式。
        它會自動重算 start/end datetime (如果相關欄位被變更)。
        """
        
        # --- 1. 取得新值 (若無則用舊值) ---
        new_real_time_mode = kwargs.get("real_time_mode", self.real_time_mode)
        new_trade_date = kwargs.get("trade_date", self.trade_date)
        new_session_type = kwargs.get("session_type", self.session_type)
        new_data_source=kwargs.get("data_source", self.data_source)
        new_tz = kwargs.get("tz", self.tz)

        # --- 2. 判斷是否需要重算 start/end datetime ---
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

        # --- 3. 回傳「新的」實例 ---
        return RunContext(
            real_time_mode=new_real_time_mode,
            trade_date=new_trade_date,
            session_type=new_session_type,
            data_source=new_data_source,
            tz=new_tz,
            start_datetime=start_dt,
            end_datetime=end_dt
        )
    
