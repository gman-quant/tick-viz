# config/run_context.py

# Standard Library Imports
from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

# Local Application Imports
from config.config import TAIWAN_TZ
from config.types import SessionType, DataSource
from src.processing.bars.volume_bars import get_volume_per_bar
from src.utils.session_time import get_session_datetime_range, in_which_session

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
            start_dt, end_dt = get_session_datetime_range(
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
    # 📦 (Method) 取得「靜態報告」用的 Context
    # ------------------------------------------------------------
    def as_static_report_context(self) -> "RunContext":
        """
        (Immutable - 專用函式)
        建立一個「新的」、專用於「靜態報告」的 RunContext 實例。

        此函式只做一件事：將 real_time_mode 設為 False。
        
        (這會自動觸發 get_time_range (in figure_utils) 
        使用「完整盤勢」的「固定視窗」，而非「即時」的「滑動視窗」。)
        """
        
        # --- 回傳一個「新的」實例，只修改 real_time_mode ---
        return RunContext(
            real_time_mode=False, # <--- 唯一的關鍵變更
            
            # (所有其他欄位 100% 複製 'self' 的現有值)
            data_source=self.data_source,
            tz=self.tz,
            trade_date=self.trade_date,
            session_type=self.session_type, 
            start_datetime=self.start_datetime, # <--- 複製現有值
            end_datetime=self.end_datetime     # <--- 複製現有值
        )
