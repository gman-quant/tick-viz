# src/web/shared_state.py

# Standard Library Imports
import threading

# Third-Party Imports
import pandas as pd

# Local Application Imports
from config.config import DEFAULT_LOOKBACK_MINUTES, DEFAULT_TICK_WINDOW, DEFAULT_TIME_WINDOW
from config.run_context import RunContext

# ------------------------------------------------------------
# 📦 全域共用狀態管理 (Multithread-Safe)
# ------------------------------------------------------------
class SharedState:
    """
    一個集中管理全域狀態的類別，專門用於在 T_Data (資料執行緒) 
    和 MainThread (Dash Web 執行緒) 之間安全地交換資料。
    
    所有對 state 屬性的存取都應使用 self.lock 進行保護。
    """
    def __init__(self):
        self.lock = threading.Lock()
        
        # --- Context ---
        self.context:      RunContext = RunContext()
        
        # --- Dataframes ---
        self.latest_df:       pd.DataFrame | None = None # 原始 Tick DataFrame
        self.plot_df:         pd.DataFrame | None = None # 包含衍生指標的 DataFrame
        self.active_kbars_df: pd.DataFrame | None = None
        
        # --- Market Data ---
        self.txf_prev_close:    float | None = None
        self.taiex_prev_close:  float | None = None
        
        # --- (NEW) UI 狀態 ---
        # (這個值將由 Dash UI (滑桿) 來寫入)
        self.ui_lookback_minutes: int = DEFAULT_LOOKBACK_MINUTES

        # (可由 UI 動態調整的策略參數)
        self.param_tick_window: int = DEFAULT_TICK_WINDOW
        self.param_time_window: str = DEFAULT_TIME_WINDOW


# --- 建立全域單例 (Singleton) ---
shared_state = SharedState()