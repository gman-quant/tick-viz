# src/web/shared_state.py

# Standard Library Imports
import threading

# Third-Party Imports
import pandas as pd

# Local Application Imports
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
        self.latest_df:  pd.DataFrame | None = None # 原始 Tick DataFrame
        self.plot_df:    pd.DataFrame | None = None # 包含衍生指標的 DataFrame
        self.kbars_1min: pd.DataFrame | None = None # 1 分 K
        
        # --- Market Data ---
        self.txf_prev_close:    float | None = None
        self.taiex_prev_close:  float | None = None

# --- 建立全域單例 (Singleton) ---
shared_state = SharedState()