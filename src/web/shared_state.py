# src/web/shared_state.py

import threading
import pandas as pd

class SharedState:
    def __init__(self):
        self.lock = threading.Lock()
        self.latest_df: pd.DataFrame | None = None
        self.txf_prev_close: float | None = None
        self.taiex_prev_close: float | None = None

        # 【新增】存放預先計算好的資料
        self.plot_df: pd.DataFrame | None = None
        self.kbars_1min: pd.DataFrame | None = None


shared_state = SharedState()