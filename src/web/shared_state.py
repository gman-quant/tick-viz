# src/web/shared_state.py

import threading
import pandas as pd

from config.run_context import RunContext

class SharedState:
    def __init__(self):
        self.lock = threading.Lock()
        self.context: RunContext = RunContext()
        self.latest_df: pd.DataFrame | None = None
        self.txf_prev_close: float | None = None
        self.taiex_prev_close: float | None = None
        self.plot_df: pd.DataFrame | None = None
        self.kbars_1min: pd.DataFrame | None = None


shared_state = SharedState()

