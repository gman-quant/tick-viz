# src/web/shared_state.py

import threading

class SharedState:
    def __init__(self):
        self.lock = threading.Lock()
        self.latest_df = None
        self.txf_prev_close = None
        self.taiex_prev_close = None

shared_state = SharedState()
