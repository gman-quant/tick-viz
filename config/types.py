# config/types.py


from enum import Enum

class SessionType(Enum):
    UNKNOWN = "unknown"
    DAY = "day"
    NIGHT = "night"

class DataSource(Enum):
    KAFKA = "kafka"
    SHIOAJI = "shioaji"

class ReportMode(Enum):
    LIVE = "live"
    HISTORY = "history"
