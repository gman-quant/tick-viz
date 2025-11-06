# config/types.py


from enum import Enum

class SessionType(Enum):
    CLOSED = "closed"
    DAY = "day"
    NIGHT = "night"

class DataSource(Enum):
    KAFKA = "kafka"
    SHIOAJI = "shioaji"

class ReportMode(Enum):
    LIVE = "live"
    HISTORY = "history"
