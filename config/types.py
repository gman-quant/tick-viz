# config/types.py

# Standard Library Imports
from enum import Enum

# ------------------------------------------------------------
# 📦 盤別類型 (Enum)
# ------------------------------------------------------------
class SessionType(Enum):
    CLOSED = "closed"
    DAY = "day"
    NIGHT = "night"

# ------------------------------------------------------------
# 📦 資料來源類型 (Enum)
# ------------------------------------------------------------
class DataSource(Enum):
    KAFKA = "kafka"
    SHIOAJI = "shioaji"

