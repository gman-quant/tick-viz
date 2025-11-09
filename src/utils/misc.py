# src/utils/misc.py

# Standard Library Imports
import os

# ------------------------------------------------------------
# 📦 雜項工具函式
# ------------------------------------------------------------
def clear_console():
    """清除終端機畫面，Windows 用 cls，其他用 clear"""
    os.system('cls' if os.name == 'nt' else 'clear')