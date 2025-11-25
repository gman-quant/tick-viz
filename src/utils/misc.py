# src/utils/misc.py

# Standard Library Imports
import os

# ------------------------------------------------------------
# 📦 雜項工具函式
# ------------------------------------------------------------
def clear_console():
    """清除終端機畫面，Windows 用 cls，其他用 clear"""
    os.system('cls' if os.name == 'nt' else 'clear')

# ------------------------------------------------------------
# 📦 合約物件
# ------------------------------------------------------------
def get_contract(api, symbol: str):
    """取得 Shioaji 合約物件"""
    if symbol == "txf":
        return api.Contracts.Futures.TXF.TXFR1
    elif symbol == "tse":
        return api.Contracts.Indexs.TSE.TSE001
    else:
        raise ValueError(f"Unsupported symbol: {symbol}")