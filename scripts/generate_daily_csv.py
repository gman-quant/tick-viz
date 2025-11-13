# scripts/generate_daily_csv.py

# Standard Library Imports
from datetime import datetime
import re

# Third-Party Imports
import polars as pl

# Local Application Imports
from src.utils.session_time import in_which_session
from config.config import CACHE_DIR

# ------------------------------------------------------------
# 1. 設定 (路徑, 正規表示式)
# ------------------------------------------------------------
DAILY_CSV_PATH = CACHE_DIR / "daily_txf.csv"

# (用於找出 Parquet 檔中的日期)
PARQUET_PATTERN = re.compile(r"txf-ticks_(\d{4}-\d{2}-\d{2})\.parquet")

# ------------------------------------------------------------
# 2. (Polars) Tick 轉日線/夜盤 (OHLCV + VWAP)
# ------------------------------------------------------------
def convert_tick_to_daily(df_tick: pl.DataFrame, file_date: str) -> pl.DataFrame:
    """
    將單一 Parquet 檔案的 Tick 資料，轉換為日盤/夜盤的 K 線 summary。
    """
    
    # --- (A) 欄位前處理 ---
    df = df_tick.with_columns([
        pl.col("close").alias("price"),
        # (判斷 Tick 屬於日盤、夜盤或休市)
        pl.col("datetime").map_elements(
            lambda x: in_which_session(x).value,
            return_dtype=pl.String
        ).alias("session"),
        (pl.col("close") * pl.col("volume")).alias("pv")  # (為了計算 VWAP)
    ])
    
    # --- (B) 依盤別 (session) 分組 ---
    df_daily = df.group_by("session").agg([
        pl.first("datetime").dt.date().alias("date"),
        pl.first("price").alias("open"),
        pl.max("price").alias("high"),
        pl.min("price").alias("low"),
        pl.last("price").alias("close"),
        (pl.sum("pv") / pl.sum("volume")).alias("vwap"), # (VWAP)
        pl.sum("volume").alias("volume"),
    ]).with_columns([
        pl.lit(file_date).alias("file_date") # (標記來源 Parquet 檔案日期)
    ])

    # --- (C) 整理欄位順序 ---
    return df_daily.select(["file_date", "date", "session", "open", "high", "low", "close", "vwap", "volume"])


# ------------------------------------------------------------
# 3. 主處理流程 (Script Entry)
# ------------------------------------------------------------
def process_all_ticks():
    """
    讀取所有 Tick Parquet 檔案，並將其增量更新至
    """
    
    # --- (A) 讀取已存在的日線 CSV ---
    # (若無則建立空的 DataFrame)
    if DAILY_CSV_PATH.exists():
        df_existing = pl.read_csv(DAILY_CSV_PATH)
    else:
        df_existing = pl.DataFrame(schema={
            "file_date": pl.Utf8, "date": pl.Date, "session": pl.Utf8,
            "open": pl.Float64, "high": pl.Float64, "low": pl.Float64,
            "close": pl.Float64, "vwap": pl.Float64, "volume": pl.Int64,
        })

    # --- (B) 找出已處理的 file_date 清單 (排除今天) ---
    # (排除今天可確保今天不完整的資料能被重複處理)
    today_str = datetime.now().strftime("%Y-%m-%d")
    processed_dates = set(
        df_existing.filter(pl.col("file_date") != today_str)["file_date"].to_list()
    )

    # --- (C) 迭代 Parquet 檔, 處理新資料 ---
    all_new_rows = []
    for file in CACHE_DIR.glob("txf-ticks_*.parquet"):
        match = PARQUET_PATTERN.match(file.name)
        if not match:
            continue
        file_date = match.group(1)

        if file_date in processed_dates:
            continue  # (已處理過且非今日，跳過)

        # --- (執行轉換) ---
        df_tick = pl.read_parquet(file)
        df_daily = convert_tick_to_daily(df_tick, file_date)

        all_new_rows.extend(df_daily.to_dicts())
        processed_dates.add(file_date) # (加入，避免重複處理)

    if not all_new_rows:
        print("✅ 沒有新資料需要處理")
        return

    # --- (D) 合併、排序、存回 CSV ---
    df_new = pl.DataFrame(all_new_rows)
    
    # (確保新舊 DF 的 date 欄位都是 Date 型別)
    df_existing = df_existing.with_columns(pl.col("date").cast(pl.Date))
    df_new = df_new.with_columns(pl.col("date").cast(pl.Date))
    
    # (統一新 DF 的欄位型別, 避免合併出錯)
    for col, dtype in df_existing.schema.items():
        if col in df_new.columns:
            df_new = df_new.with_columns([pl.col(col).cast(dtype)])
    
    # (合併新舊資料)
    df_combined = pl.concat([df_existing, df_new])
    
    # (過濾掉 'closed' session)
    df_combined = df_combined.filter(pl.col("session") != "closed")
    
    # (去重並排序: 確保 day 在 night 之前)
    session_order = {"day": 0, "night": 1}
    df_sorted = (
        df_combined.unique(subset=["date", "session"])
        .with_columns(pl.col("session").map_elements(
            lambda s: session_order.get(s, 99),
            return_dtype=pl.Int64
        ).alias("session_order"))
        .sort(["date", "session_order"])
        .drop("session_order")
    )

    # --- (E) 存回 CSV ---
    df_sorted.write_csv(DAILY_CSV_PATH)
    print(f"✅ 新增並排序後，共有 {df_sorted.height}  B資料寫入 {DAILY_CSV_PATH}")

    # --- (F) (清理機制) 刪除今日不完整的 Tick 檔 ---
    # (若在 13:45 日盤收盤前執行，則今日的 Tick 檔不完整，應刪除)
    now = datetime.now()
    cutoff_time = now.replace(hour=13, minute=45, second=0, microsecond=0)

    if now < cutoff_time:
        today_str = now.strftime("%Y-%m-%d")
        today_tick_file = CACHE_DIR / f"txf-ticks_{today_str}.parquet"

        if today_tick_file.exists():
            today_tick_file.unlink()
            print(f"🗑️ 13:45 前刪除不完整 tick 檔：{today_tick_file.name}")


# ------------------------------------------------------------
# 4. 執行腳本
# ------------------------------------------------------------
if __name__ == "__main__":
    process_all_ticks()

'''
python -m scripts.generate_daily_csv
'''