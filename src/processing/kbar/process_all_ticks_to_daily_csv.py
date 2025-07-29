# src/processing/kbar/process_all_ticks_to_daily_csv.py

from datetime import datetime
import polars as pl
from pathlib import Path
import re
from src.utils.session_time import in_which_session

# --- 設定路徑 ---
DATA_DIR = Path("data")
DAILY_CSV_PATH = Path("data/txf_daily.csv")

# --- 正規表示式找出日期 ---
PARQUET_PATTERN = re.compile(r"txf-ticks_(\d{4}-\d{2}-\d{2})\.parquet")

# --- Tick 轉日線（含分日夜盤） ---
def convert_tick_to_daily(df_tick: pl.DataFrame, file_date: str) -> pl.DataFrame:
    df = df_tick.with_columns([
        pl.col("close").alias("price"),
        pl.col("datetime").map_elements(
            lambda x: in_which_session(x.time()).value,
            return_dtype=pl.String
        ).alias("session")
    ])

    df_daily = df.group_by("session").agg([
        pl.first("datetime").dt.date().alias("date"),
        pl.first("price").alias("open"),
        pl.max("price").alias("high"),
        pl.min("price").alias("low"),
        pl.last("price").alias("close"),
        pl.sum("volume").alias("volume")
    ]).with_columns([
        pl.lit(file_date).alias("file_date")
    ])

    return df_daily.select(["file_date", "date", "session", "open", "high", "low", "close", "volume"])


# --- 主處理流程 ---
def process_all_ticks():
    # 1. 讀取已存在的日線CSV（若無則建立空的DataFrame）
    if DAILY_CSV_PATH.exists():
        df_existing = pl.read_csv(DAILY_CSV_PATH)
    else:
        df_existing = pl.DataFrame(schema={
            "file_date": pl.Utf8,
            "date": pl.Date,
            "session": pl.Utf8,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
        })

    # 2. 找出已處理的 file_date 清單（排除今天）
    today_str = datetime.now().strftime("%Y-%m-%d")
    processed_dates = set(
        df_existing.filter(pl.col("file_date") != today_str)["file_date"].to_list()
    )

    # 3. 開始處理 parquet 檔
    all_new_rows = []
    for file in DATA_DIR.glob("txf-ticks_*.parquet"):
        match = PARQUET_PATTERN.match(file.name)
        if not match:
            continue
        file_date = match.group(1)

        if file_date in processed_dates:
            continue  # 已處理，跳過

        df_tick = pl.read_parquet(file)
        df_daily = convert_tick_to_daily(df_tick, file_date)

        all_new_rows.extend(df_daily.to_dicts())
        processed_dates.add(file_date)

    if not all_new_rows:
        print("✅ 沒有新資料需要處理")
        return

    # 4. 合併並排序
    df_new = pl.DataFrame(all_new_rows)
    
    df_existing = df_existing.with_columns(pl.col("date").cast(pl.Date))
    df_new = df_new.with_columns(pl.col("date").cast(pl.Date))

    df_combined = pl.concat([df_existing, df_new])

    session_order = {"day": 0, "night": 1}
    df_sorted = (
        df_combined.unique(subset=["date", "session"])
        .with_columns(pl.col("session").map_elements(lambda s: session_order.get(s, 99)).alias("session_order"))
        .sort(["date", "session_order"])
        .drop("session_order")
    )

    # 5. 存回 CSV
    df_sorted.write_csv(DAILY_CSV_PATH)
    print(f"✅ 新增並排序後，共有 {df_sorted.height} 筆資料寫入 {DAILY_CSV_PATH}")

    # 6. 若當前時間早於 13:45，且已有今天的 ticks 檔案，則刪除
    now = datetime.now()
    cutoff_time = now.replace(hour=13, minute=45, second=0, microsecond=0)

    if now < cutoff_time:
        today_str = now.strftime("%Y-%m-%d")
        today_tick_file = DATA_DIR / f"txf-ticks_{today_str}.parquet"

        if today_tick_file.exists():
            today_tick_file.unlink()
            print(f"🗑️ 13:45 前刪除不完整 tick 檔：{today_tick_file.name}")


if __name__ == "__main__":
    process_all_ticks()

'''
python -m src.processing.kbar.process_all_ticks_to_daily_csv
'''