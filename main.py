# tick-viz/main.py
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, TopicPartition

# 從設定檔導入參數
import config

# 導入各模組功能
from src.data_sourcing import fetch_ticks, market_data
from src.processing import volume_bars, metrics
from src.visualization import main_chart, candlestick_chart, stats_table, report_generator

def main():
    """主執行函式"""
    # ==== 1. 初始化 ====
    print("🚀 專案初始化...")
    
    # 建立 Kafka Consumer
    consumer = Consumer({
        'bootstrap.servers': config.KAFKA_BROKER,
        'group.id': config.KAFKA_GROUP_ID,
        'enable.auto.commit': False,
        'enable.partition.eof': True
    })

    # 查詢 Kafka 起始 offset
    start_dt_utc = config.START_DATETIME.astimezone(timezone.utc)
    timestamp_ms = int(start_dt_utc.timestamp() * 1000)
    metadata = consumer.list_topics(config.KAFKA_TOPIC)
    partitions = [TopicPartition(config.KAFKA_TOPIC, p, timestamp_ms) for p in metadata.topics[config.KAFKA_TOPIC].partitions.keys()]
    current_offsets = consumer.offsets_for_times(partitions)

    # 讀取前一日收盤價
    txf_prev_close, taiex_prev_close = market_data.find_previous_close(
        api_key=config.SHIOAJI_API_KEY, 
        secret_key=config.SHIOAJI_SECRET_KEY, 
        target_date=config.START_DATETIME.date()
    )

    # 初始化資料容器
    tick_dict = {}
    print("✅ 初始化完成。\n")

    # ==== 2. 主執行迴圈 ====
    # while True: # 若要實現即時更新，可取消註解此行與底部的 time.sleep
    try:
        
        # 設定目標結束時間
        end_datetime = config.END_DATETIME if config.USE_FIXED_END_TIME else datetime.now(tz=config.TAIWAN_TZ)
        print(f"模式: {'固定時間' if config.USE_FIXED_END_TIME else '即時'} | 目標結束時間: {end_datetime}")

        # 1. 從 Kafka 獲取 Tick 資料
        df, current_offsets = fetch_ticks.fetch_ticks_from_kafka(
            consumer=consumer, 
            offsets=current_offsets, 
            start_datetime=config.START_DATETIME, 
            end_datetime=end_datetime, 
            tick_dict=tick_dict
        )

        if df.empty:
            print("未獲取任何新資料，流程結束。")
            return # 或 continue in a loop

        # 2. 資料處理與分析
        print("🛠️ 開始進行資料處理與圖表繪製...")
        # (這裡省略了原始碼中 metrics 和 main_chart 的呼叫，因為它們被整合在 report_generator 中)
        df_vol_kbars = volume_bars.generate_volume_bars(df, volume_per_bar=config.VOLUME_PER_BAR)

        # 3. 產生視覺化元件
        stats_html = stats_table.generate_stats_html(df) # 假設已將 print_intraday_stats 改名
        fig_candlestick = candlestick_chart.plot_candlestick_with_volume_delta(df_vol_kbars)
        
        # 假設 plot_tick_analysis 已修改為只回傳 fig 物件
        fig_main_analysis = main_chart.plot_tick_analysis(df, txf_prev_close, taiex_prev_close)

        # 4. 生成 HTML 報告
        report_title = f"TXF-Charts_{config.START_DATETIME.strftime('%Y-%m-%d_%H%M')}"
        output_file = config.OUTPUT_DIR / f"{report_title}.html"
        
        report_generator.generate_html_report(
            figures=[fig_candlestick, fig_main_analysis],
            stats_html=stats_html,
            output_path=output_file,
            report_title=report_title,
            refresh_interval=config.REFRESH_INTERVAL_SECONDS
        )

    except Exception as e:
        print(f"❌ 發生未預期的錯誤: {e}")
    
    #     break # 如果在迴圈中，可以選擇中斷
    #     time.sleep(config.REFRESH_INTERVAL_SECONDS) # 等待下一輪更新

if __name__ == "__main__":
    main()