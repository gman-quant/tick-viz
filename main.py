# tick-viz/main.py
import os
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
    partitions = list(metadata.topics[config.KAFKA_TOPIC].partitions.keys())
    topic_partitions = [TopicPartition(config.KAFKA_TOPIC, p, timestamp_ms) for p in partitions]
    fixed_offsets = consumer.offsets_for_times(topic_partitions)
    current_offsets = fixed_offsets.copy()

    # 讀取前一日收盤價
    txf_prev_close, taiex_prev_close = market_data.find_previous_close(
        api_key=config.SHIOAJI_API_KEY, 
        secret_key=config.SHIOAJI_SECRET_KEY, 
        target_date=config.START_DATETIME.date()
    )

    # 初始化資料容器
    df = None
    tick_dict = {}
    print("✅ 初始化完成。\n")

    # ==== 2. 主執行迴圈 ====
    # while True: # 若要實現即時更新，可取消註解此行與底部的 time.sleep
    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        
        # 設定目標結束時間
        end_datetime = config.END_DATETIME if config.USE_FIXED_END_TIME else datetime.now(tz=config.TAIWAN_TZ)
        print(f"模式: {'固定時間' if config.USE_FIXED_END_TIME else '即時'} | 目標結束時間: {end_datetime}")

        try:
            # 1. 獲取 Tick 資料
            if config.USE_FIXED_END_TIME:
                # 從 Shioaji
                df = fetch_ticks.fetch_ticks_from_shioaji(
                    api_key=config.SHIOAJI_API_KEY, 
                    secret_key=config.SHIOAJI_SECRET_KEY, 
                    start_datetime=config.START_DATETIME, 
                    end_datetime=end_datetime
                )
            else:
                # 從 Kafka
                df, current_offsets = fetch_ticks.fetch_ticks_from_kafka(
                    consumer=consumer, 
                    offsets=current_offsets, 
                    start_datetime=config.START_DATETIME, 
                    end_datetime=end_datetime, 
                    tick_dict=tick_dict
                )

            if not df.empty:
                print("資料獲取完畢,準備繪圖...\n")

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
                output_file = config.OUTPUT_DIR / f"{config.REPORT_TITLE}.html"
                
                report_generator.generate_html_report(
                    figures=[fig_candlestick, fig_main_analysis],
                    stats_html=stats_html,
                    output_path=output_file,
                    report_title=config.REPORT_TITLE,
                    refresh_interval=config.REFRESH_INTERVAL_SECONDS
                )
            else:
                print("⚠️ 未獲取到任何資料，請檢查時間範圍或資料來源。")

        except Exception as e:
            print(f"❌ 發生未預期的錯誤: {e}")

        if config.USE_FIXED_END_TIME:
            break
        
        time.sleep(config.REFRESH_INTERVAL_SECONDS - 3)

        

if __name__ == "__main__":
    main()