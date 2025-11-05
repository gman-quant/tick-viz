# 📈 台指期貨盤中動態分析儀表板 (TXF Intraday Analytics Dashboard)

![Python](https://img.shields.io/badge/python-3.9%2B-blue) 
![Apache Kafka](https://img.shields.io/badge/Kafka-required-orange) 
![Shioaji](https://img.shields.io/badge/Shioaji-required-orange) 
![License: MIT](https://img.shields.io/badge/License-MIT-green)

本專案專注於台指期的即時分析與監控，打造高效能可視化儀表板。它能即時消費 Kafka 中的 Tick 數據流，或回測 Shioaji 的歷史資料，透過多維度指標視覺化盤中多空狀態。

核心架構將「後端資料運算」與「前端 UI 渲染」徹底分離，確保即時儀表板在大量資料更新下依然保持流暢不卡頓。

---

## ✨ 主要功能

-   **雙模式數據源**：支援從 `Kafka` 即時消費 tick 數據，或透過 `Shioaji API` 抓取歷史 tick 數據進行分析。
-   **高效能即時架構**：採用多執行緒模型，將資料處理 (data_loop) 與 UI 渲染 (dash_app) 分離，確保即時儀表板流暢高效。
-   **多維度指標計算**：即時計算技術分析指標 **VWAP**、**High&Low**、**淨成交強度指標**等，可依需求自行設計。
-   **進階圖表視覺化**：
    -   **主分析圖**：整合逐筆成交價格及其相關技術分析圖表。
    -   **量價K棒圖**：不同聚合週期的K線圖(1-min, 5-min, 10-min)。
    -   **日線K棒圖**：獨立腳本，繪製日夜盤之分的日線圖。
-   **動態儀表板 & 靜態報告**：
    -   **即時模式**：提供基於 Dash 的動態網頁儀表板（UPDATE_INTERVAL 自動刷新）。
    -   **歷史模式**：自動生成包含所有圖表與統計數據的單一 HTML 報告。

---

## 📸 儀表板預覽
![1](docs/1.png)
![2](docs/2.png)
![3](docs/3.png)
![4](docs/4.png)

---

## 🛠️ 技術棧

-   **核心語言**: Python 3.9+
-   **Web框架**: Dash (by Plotly)
-   **數據處理**: Pandas
-   **圖表繪製**: Plotly
-   **即時數據**: Confluent-Kafka for Python
-   **歷史數據**: Shioaji (永豐金證券 API)

---

## 🚀 安裝與設定

#### 1. 複製本專案
```bash
git clone https://github.com/gman-quant/tick-viz.git
cd tick-viz
```

#### 2. 建立並啟用虛擬環境
```bash
# macOS / Linux
python -m venv venv
source venv/bin/activate

# Windows (Git Bash)
python -m venv venv
source venv/Scripts/activate
```

#### 3. 安裝相依套件
```bash
pip install -r requirements.txt
```

#### 4. 進行環境設定
建立'.env' 設定可參考'.env.example' 如下:

```python
# tick-viz/.env.example

# Shioaji API credentials
SHIOAJI_API_KEY=your_shioaji_api_key_here
SHIOAJI_SECRET_KEY=your_shioaji_secret_key_here

# Kafka broker and topic
KAFKA_BROKER=your_kafka_address:9092
KAFKA_TOPIC=your_topic_name
```

修改 config/config.py 中的效能調校參數：
```python
# config/config.py
FETCH_INTERVAL  = 2    # [秒] consumer.poll() 的最長等待時間
UPDATE_INTERVAL = 2    # [秒] UI 更新週期
```

---

## 💡 使用方式

本專案支援兩種運行模式：

### 🟢 即時模式（`real_time_mode=1`）

用於接收來自 Kafka 串流來源的 **即時 tick 資料**。

- 啟動後端資料迴圈 (Consumer) 與前端 Dash 伺服器 (Web App)。
- 後端 (data_loop) 從 Kafka 獲取資料並執行技術分析計算（rolling vwap, kbars ...）。
- 前端 (dash_app) Dash 更新圖表，僅讀取已算好的資料，確保 UI 流暢。
- 左上角「⬜️ 點擊生成報告」按鈕：
  - 功能：將目前儀表板的圖表與統計資料生成 靜態 HTML 報告，存至 output/TXF-Charts-Live-Static.html。
- 啟動方式：
```bash
source venv/bin/activate
python main.py --real-time-mode 1
```
啟動後請開啟瀏覽器訪問 http://localhost:8080

### 🔵 歷史模式（`real_time_mode=0`）

用於回看特定區間的 **歷史 tick 資料**。

- 自訂起迄日期（--date-start, --date-end）。
- 可分別產出日盤與夜盤報告（--session）。
- 自動略例假日。
- 全部資料處理完畢後自動結束，並將 HTML 報告存於 output/。
- 啟動方式：
```bash
source venv/bin/activate
python main.py --real-time-mode 0 --date-start 2025-10-01 --date-end 2025-10-31 --session whole
# --session 可選 'day'（日盤）、'night'（夜盤）、或 'whole'（日+夜）
```

### 📅 日線圖更新

用於將 tick 資料聚合成日線 K 棒並繪製圖表。

- 更新每日/夜盤日線 CSV
- 確認歷史日線資料完整性
- 為報告或分析提供日線圖
> 輸出：
> - 日線CSV：`data/daily_txf.csv`
> - 日線圖表：`output/TXF-Daily-Chart.html`
- 啟動方式：
```bash
source venv/bin/activate && python -m src.processing.kbar.process_all_ticks_to_daily_csv
python plot_txf_kbar.py
```

📂 所有輸出報告會自動儲存至 `output/` 資料夾。

---

## 📁 專案結構

```text
TICK-VIZ/
├── src/                                           # 核心原始碼
│   ├── data_sourcing/                             # 數據獲取模組（Kafka、Shioaji、前收盤價）
│   │   ├── fetch_ticks.py                         # 從 Kafka/Shioaji 取得 tick 資料
│   │   └── market_data.py                         # 取得市場歷史資料及前收盤價
│   │
│   ├── processing/                                # 資料處理模組
│   │   ├── main_process.py                        # 核心資料流與報告生成
│   │   ├── kbars.py                               # ticks → 分K（時間型 K棒）
│   │   ├── metrics.py                             # 技術指標計算
│   │   ├── volume_bars.py                         # 成交量型 K棒（Volume-based K bars）
│   │   └── kbar/                                  # ticks → 日K
│   │       └── process_all_ticks_to_daily_csv.py  # 聚合生成日K CSV
│   │
│   ├── utils/                                     # 工具模組
│   │   ├── misc.py                                # 雜項工具函式
│   │   ├── resource_contexts.py                   # 資源管理上下文
│   │   ├── session_time.py                        # 交易日盤/夜盤判斷與時間
│   │   └── time_parser.py                         # 字串與 datetime 互轉
│   │
│   ├── visualization/                             # 圖表與報告產出模組
│   │   ├── stats_table.py                         # 統計表格生成（Dash & HTML）
│   │   ├── main_chart.py                          # 主分析圖表
│   │   ├── candlestick_chart.py                   # K棒圖表
│   │   └── report_generator.py                    # 靜態 HTML 報告生成
│   │
│   └── web/                                       # Web/Dash 相關功能
│       ├── dash_app.py                            # Dash App 建立與回呼
│       ├── shared_state.py                        # Web共享狀態（thread-safe）
│       └── assets/                                # CSS/JS/靜態資源
│           └── style.css                          # 自訂樣式
│
├── config/                                        # 專案設定與型別
│   ├── config.py                                  # 全域參數、API 金鑰等
│   ├── run_context.py                             # 執行上下文 RunContext
│   └── types.py                                   # 自定型別與列舉（SessionType、DataSource）
│
├── output/                                        # 預設報告輸出資料夾（HTML、圖表等）
├── data/                                          # 本地快取 Tick/Kbars 資料（Parquet）
├── docs/                                          # 文件、截圖、開發紀錄
├── main.py                                        # 專案主程式（即時與歷史模式）
├── plot_txf_kbar.py                               # TXF 日K圖繪製工具
├── requirements.txt                               # Python 套件依賴清單
├── .env.example                                   # 環境變數範例
├── LICENSE                                        # 專案授權
└── README.md                                      # 專案說明文件
```

---

## 📄 授權條款

本專案採用 [MIT License](LICENSE) 授權。