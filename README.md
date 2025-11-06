# 📈 台指期即時分析儀表板 (TXF Real-time Dashboard)

![Python](https://img.shields.io/badge/python-3.9%2B-blue) 
![Apache Kafka](https://img.shields.io/badge/Kafka-required-orange) 
![Shioaji](https://img.shields.io/badge/Shioaji-required-orange) 
![License: MIT](https://img.shields.io/badge/License-MIT-green)

本專案旨在開發台指期即時盤中多空分析工具。 核心架構將「資料處理」與「UI渲染」徹底分離，確保海量 Tick 資料更新下，介面依然流暢。
- 功能：視覺化盤中多空狀態。
- 模式：支援即時 Kafka 串流與 Shioaji 歷史回測。

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
建立'.env'，設定可參考'.env.example' 如下:

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
source venv/bin/activate
python -m scripts.generate_daily_csv
python -m scripts.plot_txf_kbar
```

📂 所有輸出報告會自動儲存至 `output/` 資料夾。

---

## 📁 專案結構

```text
TICK-VIZ/
├── config/                       # 📂 專案設定與型別
│   ├── config.py                 # ├─ 全域常數 (API 金鑰, Kafka 主題, 交易時間定義等)
│   ├── run_context.py            # ├─ 執行上下文 (RunContext 資料類別)
│   └── types.py                  # └─ 自定型別與列舉 (SessionType, DataSource)
│
├── data/                         # 📂 本地快取資料 (存放 Parquet/CSV 供回測用)
├── docs/                         # 📂 文件、截圖、開發紀錄
├── output/                       # 📂 預設報告輸出資料夾 (存放 HTML 報告)
│
├── scripts/                      # 📂 【工具腳本】(獨立、批次執行的工具)
│   ├── generate_daily_csv.py     # ├─ 將 Tick 聚合成日 K 並存為 CSV
│   └── plot_txf_kbar.py          # └─ 日 K 繪圖工具
│
├── src/                          # 📂 核心原始碼 (所有應用程式邏輯)
│   ├── core/                     # ├─ 📂 【應用核心】(負責協調、狀態和流程控制)
│   │   ├── loop_manager.py       # │  ├─ 【外層核心】24/7 服務管理器 (原 service.py)
│   │   └── session_processor.py  # │  └─ 【內層核心】「單一盤別」資料處理迴圈 (原 processing/main_process.py)
│   │                               │
│   ├── data_sourcing/            # ├─ 📂 數據獲取 (從 Kafka/Shioaji 取得資料)
│   │   ├── fetch_ticks.py        # │  ├─ 獲取 Tick
│   │   └── market_data.py        # │  └─ 獲取市場歷史資料 (例如：前收盤價)
│   │                               │
│   ├── processing/               # ├─ 📂 【資料處理】(純粹的資料轉換與計算)
│   │   ├── bars/                 # │  ├─ 📂 K 棒合成
│   │   │   ├── time_bars.py      # │  │  ├─ 時間型 K 棒 (原 kbars.py)
│   │   │   └── volume_bars.py    # │  │  └─ 成交量型 K 棒
│   │   └── metrics.py            # │  └─ 計算技術指標 (如 RVWAP) 並準備繪圖用 DF
│   │                               │
│   ├── utils/                    # ├─ 📂 共用工具模組 (時間、資源管理等)
│   │   ├── misc.py               # │  ├─ 雜項工具
│   │   ├── resource_contexts.py  # │  ├─ 資源管理器 (Shioaji/Kafka context)
│   │   ├── session_time.py       # │  ├─ 交易時間計算
│   │   └── time_parser.py        # │  └─ CLI 日期解析
│   │                               │
│   ├── visualization/            # ├─ 📂 圖表與報告產出
│   │   ├── stats_table.py        # │  ├─ 統計表格生成
│   │   ├── main_chart.py         # │  ├─ 主分析圖
│   │   ├── candlestick_chart.py  # │  ├─ K 棒圖
│   │   └── report_generator.py   # │  └─ 靜態 HTML 報告生成
│   │                               │
│   └── web/                      # └─ 📂 Web/Dash 相關功能
│       ├── dash_app.py           #    ├─ Dash App 的 Layout 與 Callbacks
│       ├── shared_state.py       #    ├─ 【關鍵】跨執行緒共享狀態 (thread-safe)
│       └── assets/               #    └─ 📂 Dash 靜態資源 (CSS/JS)
│
├── tests/                        # 📂 【自動化測試】(確保邏輯正確性)
│
├── main.py                       # 📜 【專案主入口】解析 CLI 參數、啟動 Dash 與核心服務
├── requirements.txt              # 📋 Python 套件依賴清單
├── .env.example                  # 🔑 環境變數範例 (API Key/Secret)
├── LICENSE                       # 📄 專案授權
└── README.md                     # 📖 專案說明文件
```

---

## 📄 授權條款

本專案採用 [MIT License](LICENSE) 授權。