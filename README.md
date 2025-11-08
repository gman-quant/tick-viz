# 📈 台指期即時分析儀表板 (TXF Real-time Dashboard)

![Python](https://img.shields.io/badge/python-3.9%2B-blue) 
![Apache Kafka](https://img.shields.io/badge/Kafka-required-orange) 
![Shioaji](https://img.shields.io/badge/Shioaji-required-orange) 
![License: MIT](https://img.shields.io/badge/License-MIT-green)

本專案為一套台指期即時盤中多空分析工具。

核心架構分離「資料處理」與「UI 渲染」，確保在即時 Ticks 湧入下，儀表板介面依然保持高效流暢。

- 主要功能：即時計算關鍵多空指標（如 VWAP, Rolling VWAP, 1 分 K），並視覺化盤中多空狀態。

- 支援模式：

  - 即時：串接即時 Kafka 串流，提供 24/7 盤中監控儀表板。

  - 歷史：支援 Shioaji API 或 Kafka 歷史資料，用於回測並生成靜態 HTML 報告。

---

## 📸 儀表板預覽
![1](docs/1.png)
![2](docs/2.png)
![3](docs/3.png)
![4](docs/4.png)

---

## ✨ 主要功能

-   雙模式資料源：支援從 `Kafka` 即時消費 tick 資料，或透過 `Shioaji API` 抓取歷史 tick 資料進行分析。
-   高效能即時架構：採用多執行緒模型，將資料處理 (data_loop) 與 UI 渲染 (dash_app) 分離，確保即時儀表板流暢高效。
-   多維度指標計算：即時計算技術分析指標 VWAP、High&Low、淨成交強度指標等，可依需求自行設計。
-   進階圖表視覺化：
    -   主分析圖：整合逐筆成交價格及其相關技術分析圖表。
    -   量價K棒圖：多週期的K線圖(1, 3, 5, 10 分)。
    -   日線K棒圖：獨立模組，支援日夜盤分段視覺化。
-   動態儀表板 & 靜態報告：
    -   即時模式：提供基於 Dash 的動態網頁儀表板，支援自動刷新（UPDATE_INTERVAL）。
    -   歷史模式：自動生成整合圖表與統計摘要的單一 HTML 報告。

---

## 🏗️ 核心架構 (即時模式)

本專案的後端 T_Data 服務採用「狀態機」與「任務執行者」分離的設計，確保 24/7 穩定運行與盤別切換的強固性。

1. data_loop_manager (狀態機 & 服務管理器)：

    - src/service.py 中的 24/7 迴圈，負責偵測當前時段（日盤、夜盤、休市）。

    - 職責：判斷是否應啟動新盤別，建立 RunContext，並呼叫 run_single_session_task。

2. run_single_session_task (盤別生命週期管理器)：

    - src/service.py 中的函式，負責「單一盤別」（例如今日日盤）的完整生命週期。

    - 職責：清除舊狀態、使用 offsets_for_times 精確初始化 Kafka offset，然後呼叫並等待 process_market_session 執行完畢。

3. process_market_session (增量資料處理迴圈)：

    - src/processing/main_process.py 中的核心迴圈，負責「盤中」的持續運算。

    - 職責：不斷從 Kafka 獲取新資料、進行增量計算（如 RVWAP）、並將結果更新至 shared_state 供前端使用。

    - 此迴圈結束（例如偵測到收盤）後，控制權交還給 run_single_session_task，後者隨之結束，data_loop_manager 再次進入偵測狀態。

---

## 🛠️ 使用技術

-   **核心語言**: Python 3.9+
-   **Web框架**: Dash (by Plotly)
-   **資料處理**: Pandas
-   **圖表繪製**: Plotly
-   **即時資料**: Confluent-Kafka for Python
-   **歷史資料**: Shioaji (永豐金證券 API)

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
建立.env 檔案（可複製 .env.example），並填入以下資訊：

```python
# tick-viz/.env

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

- 用於接收來自 Kafka 串流來源的 即時 tick 資料，並啟動 24/7 儀表板。

- 啟動：同時啟動「後端資料服務 (T_Data)」與「前端 Dash 伺服器 (WebApp)」。

- 後端：請參考上方的 [核心架構說明](#%EF%B8%8F-核心架構-即時模式)。

- 前端 (WebApp)：Dash 儀表板會定期（UPDATE_INTERVAL）讀取後端已算好的資料來更新圖表，確保 UI 流暢。

- 靜態報告：

  - 左上角「⬜️ 點擊生成報告」按鈕。

  - 功能：將目前儀表板的圖表與統計資料，生成一份靜態 HTML 報告，存至 output/TXF-Charts-Live-Static.html。

- 啟動方式：
```bash
source venv/bin/activate
python main.py --real-time-mode 1
```
啟動後請開啟瀏覽器訪問 http://localhost:8080

### 🔵 歷史模式（`real_time_mode=0`）

- 用於回測特定日期區間的歷史 tick 資料（可來自 Kafka 或 Shioaji）。

- 功能：

  - 可自訂起迄日期（--date-start, --date-end）。

  - 可分別產出日盤與夜盤報告（--session）。

  - 自動略過台股例假日。

  - 此模式**不啟動**即時儀表板。

  - 程式會在全部資料處理完畢後自動結束，並將 HTML 靜態報告存於 output/。

- 啟動方式：
```bash
source venv/bin/activate
python main.py --real-time-mode 0 --date-start 2025-10-01 --date-end 2025-10-31 --session whole
# --session 可選 'day'（日盤）、'night'（夜盤）、或 'whole'（日+夜）
```

### 📅 日線圖更新

- 用於將 tick 資料聚合成日線 K 棒並繪製圖表，此為獨立腳本。

- 功能：

  - 更新 data/daily_txf.csv（包含日盤與夜盤的日線資料）。

  - 繪製日線圖表 output/TXF-Daily-Chart.html。

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
│   │   ├── loop_manager.py       # │  ├─ 【外層核心】24/7 服務管理器
│   │   └── session_processor.py  # │  └─ 【內層核心】「單一盤別」資料處理迴圈
│   │                               │
│   ├── data_sourcing/            # ├─ 📂 資料獲取 (從 Kafka/Shioaji 取得資料)
│   │   ├── fetch_ticks.py        # │  ├─ 獲取 Tick
│   │   └── market_data.py        # │  └─ 獲取市場歷史資料 (例如：前收盤價)
│   │                               │
│   ├── processing/               # ├─ 📂 【資料處理】(純粹的資料轉換與計算)
│   │   ├── bars/                 # │  ├─ 📂 K 棒合成
│   │   │   ├── time_bars.py      # │  │  ├─ 時間型 K 棒
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