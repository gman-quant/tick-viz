# 📈 台指期貨盤中動態分析儀表板 (TXF Intraday Dynamic Analysis Dashboard)

![Python](https://img.shields.io/badge/python-3.9%2B-blue) 
![Apache Kafka](https://img.shields.io/badge/Kafka-required-orange) 
![Shioaji](https://img.shields.io/badge/Shioaji-required-orange) 
![License: MIT](https://img.shields.io/badge/License-MIT-green)


本專案旨在提供一個視覺化的儀表板，即時分析或歷史回測台灣指數期貨（TXF）的 tick 級別數據流，並從中洞察盤中多空狀態。

---

## ✨ 主要功能

-   **雙模式數據源**：支援從 `Kafka` 即時消費 tick 數據，或透過 `Shioaji API` 抓取歷史 tick 數據進行分析。
-   **多維度指標計算**：即時計算 **VWAP**(成交量加權平均價)、**盤中高低價**、**淨成交強度指標**、**淨主動成交量**與**累計成交量**等技術指標。
-   **進階圖表視覺化**：
    -   **主分析圖**：整合逐筆成交價格及其相關技術分析圖表。
    -   **量價K棒圖**：不同聚合週期的K線圖(1-min, 5-min, 10-min)。
    -   **日線K棒圖**：日夜盤之分的日線圖。
-   **自動化報告生成**：將所有圖表與統計數據整合為單一的 `HTML` 報告，並支援在即時模式下**自動定時更新**頁面。
-   **高效率終端監控**：在終端機中提供一個乾淨、會原地更新的狀態面板，方便監控程式運行狀態。

---

## 📸 儀表板預覽
![1](docs/1.png)
![2](docs/2.png)
![3](docs/3.png)
![4](docs/4.png)

---

## 🛠️ 技術棧

-   **核心語言**: Python 3.9+
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
KAFKA_BROKER=your_kafka_addreee:9092
KAFKA_TOPIC=your_topic_name
```

修改'config.py' 設定參考如下:
```python
# config/config.py

# === Timezone setting ===
TAIWAN_TZ = ZoneInfo("Asia/Taipei")

# === Report and chart settings ===
CLEAR_SCREEN_EACH_CYCLE = True
UPDATE_INTERVAL = 12  # seconds
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"
OUTPUT_DIR.mkdir(exist_ok=True)  # 確保目錄存在
```

---

## 💡 使用方式

本專案支援兩種運行模式：

### 🟢 即時模式（`real_time_mode=True`）

用於接收來自 Kafka 串流來源的 **即時 tick 資料**。

- 自動判斷當前時間屬於日盤或夜盤，並建立執行上下文。
- 啟動本地 **Dash 伺服器**（localhost:8080），提供可動態更新的儀表板。
- 程式常駐執行，並定期刷新報告與畫面。
- **左上角「⬜️ 點擊生成報告」按鈕**：  
  - 功能：將目前儀表板的圖表與統計資料生成 **靜態 HTML 報告**，存至 `output/TXF-Charts-Live-Static.html`。  
  - 注意：僅在即時模式下可使用，按鈕是浮動的，不佔用主要畫面區域。
- 啟動方式：
```bash
# 🟢 即時更新模式
source venv/bin/activate
python main.py --real-time-mode 1
```

### 🔵 歷史模式（`real_time_mode=False`）

適用於回看特定區間的 **歷史 tick 資料**。

- 自訂起迄日期。
- 可分別產出日盤與夜盤報告。
- 自動略過週末（六、日）。
- 全部資料處理完畢後自動結束。
- 啟動方式：
```bash
# 🔵 歷史回顧模式
source venv/bin/activate
python main.py --real-time-mode 0 --date-start 2025-10-01 --date-end 2025-10-31 --session whole
# --session 可選 'day'（日盤）、'night'（夜盤）、或 'whole'（日+夜）
```

### 📅 日線圖更新

此流程將 tick 資料聚合成日線 K 棒並繪製圖表，適合：

- 更新每日/夜盤日線 CSV
- 確認歷史日線資料完整性
- 為報告或分析提供日線圖
> 輸出：
> - 日線CSV：`data/`
> - 日線圖表：`output/`

- 啟動方式：
```bash
# 📅 日線圖更新
source venv/bin/activate && python -m src.processing.kbar.process_all_ticks_to_daily_csv
python plot_txf_kbar.py
```


📂 所有輸出報告會自動儲存至 `output/` 資料夾（可在 `config.py` 中修改）。

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
│   │   ├── metrics.py                             # 技術指標計算（MA、RSI 等）
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