# 📈 台指期貨盤中動態分析儀表板 (TXF Intraday Dynamic Analysis Dashboard)

![Python](https://img.shields.io/badge/python-3.9%2B-blue) 
![Apache Kafka](https://img.shields.io/badge/Kafka-required-orange) 
![Shioaji](https://img.shields.io/badge/Shioaji-required-orange) 
![License: MIT](https://img.shields.io/badge/License-MIT-green)


本專案旨在提供一個視覺化的儀表板，用於即時（或歷史回測）分析台灣指數期貨（TXF）的 tick 級別數據流，並從中洞察盤中多空狀態。

---

## ✨ 主要功能

-   **雙模式數據源**：支援從 `Kafka` 即時消費 tick 數據，或透過 `Shioaji API` 抓取歷史 tick 數據進行分析。
-   **多維度指標計算**：即時計算 **VWAP** (成交量加權平均價)、**盤中高低價**、**累計買賣盤成交量**、**淨主動成交量**以及**期現貨價差**等關鍵指標。
-   **進階圖表視覺化**：
    -   **主分析圖**：整合價格、VWAP、價差、淨成交量於一體的四合一互動式圖表。
    -   **量價K棒圖**：以固定成交口數生成「等量K棒 (Volume Bars)」，並在K棒上標示買賣盤的成交量差 (Volume Delta)。
-   **自動化報告生成**：將所有圖表與統計數據整合為單一的 `HTML` 報告，並支援在即時模式下**自動定時刷新**頁面。
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
- 啟動本地 aiohttp 伺服器（localhost:8080），提供可動態更新的 HTML 報告。
- 程式常駐執行，並定期刷新報告與畫面。

### 🟡 歷史模式（`real_time_mode=False`）

適用於回看特定區間的 **歷史 tick 資料**。

- 自訂起迄日期。
- 可分別產出日盤與夜盤報告。
- 自動略過週末（六、日）。
- 全部資料處理完畢後自動結束。

#### 如何設定日期範圍？

請修改 `main()` 中的以下區段：
```python
start_date = date(2025, 7, 7)
end_date = date(2025, 7, 14)
```

#### 如何設定日盤或夜盤？

請修改 `main()` 中的 for day_session in range(...)：
```python
for day_session in range(1, 1 + 1):  # 只輸出 day session(1)
or
for day_session in range(0, 0 + 1):  # 只輸出 night session(0)
or
for day_session in range(0, 1 + 1):  # 同時輸出
```

#### 啟動方式
```bash
python main.py
```

📂 所有輸出報告會自動儲存至 `output/` 資料夾（可在 `config.py` 中修改）。

---

## 📁 專案結構

```text
TICK-VIZ/
├── src/                         # 核心原始碼
│   ├── data_sourcing/           # 數據獲取模組（Kafka、Shioaji）
│   ├── processing/              # 數據處理模組（K棒生成、指標計算等）
│   ├── utils/                   # 工具模組（時間、資源管理等）
│   ├── visualization/           # 圖表與報告產出模組（Plotly 等）
│   └── web/                     # Web 模組（WebSocket 等伺服器功能）
│
├── config/                      # 專案設定與型別定義
│   ├── config.py                # 全域參數與 API 金鑰設定
│   ├── run_context.py           # 執行上下文（RunContext，含交易邏輯）
│   └── types.py                 # 自定型別與列舉（SessionType、DataSource）
│
├── output/                      # 預設報告輸出資料夾（HTML、圖表等）
├── data/                        # 本地快取的 Tick/Kbars 資料（Parquet 格式）
├── docs/                        # 文件與截圖（開發紀錄、說明圖示等）
│
├── main.py                      # 專案主程式（支援即時與歷史模式）
├── main_process.py              # 核心資料處理邏輯（資料流轉、報告生成）
├── requirements.txt             # Python 套件依賴清單
├── .env.example                 # .env 環境變數範例檔
└── README.md                    # 專案說明文件
```

---

## 📄 授權條款

本專案採用 [MIT License](LICENSE) 授權。