# Tick-Viz: Real-Time Tick Analyzer

本專案旨在將一個用於分析台指期 (TXF) Tick 資料的 Jupyter Notebook 模組化，以便於維護、重用和部署。

它從 Kafka 消費即時 Tick 數據，結合 Shioaji API 獲取的前日收盤價，生成包含價格走勢、成交量分析、折溢價等多維度圖表的 HTML 報告。

## 功能

-   從 Kafka 即時消費 Tick 資料。
-   使用 Shioaji API 獲取歷史收盤價。
-   生成固定成交量 K棒 (Volume Bars)。
-   計算期貨與現貨的折溢價、淨主動成交量等衍生指標。
-   繪製多圖合一的綜合分析圖表。
-   將分析結果生成可自動刷新的 HTML 報告。

## 專案結構

```
tick-viz/
├── .env                  # API 金鑰
├── config.py             # 全域設定
├── main.py               # 主程式入口
├── requirements.txt      # Python 依賴
├── src/                  # 核心原始碼
└── README.md             # 專案說明
```

## 安裝與設定

1.  **克隆專案**
    ```bash
    git clone https://github.com/gman-quant/tick-viz.git
    cd tick-viz
    ```

2.  **安裝 Python 依賴**
    建議在虛擬環境中執行：
    ```bash
    python -m venv venv
    ```
    ```bash
    # linux/mac
    source venv/bin/activate  
    ```
    ```bash
    # windows 
    source venv\Scripts\activate
    ```
    ```bash
    pip install -r requirements.txt
    ```

3.  **設定環境變數**
    建立一個名為 `.env` 的檔案，並填入以下資訊：
    ```
    # tick-viz/.env

    # Shioaji API credentials
    SHIOAJI_API_KEY="YOUR_API_KEY"
    SHIOAJI_SECRET_KEY="YOUR_SECRET_KEY"

    # Kafka broker and topic
    KAFKA_BROKER="YOUR_KAFKA_ADDRESS:9092"
    KAFKA_TOPIC="YOUR_TOPIC_NAME"
    ```

4.  **修改設定**
    開啟 `config.py` 檔案，根據您的需求修改以下參數：
    -   分析的時間區間 (`START_DATETIME`, `END_DATETIME`)
    -   HTML 報告的輸出路徑 (`OUTPUT_DIR`)

## 如何執行

完成設定後，在終端機中執行 `main.py` 即可：

```bash
python main.py
```

程式將會開始初始化，連接 Kafka 和 Shioaji，獲取資料並在 `config.py` 中指定的 `OUTPUT_DIR` 路徑下生成 HTML 分析報告。