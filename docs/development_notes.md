# docs/development_notes.md

[TOC]

## 1. 24/7 伺服器管理 (macOS Launchd)

本專案若使用 `launchd` 搭配 `KeepAlive=true` 策略來確保 24/7 伺服器（`main.py`) 永遠在背景執行。

當需要在 VSCode 中手動修改或測試伺服器時，必須依照以下流程來「暫停」和「恢復」`launchd` 的自動守護功能。

### 1.1. 開發前 (暫停 24/7 服務)

此指令會停止服務，並防止 `launchd` 自動重啟：

```bash
# (請確認 .plist 檔案名稱是否為 com.garrett.tickviz2)
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.garrett.tickviz2.plist
```

### 1.2. 開發後 (恢復 24/7 服務)
此指令會重新載入服務，並恢復 KeepAlive 自動重啟：
```bash
# (請確認 .plist 檔案名稱是否為 com.garrett.tickviz2)
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.garrett.tickviz2.plist
```

### 1.3. launchd 除錯指令
```bash
# 查看所有與 tickviz 相關的代理程式
launchctl list | grep tickviz

# 立即測試執行 (強制重啟)
launchctl kickstart -k gui/$(id -u)/com.garrett.tickviz2

# 查看代理程式的狀態、日誌與退出碼
launchctl print gui/$(id -u)/com.garrett.tickviz2
```

## 2. Git 開發筆記

### 2.1. 暫時忽略本地 Config 修改

若需在「本地」暫時修改 config/config.py (例如切換 Kafka 位址) 且「不想」將這些修改 commit 出去時，可使用以下指令：

```bash
# (1) 暫時忽略此檔案的變更 (讓 git status 看不見)
git update-index --assume-unchanged config/config.py

# (2) 恢復追蹤此檔案的變更 (當您真的要 commit 變更時)
git update-index --no-assume-unchanged config/config.py
```

## 3. 專案手動執行指令

### 3.1. 啟動 24/7 即時伺服器
(必須先執行 launchctl bootout ... 確保 8080 埠未被佔用)
```bash
cd /path/to/tick-viz
source venv/bin/activate
python main.py --real-time-mode 1
```

### 3.2. 執行歷史回測模式
```bash
cd /path/to/tick-viz
source venv/bin/activate
python main.py --real-time-mode 0 --date-start 2025-11-06 --date-end 2025-11-07 --session whole
```

### 3.3. 執行靜態圖表腳本
```bash
cd /path/to/tick-viz
source venv/bin/activate

# (1) 先將 Parquet 轉為 K 線 CSV
python -m scripts.generate_daily_csv

# (2) 再將 K 線 CSV 繪製成 HTML
python -m scripts.plot_txf_kbar
```

