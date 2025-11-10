# tick-viz 本地開發指南

- [tick-viz 本地開發指南](#tick-viz-本地開發指南)
  - [1. 24/7 伺服器管理 (macOS Launchd)](#1-247-伺服器管理-macos-launchd)
    - [1.1. 開發前 (暫停 24/7 服務)](#11-開發前-暫停-247-服務)
    - [1.2. 開發後 (恢復 24/7 服務)](#12-開發後-恢復-247-服務)
    - [1.3. 除錯與日誌](#13-除錯與日誌)
      - [其他 launchctl 常用指令](#其他-launchctl-常用指令)
      - [編輯設定檔 (.plist)](#編輯設定檔-plist)
      - [編輯腳本與查看日誌](#編輯腳本與查看日誌)
  - [2. Git 開發筆記](#2-git-開發筆記)
    - [2.1. 暫時忽略本地 Config 修改](#21-暫時忽略本地-config-修改)
  - [3. 專案手動執行指令](#3-專案手動執行指令)
    - [3.1. 啟動 24/7 即時伺服器](#31-啟動-247-即時伺服器)
    - [3.2. 執行歷史回測模式](#32-執行歷史回測模式)
    - [3.3. 執行靜態圖表腳本](#33-執行靜態圖表腳本)


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

### 1.3. 除錯與日誌

#### 其他 launchctl 常用指令
```bash
# 查看所有與 tickviz 相關的代理程式
launchctl list | grep tickviz

# 立即測試執行 (強制重啟)
launchctl kickstart -k gui/$(id -u)/com.garrett.tickviz2

# 查看代理程式的狀態、日誌與退出碼
launchctl print gui/$(id -u)/com.garrett.tickviz2
```

#### 編輯設定檔 (.plist)
```bash
nano ~/Library/LaunchAgents/com.garrett.tickviz.plist
nano ~/Library/LaunchAgents/com.garrett.tickviz2.plist
```

#### 編輯腳本與查看日誌
```bash
# 編輯啟動腳本
nano ~/Library/Scripts/update_daily_chart.sh
nano ~/Library/Scripts/monitor_realtime_txf.sh

# 查看標準輸出 (stdout) 日誌
cat /tmp/tickviz.log
cat /tmp/tickviz2.out

# 查看錯誤 (stderr) 日誌
cat /tmp/tickviz.err
cat /tmp/tickviz2.err

# 實時監控輸出與錯誤日誌
tail -f /tmp/tickviz2.out /tmp/tickviz2.err
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
cd Projects/tick-viz
source venv/bin/activate
python main.py --real-time-mode 1
```

### 3.2. 執行歷史回測模式
```bash
cd Projects/tick-viz
source venv/bin/activate
python main.py --real-time-mode 0 --date-start 2025-11-06 --date-end 2025-11-07 --session whole
```

### 3.3. 執行靜態圖表腳本
```bash
cd Projects/tick-viz
source venv/bin/activate

# (1) 先將 Parquet 轉為 K 線 CSV
python -m scripts.generate_daily_csv

# (2) 再將 K 線 CSV 繪製成 HTML
python -m scripts.plot_txf_kbar
```

