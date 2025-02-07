# Taiwan Lottery Analysis System

台灣彩券分析預測系統,整合多種分析模型與預測算法。

## 功能特色

### 資料分析
- 歷史開獎數據分析
- 號碼出現頻率分析 
- 號碼分布規律分析
- 特徵數字關聯分析

### 預測模型
- 機器學習預測模型
- 統計概率預測
- 週期性分析預測
- 號碼關聯預測
- 綜合評分預測

### 推薦系統
- 智能選號推薦
- 客製化選號推薦
- 多維度交叉分析
- 號碼組合評分
- 預測準確度追蹤

## 使用技術
- Python
- Flask
- SQLite
- Machine Learning
- Statistical Analysis

## 安裝說明
1. 安裝相依套件:
    ```bash
    # 更新 pip
    python -m pip install --upgrade pip
    
    # 安裝相依套件
    pip install -r requirements.txt
    ```

2. 初始化資料庫:
    ```bash
    python create_db.py
    ```

3. 更新開獎資料:
    ```bash
    python Lotto_Crawler.py
    ```

4. 啟動系統:
    ```bash
    python app.py
    ```

## 系統需求
- Python 3.12+
- SQLite 3

## 相依套件
- Flask
- NumPy
- Pandas
- Scikit-learn
- Matplotlib
- Seaborn
- Requests
- BeautifulSoup4