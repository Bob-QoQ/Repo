# Lotto-Crawler
台灣樂透爬蟲 (目前支援威力彩, 大樂透)

## 使用方式
*   JSON 檔案

    已經爬取的資料放在 data 資料夾, 可以透過 https://lilysheep.com/Lotto-Crawler/data/BigLotto.json 方式取得。

    Data 格式目前為
    ```json

    {
        "開獎期號" : {
            "draw": "開獎期號",      // String
            "date": "開獎日期",      // String
            "year": 2023,           // Int 
            "month": 8,             //Int
            "day": 1,               // Int
            "price": "271,180,716", // String
            "draw_order_nums": [    // Array 抽獎順序之號碼
            17,
            33,
            40,
            12,
            28,
            45
            ],
            "size_order_nums": [    // Array 數字大小順序之號碼
            12,
            17,
            28,
            33,
            40,
            45
            ],
            "bonus_num": 39         // Int 特別號
        }
    }
    ```
    
*   Code
    
    自行爬取最新開獎資訊 (預設會從 data/ 裡的最後一期之後開始爬取)
    ```bash
    py BigLotto.py
    py SuperLotto.py
    ```

    其他使用方式

    1.  參考 example.py
    2.  繼承 LottoBase.py 額外爬取其他樂透內容
