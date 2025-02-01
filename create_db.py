import sqlite3
import json

def create_tables(conn):
    cursor = conn.cursor()
    
    # 建立大樂透資料表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS big_lotto (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        draw_term TEXT NOT NULL,
        draw_date TEXT NOT NULL,
        num1 INTEGER NOT NULL,
        num2 INTEGER NOT NULL,
        num3 INTEGER NOT NULL,
        num4 INTEGER NOT NULL,
        num5 INTEGER NOT NULL,
        num6 INTEGER NOT NULL,
        special_num INTEGER NOT NULL,
        total_sales INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # 建立威力彩資料表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS super_lotto (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        draw_term TEXT NOT NULL,
        draw_date TEXT NOT NULL,
        num1 INTEGER NOT NULL,
        num2 INTEGER NOT NULL,
        num3 INTEGER NOT NULL,
        num4 INTEGER NOT NULL,
        num5 INTEGER NOT NULL,
        num6 INTEGER NOT NULL,
        special_num INTEGER NOT NULL,
        total_sales INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # 修改今彩539資料表结构
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_cash (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        draw_term TEXT NOT NULL,
        draw_date TEXT NOT NULL,
        num1 INTEGER NOT NULL,
        num2 INTEGER NOT NULL,
        num3 INTEGER NOT NULL,
        num4 INTEGER NOT NULL,
        num5 INTEGER NOT NULL,
        total_sales INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    conn.commit()

def import_data():
    conn = sqlite3.connect('lottery.db')
    create_tables(conn)
    
    # 匯入大樂透資料
    with open('data/BigLotto.json', 'r', encoding='utf-8') as f:
        big_lotto_data = json.load(f)
        cursor = conn.cursor()
        # 遍歷字典中的每個值
        for item in big_lotto_data.values():  # 修改這裡
            nums = item['draw_order_nums']
            cursor.execute('''
            INSERT INTO big_lotto (draw_term, draw_date, num1, num2, num3, num4, num5, num6, special_num, total_sales)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item['draw'],
                item['date'],
                nums[0], nums[1], nums[2], nums[3], nums[4], nums[5],
                item['bonus_num'],
                item.get('price', None)  # 修改這裡，因為欄位名稱是 'price' 而不是 'total_sales'
            ))
    
    # 匯入威力彩資料
    with open('data/SuperLotto.json', 'r', encoding='utf-8') as f:
        super_lotto_data = json.load(f)
        for item in super_lotto_data.values():  # 修改這裡
            nums = item['draw_order_nums']
            cursor.execute('''
            INSERT INTO super_lotto (draw_term, draw_date, num1, num2, num3, num4, num5, num6, special_num, total_sales)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item['draw'],
                item['date'],
                nums[0], nums[1], nums[2], nums[3], nums[4], nums[5],
                item['bonus_num'],
                item.get('price', None)  # 修改這裡
            ))
    
    # 修改今彩539資料导入逻辑
    with open('data/DailyCash.json', 'r', encoding='utf-8') as f:
        daily_cash_data = json.load(f)
        for item in daily_cash_data.values():
            nums = item['draw_order_nums'][:5]  # 只取前5个号码
            cursor.execute('''
            INSERT INTO daily_cash (draw_term, draw_date, num1, num2, num3, num4, num5, total_sales)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item['draw'],
                item['date'],
                nums[0], nums[1], nums[2], nums[3], nums[4],
                item.get('price', None)
            ))
    
    conn.commit()
    conn.close()

if __name__ == '__main__':
    import_data() 