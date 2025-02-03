import sqlite3
import json
import os

def create_tables(cursor):
    # 建立大樂透表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS big_lotto (
        draw_term TEXT PRIMARY KEY,
        draw_date TEXT,
        draw_numbers TEXT,
        special_number TEXT,
        total_prize INTEGER
    )
    ''')

    # 建立威力彩表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS super_lotto (
        draw_term TEXT PRIMARY KEY,
        draw_date TEXT,
        draw_numbers TEXT,
        special_number TEXT,
        total_prize INTEGER
    )
    ''')

    # 建立今彩539表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_cash (
        draw_term TEXT PRIMARY KEY,
        draw_date TEXT,
        draw_numbers TEXT,
        special_number TEXT,
        total_prize INTEGER
    )
    ''')

def import_json_to_db(cursor, json_file, table_name):
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    for draw_term, draw_info in data.items():
        draw_numbers = ','.join(str(x) for x in draw_info['draw_order_nums'])
        special_number = str(draw_info.get('bonus_num', ''))
        
        # 格式化日期
        date_parts = draw_info['date'].split('/')
        formatted_date = f"{int(date_parts[0]) + 1911}/{date_parts[1]}/{date_parts[2]}"
        
        cursor.execute(f'''
        INSERT OR REPLACE INTO {table_name}
        (draw_term, draw_date, draw_numbers, special_number, total_prize)
        VALUES (?, ?, ?, ?, ?)
        ''', (draw_term, formatted_date, draw_numbers, special_number, draw_info.get('price', 0)))

def main():
    # 確保data目錄存在
    if not os.path.exists('data'):
        print("Error: 'data' directory not found")
        return

    # 連接到資料庫
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()

    # 建立資料表
    create_tables(cursor)

    # 匯入資料
    json_files = {
        'big_lotto': 'data/BigLotto.json',
        'super_lotto': 'data/SuperLotto.json',
        'daily_cash': 'data/DailyCash.json'
    }

    for table_name, json_file in json_files.items():
        if os.path.exists(json_file):
            print(f"Importing {json_file} to {table_name}...")
            import_json_to_db(cursor, json_file, table_name)
        else:
            print(f"Warning: {json_file} not found")

    # 提交更改並關閉連接
    conn.commit()
    conn.close()
    print("Database creation and data import completed successfully!")

if __name__ == "__main__":
    main() 