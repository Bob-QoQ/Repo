import random
from datetime import datetime
import sqlite3
import numpy as np

def get_lottery_config(lottery_type):
    """獲取彩券配置"""
    if lottery_type == 'big-lotto':
        return {
            'numbers': 6,
            'max_number': 49,
            'special_number': True
        }
    elif lottery_type == 'super-lotto':
        return {
            'numbers': 6,
            'max_number': 38,
            'special_number': True
        }
    else:  # daily-cash
        return {
            'numbers': 5,
            'max_number': 39,
            'special_number': False
        }

def get_quick_picks(lottery_type, count=5):
    """快速選號推薦"""
    config = get_lottery_config(lottery_type)
    results = []
    
    for _ in range(count):
        # 生成主要號碼
        numbers = sorted(random.sample(range(1, config['max_number'] + 1), config['numbers']))
        
        # 如果需要特別號
        if config['special_number']:
            special = random.randint(1, config['max_number'])
            while special in numbers:
                special = random.randint(1, config['max_number'])
        else:
            special = None
            
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': '隨機生成的號碼組合',
            'confidence': random.randint(60, 90)  # 隨機給出信心指數
        })
    
    return results

def get_hot_combinations(lottery_type, periods=50, count=5):
    """熱門號碼組合推薦"""
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    config = get_lottery_config(lottery_type)
    
    # 獲取最近期數的熱門號碼
    table_name = {
        'big-lotto': 'big_lotto',
        'super-lotto': 'super_lotto',
        'daily-cash': 'daily_cash'
    }[lottery_type]
    
    # 統計號碼出現頻率
    number_counts = {}
    for i in range(1, config['max_number'] + 1):
        cursor.execute(f'''
            SELECT COUNT(*) FROM (
                SELECT * FROM {table_name} ORDER BY draw_term DESC LIMIT ?
            ) WHERE num1=? OR num2=? OR num3=? OR num4=? OR num5=?
            {' OR num6=?' if config['numbers'] == 6 else ''}
        ''', [periods] + [i] * config['numbers'])
        number_counts[i] = cursor.fetchone()[0]
    
    # 根據出現頻率排序
    sorted_numbers = sorted(number_counts.items(), key=lambda x: x[1], reverse=True)
    hot_numbers = [num for num, _ in sorted_numbers[:config['numbers'] * 2]]
    
    results = []
    for _ in range(count):
        numbers = sorted(random.sample(hot_numbers, config['numbers']))
        
        if config['special_number']:
            special = random.choice(hot_numbers)
            while special in numbers:
                special = random.choice(hot_numbers)
        else:
            special = None
            
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': f'從最近{periods}期中選出的熱門號碼組合',
            'confidence': random.randint(70, 95)
        })
    
    conn.close()
    return results

def get_cold_combinations(lottery_type, periods=50, count=5):
    """冷門號碼組合推薦"""
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    config = get_lottery_config(lottery_type)
    
    # 獲取最近期數的冷門號碼
    table_name = {
        'big-lotto': 'big_lotto',
        'super-lotto': 'super_lotto',
        'daily-cash': 'daily_cash'
    }[lottery_type]
    
    # 統計號碼出現頻率
    number_counts = {}
    for i in range(1, config['max_number'] + 1):
        cursor.execute(f'''
            SELECT COUNT(*) FROM (
                SELECT * FROM {table_name} ORDER BY draw_term DESC LIMIT ?
            ) WHERE num1=? OR num2=? OR num3=? OR num4=? OR num5=?
            {' OR num6=?' if config['numbers'] == 6 else ''}
        ''', [periods] + [i] * config['numbers'])
        number_counts[i] = cursor.fetchone()[0]
    
    # 根據出現頻率排序（從低到高）
    sorted_numbers = sorted(number_counts.items(), key=lambda x: x[1])
    cold_numbers = [num for num, _ in sorted_numbers[:config['numbers'] * 2]]
    
    results = []
    for _ in range(count):
        numbers = sorted(random.sample(cold_numbers, config['numbers']))
        
        if config['special_number']:
            special = random.choice(cold_numbers)
            while special in numbers:
                special = random.choice(cold_numbers)
        else:
            special = None
            
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': f'從最近{periods}期中選出的冷門號碼組合',
            'confidence': random.randint(50, 75)  # 冷門號碼的信心指數較低
        })
    
    conn.close()
    return results

def get_balanced_combinations(lottery_type, periods=50, count=5):
    """平衡號碼組合推薦"""
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    config = get_lottery_config(lottery_type)
    
    table_name = {
        'big-lotto': 'big_lotto',
        'super-lotto': 'super_lotto',
        'daily-cash': 'daily_cash'
    }[lottery_type]
    
    # 分析歷史數據中的奇偶比例和大小比例
    cursor.execute(f'''
        SELECT num1, num2, num3, num4, num5{', num6' if config['numbers'] == 6 else ''}
        FROM {table_name}
        ORDER BY draw_term DESC
        LIMIT ?
    ''', [periods])
    historical_draws = cursor.fetchall()
    
    # 計算平均奇偶比例和大小比例
    odd_count = 0
    big_count = 0
    total_numbers = 0
    
    for draw in historical_draws:
        for num in draw:
            if num % 2 == 1:  # 奇數
                odd_count += 1
            if num > config['max_number'] // 2:  # 大數
                big_count += 1
            total_numbers += 1
    
    # 計算理想的奇偶和大小比例
    target_odd_ratio = odd_count / total_numbers
    target_big_ratio = big_count / total_numbers
    
    results = []
    for _ in range(count):
        valid_combination = False
        attempt = 0
        
        while not valid_combination and attempt < 100:
            # 生成候選號碼
            numbers = sorted(random.sample(range(1, config['max_number'] + 1), config['numbers']))
            
            # 計算當前組合的比例
            current_odd = sum(1 for n in numbers if n % 2 == 1) / config['numbers']
            current_big = sum(1 for n in numbers if n > config['max_number'] // 2) / config['numbers']
            
            # 檢查是否符合平衡標準
            if abs(current_odd - target_odd_ratio) <= 0.2 and abs(current_big - target_big_ratio) <= 0.2:
                valid_combination = True
            
            attempt += 1
        
        # 生成特別號
        if config['special_number']:
            special = random.randint(1, config['max_number'])
            while special in numbers:
                special = random.randint(1, config['max_number'])
        else:
            special = None
        
        # 修改這部分,生成中文說明文字
        odd_count = sum(1 for n in numbers if n % 2 == 1)
        even_count = sum(1 for n in numbers if n % 2 == 0)
        big_count = sum(1 for n in numbers if n > config['max_number'] // 2)
        small_count = sum(1 for n in numbers if n <= config['max_number'] // 2)
        
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': f'平衡組合',
            'confidence': random.randint(65, 85),
            'details': {
                '奇偶比例': f'{odd_count}:{even_count}',
                '大小比例': f'{big_count}:{small_count}',
                '說明': f'此組合包含{odd_count}個奇數、{even_count}個偶數，'
                      f'{big_count}個大數(>{config["max_number"]//2})、'
                      f'{small_count}個小數(≤{config["max_number"]//2})'
            }
        })
    
    conn.close()
    return results

def get_lucky_numbers(lottery_type, birth_date='', lucky_numbers=None, count=5):
    """根據生日和幸運數字生成推薦組合"""
    config = get_lottery_config(lottery_type)
    results = []
    
    # 處理生日數字
    birth_numbers = set()
    if birth_date:
        try:
            date_obj = datetime.strptime(birth_date, '%Y-%m-%d')
            birth_numbers.update([
                date_obj.day,
                date_obj.month,
                date_obj.year % 100,
                (date_obj.year // 100) % 100
            ])
            # 只保留在範圍內的數字
            birth_numbers = {n for n in birth_numbers if 1 <= n <= config['max_number']}
        except ValueError:
            pass
    
    # 處理幸運數字
    if lucky_numbers:
        try:
            lucky_nums = {int(n) for n in lucky_numbers if n.strip()}
            lucky_nums = {n for n in lucky_nums if 1 <= n <= config['max_number']}
            birth_numbers.update(lucky_nums)
        except ValueError:
            pass
    
    # 如果沒有足夠的幸運數字，添加一些隨機數字
    available_numbers = list(birth_numbers)
    if len(available_numbers) < config['numbers']:
        additional_numbers = [n for n in range(1, config['max_number'] + 1) 
                            if n not in birth_numbers]
        available_numbers.extend(additional_numbers)
    
    for _ in range(count):
        if len(birth_numbers) >= config['numbers']:
            # 優先使用生日和幸運數字
            numbers = sorted(random.sample(list(birth_numbers), config['numbers']))
            reason = '使用生日和幸運數字組合'
            confidence = 80
        else:
            # 混合使用生日數字和隨機數字
            numbers = []
            # 先加入一些生日/幸運數字
            if birth_numbers:
                numbers.extend(random.sample(list(birth_numbers), 
                                          min(len(birth_numbers), config['numbers'] - 1)))
            
            # 補充隨機數字
            remaining = config['numbers'] - len(numbers)
            if remaining > 0:
                available = [n for n in range(1, config['max_number'] + 1) 
                           if n not in numbers]
                numbers.extend(random.sample(available, remaining))
            
            numbers.sort()
            reason = '混合生日/幸運數字和隨機數字'
            confidence = 70
        
        # 生成特別號
        if config['special_number']:
            special = random.randint(1, config['max_number'])
            while special in numbers:
                special = random.randint(1, config['max_number'])
        else:
            special = None
        
        # 添加詳細說明
        details = {
            'birth_numbers_used': [n for n in numbers if n in birth_numbers],
            'lucky_numbers_used': [n for n in numbers if n in lucky_nums] if lucky_numbers else [],
            'random_numbers_used': [n for n in numbers if n not in birth_numbers]
        }
        
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': reason,
            'confidence': confidence,
            'details': details
        })
    
    return results

# ... 其他推薦函數的實現 ... 