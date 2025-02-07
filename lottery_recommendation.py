import random
from datetime import datetime
import sqlite3
import numpy as np

def get_lottery_config(lottery_type):
    """獲取彩券配置"""
    configs = {
        'big-lotto': {
            'numbers': 6,          # 大樂透選6個號碼
            'max_number': 49,      # 號碼範圍1-49
            'special_number': True, # 有特別號
            'special_max': 49      # 特別號範圍1-49
        },
        'super-lotto': {
            'numbers': 6,          # 威力彩選6個號碼
            'max_number': 38,      # 第一區號碼範圍1-38
            'special_number': True, # 有特別號(第二區)
            'special_max': 8       # 第二區號碼範圍1-8
        },
        'daily-cash': {
            'numbers': 5,          # 今彩539選5個號碼
            'max_number': 39,      # 號碼範圍1-39
            'special_number': False # 沒有特別號
        }
    }
    return configs[lottery_type]

def get_special_number(lottery_type, config, numbers):
    """生成特別號的通用函數"""
    if not config['special_number']:
        return None
        
    if lottery_type == 'super-lotto':
        # 威力彩特別號範圍是1-8，且是獨立的第二區
        return random.randint(1, 8)
    elif lottery_type == 'big-lotto':
        # 大樂透的特別號是從已選的6個號碼中抽出1個
        return random.choice(numbers)
    else:
        # 其他彩種的特別號邏輯
        special = random.randint(1, config['special_max'])
        while special in numbers:
            special = random.randint(1, config['special_max'])
        return special

def get_quick_picks(lottery_type, count=5):
    """快速選號推薦"""
    config = get_lottery_config(lottery_type)
    results = []
    
    for _ in range(count):
        # 生成主要號碼
        numbers = sorted(random.sample(range(1, config['max_number'] + 1), config['numbers']))
        
        # 生成特別號
        special = get_special_number(lottery_type, config, numbers)
            
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': '隨機生成的號碼組合',
            'confidence': random.randint(60, 90)
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
        
        special = get_special_number(lottery_type, config, numbers)
        
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
        
        special = get_special_number(lottery_type, config, numbers)
        
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
        special = get_special_number(lottery_type, config, numbers)
        
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
        special = get_special_number(lottery_type, config, numbers)
        
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

def get_missing_value_combinations(lottery_type, periods=50, count=5):
    """根據遺漏值分析推薦號碼組合"""
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    config = get_lottery_config(lottery_type)
    
    table_name = {
        'big-lotto': 'big_lotto',
        'super-lotto': 'super_lotto',
        'daily-cash': 'daily_cash'
    }[lottery_type]
    
    # 獲取最近開出的號碼
    cursor.execute(f'''
        SELECT num1, num2, num3, num4, num5{', num6' if config['numbers'] == 6 else ''}
        FROM {table_name}
        ORDER BY draw_term DESC
        LIMIT ?
    ''', [periods])
    recent_draws = cursor.fetchall()
    
    # 計算每個號碼的遺漏值
    missing_values = {}
    for i in range(1, config['max_number'] + 1):
        # 找出最後一次出現的位置
        for idx, draw in enumerate(recent_draws):
            if i in draw:
                missing_values[i] = idx
                break
        else:
            # 如果在觀察期內都沒出現
            missing_values[i] = periods
    
    # 根據遺漏值排序
    sorted_numbers = sorted(missing_values.items(), key=lambda x: x[1], reverse=True)
    high_missing_numbers = [num for num, _ in sorted_numbers[:config['numbers'] * 2]]
    
    results = []
    for _ in range(count):
        # 從遺漏值高的號碼中選擇
        numbers = sorted(random.sample(high_missing_numbers, config['numbers']))
        
        # 生成特別號
        special = get_special_number(lottery_type, config, numbers)
        
        # 計算平均遺漏值
        avg_missing = sum(missing_values[n] for n in numbers) / len(numbers)
        
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': '遺漏值分析推薦',
            'details': {
                '平均遺漏期數': f'{avg_missing:.1f}期',
                '號碼遺漏值': ', '.join(f'{n}({missing_values[n]}期)' for n in numbers)
            }
        })
    
    conn.close()
    return results

def get_periodic_combinations(lottery_type, periods=50, count=5):
    """根據週期性分析推薦號碼組合"""
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    config = get_lottery_config(lottery_type)
    
    table_name = {
        'big-lotto': 'big_lotto',
        'super-lotto': 'super_lotto',
        'daily-cash': 'daily_cash'
    }[lottery_type]
    
    # 獲取最近開出的號碼
    cursor.execute(f'''
        SELECT num1, num2, num3, num4, num5{', num6' if config['numbers'] == 6 else ''}
        FROM {table_name}
        ORDER BY draw_term DESC
        LIMIT ?
    ''', [periods])
    recent_draws = cursor.fetchall()
    
    # 分析每個號碼的出現週期
    number_periods = {}
    for i in range(1, config['max_number'] + 1):
        appearances = []
        last_appearance = None
        
        # 記錄每次出現的位置
        for idx, draw in enumerate(recent_draws):
            if i in draw:
                if last_appearance is not None:
                    period = idx - last_appearance
                    appearances.append(period)
                last_appearance = idx
        
        # 計算平均週期和標準差
        if appearances:
            avg_period = sum(appearances) / len(appearances)
            std_dev = (sum((x - avg_period) ** 2 for x in appearances) / len(appearances)) ** 0.5
            stability = 1 / (std_dev + 1)  # 週期穩定性指標
        else:
            avg_period = periods
            stability = 0
        
        number_periods[i] = {
            'avg_period': avg_period,
            'stability': stability,
            'last_seen': last_appearance if last_appearance is not None else periods
        }
    
    # 選擇週期性較穩定且即將出現的號碼
    sorted_numbers = sorted(
        number_periods.items(),
        key=lambda x: (x[1]['stability'], -abs(x[1]['last_seen'] % x[1]['avg_period'])),
        reverse=True
    )
    
    periodic_numbers = [num for num, _ in sorted_numbers[:config['numbers'] * 2]]
    
    results = []
    for _ in range(count):
        numbers = sorted(random.sample(periodic_numbers, config['numbers']))
        
        special = get_special_number(lottery_type, config, numbers)
        
        # 生成號碼詳細資訊
        number_details = []
        for n in numbers:
            period_info = number_periods[n]
            number_details.append(
                f'{n}(平均每{period_info["avg_period"]:.1f}期出現一次, '
                f'最後一次出現在{period_info["last_seen"]}期前)'
            )
        
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': '週期性分析推薦',
            'details': {
                '號碼週期資訊': '\n'.join(number_details)
            }
        })
    
    conn.close()
    return results

def get_consecutive_combinations(lottery_type, count=5):
    """生成連號組合推薦"""
    config = get_lottery_config(lottery_type)
    results = []
    
    for _ in range(count):
        numbers = []
        # 隨機選擇一個起始點，確保後續的連號不會超出範圍
        max_start = config['max_number'] - config['numbers'] + 1
        start = random.randint(1, max_start)
        
        # 生成連號
        consecutive_count = random.randint(2, min(4, config['numbers']))  # 最多4個連號
        numbers.extend(range(start, start + consecutive_count))
        
        # 補充其他非連號
        remaining = config['numbers'] - consecutive_count
        available = [n for n in range(1, config['max_number'] + 1) 
                    if n not in numbers]
        numbers.extend(random.sample(available, remaining))
        numbers.sort()
        
        # 生成特別號
        special = get_special_number(lottery_type, config, numbers)
        
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': '連號組合推薦',
            'details': {
                '連號說明': f'包含{consecutive_count}個連續號碼'
            }
        })
    
    return results

def get_same_tail_combinations(lottery_type, count=5):
    """生成同尾數組合推薦"""
    config = get_lottery_config(lottery_type)
    results = []
    
    for _ in range(count):
        numbers = []
        # 隨機選擇1-2個尾數
        tail_count = random.randint(1, 2)
        selected_tails = random.sample(range(10), tail_count)  # 從0-9中選擇尾數
        
        for tail in selected_tails:
            # 找出所有符合該尾數的號碼
            tail_numbers = [n for n in range(1, config['max_number'] + 1) 
                          if n % 10 == tail]
            # 從中選擇2-3個號碼
            count_per_tail = random.randint(2, min(3, len(tail_numbers)))
            numbers.extend(random.sample(tail_numbers, count_per_tail))
        
        # 補充其他非同尾數號碼
        remaining = config['numbers'] - len(numbers)
        if remaining > 0:
            available = [n for n in range(1, config['max_number'] + 1) 
                        if n not in numbers]
            numbers.extend(random.sample(available, remaining))
        
        numbers.sort()
        
        # 生成特別號
        special = get_special_number(lottery_type, config, numbers)
        
        # 分析尾數分布
        tail_groups = {}
        for n in numbers:
            tail = n % 10
            if tail not in tail_groups:
                tail_groups[tail] = []
            tail_groups[tail].append(n)
        
        # 生成尾數說明
        tail_details = []
        for tail, nums in tail_groups.items():
            if len(nums) > 1:  # 只顯示有多個號碼的尾數
                tail_details.append(f'尾數{tail}: {", ".join(map(str, nums))}')
        
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': '同尾數組合推薦',
            'details': {
                '尾數分析': '\n'.join(tail_details)
            }
        })
    
    return results

def get_symmetric_combinations(lottery_type, count=5):
    """生成對稱號碼組合推薦"""
    config = get_lottery_config(lottery_type)
    results = []
    
    for _ in range(count):
        numbers = set()  # 使用 set 來避免重複
        # 選擇2-3對對稱數字
        symmetric_pairs_count = random.randint(2, min(3, config['numbers'] // 2))
        
        # 生成對稱數字對
        while len(numbers) < symmetric_pairs_count * 2:
            # 選擇一個數字，計算其對稱數字
            num = random.randint(1, config['max_number'] // 2)
            symmetric_num = config['max_number'] + 1 - num
            if num not in numbers and symmetric_num not in numbers:
                numbers.add(num)
                numbers.add(symmetric_num)
        
        # 補充其他非對稱數字
        remaining = config['numbers'] - len(numbers)
        if remaining > 0:
            available = [n for n in range(1, config['max_number'] + 1) 
                        if n not in numbers]
            numbers.update(random.sample(available, remaining))
        
        numbers = sorted(list(numbers))  # 轉換回列表並排序
        
        # 生成特別號
        special = get_special_number(lottery_type, config, numbers)
        
        # 找出對稱數字對
        symmetric_pairs = []
        used_numbers = set()
        for n in numbers:
            if n not in used_numbers:
                symmetric = config['max_number'] + 1 - n
                if symmetric in numbers:
                    symmetric_pairs.append(f"{n}-{symmetric}")
                    used_numbers.add(n)
                    used_numbers.add(symmetric)
        
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': '對稱號碼組合推薦',
            'details': {
                '對稱數字對': ', '.join(symmetric_pairs),
                '說明': f'包含{len(symmetric_pairs)}組對稱數字'
            }
        })
    
    return results

def get_high_frequency_combinations(lottery_type, periods=50, count=5):
    """生成高頻號碼組合推薦"""
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    config = get_lottery_config(lottery_type)
    
    table_name = {
        'big-lotto': 'big_lotto',
        'super-lotto': 'super_lotto',
        'daily-cash': 'daily_cash'
    }[lottery_type]
    
    # 獲取最近開出的號碼
    cursor.execute(f'''
        SELECT num1, num2, num3, num4, num5{', num6' if config['numbers'] == 6 else ''}
        FROM {table_name}
        ORDER BY draw_term DESC
        LIMIT ?
    ''', [periods])
    recent_draws = cursor.fetchall()
    
    # 分析每個號碼的出現頻率和週期
    number_stats = {}
    for i in range(1, config['max_number'] + 1):
        appearances = []
        last_appearance = None
        frequency = 0
        
        # 記錄每次出現的位置和計算頻率
        for idx, draw in enumerate(recent_draws):
            if i in draw:
                frequency += 1
                if last_appearance is not None:
                    period = idx - last_appearance
                    appearances.append(period)
                last_appearance = idx
        
        # 計算平均週期和頻率分數
        if appearances:
            avg_period = sum(appearances) / len(appearances)
            frequency_score = frequency * (1 / (avg_period + 1))  # 頻率越高、週期越短，分數越高
        else:
            frequency_score = 0
        
        number_stats[i] = {
            'frequency': frequency,
            'avg_period': avg_period if appearances else periods,
            'frequency_score': frequency_score,
            'last_seen': last_appearance if last_appearance is not None else periods
        }
    
    # 根據頻率分數排序
    sorted_numbers = sorted(
        number_stats.items(),
        key=lambda x: x[1]['frequency_score'],
        reverse=True
    )
    
    high_frequency_numbers = [num for num, _ in sorted_numbers[:config['numbers'] * 2]]
    
    results = []
    for _ in range(count):
        numbers = sorted(random.sample(high_frequency_numbers, config['numbers']))
        
        special = get_special_number(lottery_type, config, numbers)
        
        # 生成號碼詳細資訊
        number_details = []
        for n in numbers:
            stats = number_stats[n]
            number_details.append(
                f'{n}(出現{stats["frequency"]}次, '
                f'平均每{stats["avg_period"]:.1f}期出現一次, '
                f'最後一次出現在{stats["last_seen"]}期前)'
            )
        
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': '高頻號碼組合推薦',
            'details': {
                '號碼分析': '\n'.join(number_details)
            }
        })
    
    conn.close()
    return results

def get_golden_ratio_combinations(lottery_type, count=5):
    """基於黃金分割比例推薦號碼組合"""
    config = get_lottery_config(lottery_type)
    results = []
    golden_ratio = 1.618034  # 黃金比例
    
    for _ in range(count):
        numbers = set()
        
        # 使用黃金分割點劃分號碼範圍
        sections = []
        current = 1
        while current <= config['max_number']:
            next_point = min(int(current * golden_ratio), config['max_number'])
            sections.append((int(current), next_point))
            current = next_point + 1
        
        # 從每個分段中選擇號碼
        while len(numbers) < config['numbers']:
            section = random.choice(sections)
            number = random.randint(section[0], section[1])
            if number not in numbers and 1 <= number <= config['max_number']:
                numbers.add(number)
        
        numbers = sorted(list(numbers))
        
        # 生成特別號
        special = get_special_number(lottery_type, config, numbers)
        
        # 生成分段說明
        section_details = []
        for n in numbers:
            for i, (start, end) in enumerate(sections, 1):
                if start <= n <= end:
                    section_details.append(f'{n}(第{i}分段:{start}-{end})')
                    break
        
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': '黃金分割推薦',
            'details': {
                '分段說明': '\n'.join(section_details),
                '原理': '使用黃金比例(φ≈1.618)將號碼範圍分段選取'
            }
        })
    
    return results

def get_fibonacci_combinations(lottery_type, count=5):
    """基於費波那契數列推薦號碼組合"""
    config = get_lottery_config(lottery_type)
    results = []
    
    # 生成費波那契數列直到超過最大號碼
    fibonacci = [1, 1]
    while fibonacci[-1] <= config['max_number']:
        fibonacci.append(fibonacci[-1] + fibonacci[-2])
    fibonacci.pop()  # 移除超過範圍的最後一個數
    
    for _ in range(count):
        numbers = set()
        
        # 從費波那契數列中選擇號碼
        fib_count = min(3, config['numbers'])  # 至少選擇3個費波那契數
        available_fibs = [n for n in fibonacci if n <= config['max_number']]
        if available_fibs:
            fib_numbers = random.sample(available_fibs, min(fib_count, len(available_fibs)))
            numbers.update(fib_numbers)
        
        # 補充其他號碼
        remaining = config['numbers'] - len(numbers)
        if remaining > 0:
            available = [n for n in range(1, config['max_number'] + 1) 
                        if n not in numbers]
            numbers.update(random.sample(available, remaining))
        
        numbers = sorted(list(numbers))
        
        # 生成特別號
        special = get_special_number(lottery_type, config, numbers)
        
        # 標記費波那契數
        fib_details = []
        for n in numbers:
            if n in fibonacci:
                fib_details.append(f'{n}(費波那契數)')
            else:
                fib_details.append(str(n))
        
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': '費波那契數列推薦',
            'details': {
                '號碼說明': ', '.join(fib_details),
                '原理': '包含費波那契數列中的數字(1,1,2,3,5,8,13,21,34...)'
            }
        })
    
    return results

def get_arithmetic_combinations(lottery_type, count=5):
    """基於等差數列推薦號碼組合"""
    config = get_lottery_config(lottery_type)
    results = []
    
    for _ in range(count):
        numbers = set()
        
        # 隨機選擇等差數列的起始點和公差
        start = random.randint(1, config['max_number'] // 2)
        # 根據最大號碼和需要的號碼數量來限制公差範圍
        max_diff = (config['max_number'] - start) // (config['numbers'] - 1)
        if max_diff < 1:
            max_diff = 1
        diff = random.randint(1, min(max_diff, 5))  # 限制公差最大為5
        
        # 生成等差數列
        sequence_count = random.randint(3, config['numbers'])  # 至少3個等差數
        for i in range(sequence_count):
            num = start + i * diff
            if 1 <= num <= config['max_number']:
                numbers.add(num)
        
        # 補充其他號碼
        remaining = config['numbers'] - len(numbers)
        if remaining > 0:
            available = [n for n in range(1, config['max_number'] + 1) 
                        if n not in numbers]
            numbers.update(random.sample(available, remaining))
        
        numbers = sorted(list(numbers))
        
        # 生成特別號
        special = get_special_number(lottery_type, config, numbers)
        
        # 找出等差數列
        arithmetic_sequences = []
        for i in range(len(numbers)-2):
            for j in range(i+1, len(numbers)-1):
                for k in range(j+1, len(numbers)):
                    if numbers[k] - numbers[j] == numbers[j] - numbers[i]:
                        arithmetic_sequences.append(
                            f"{numbers[i]},{numbers[j]},{numbers[k]}(公差:{numbers[j]-numbers[i]})"
                        )
        
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': '等差數列推薦',
            'details': {
                '等差序列': '\n'.join(arithmetic_sequences),
                '說明': f'包含{len(arithmetic_sequences)}組等差數列'
            }
        })
    
    return results

def get_zodiac_combinations(lottery_type, count=5):
    """基於生肖特性推薦號碼組合"""
    config = get_lottery_config(lottery_type)
    results = []
    
    # 定義生肖對應的號碼（根據台灣彩券的生肖號碼分配）
    zodiac_numbers = {
        '鼠': [1, 13, 25, 37, 49],
        '牛': [2, 14, 26, 38],
        '虎': [3, 15, 27, 39],
        '兔': [4, 16, 28, 40],
        '龍': [5, 17, 29, 41],
        '蛇': [6, 18, 30, 42],
        '馬': [7, 19, 31, 43],
        '羊': [8, 20, 32, 44],
        '猴': [9, 21, 33, 45],
        '雞': [10, 22, 34, 46],
        '狗': [11, 23, 35, 47],
        '豬': [12, 24, 36, 48]
    }
    
    for _ in range(count):
        numbers = set()
        selected_zodiacs = []
        
        # 隨機選擇3-4個生肖
        zodiac_count = random.randint(3, 4)
        chosen_zodiacs = random.sample(list(zodiac_numbers.keys()), zodiac_count)
        
        # 從每個選中的生肖中選擇號碼
        for zodiac in chosen_zodiacs:
            available = [n for n in zodiac_numbers[zodiac] 
                        if n <= config['max_number'] and n not in numbers]
            if available:
                num = random.choice(available)
                numbers.add(num)
                selected_zodiacs.append(f"{zodiac}({num})")
        
        # 補充其他號碼
        remaining = config['numbers'] - len(numbers)
        if remaining > 0:
            available = [n for n in range(1, config['max_number'] + 1) 
                        if n not in numbers]
            numbers.update(random.sample(available, remaining))
        
        numbers = sorted(list(numbers))
        
        # 生成特別號
        special = get_special_number(lottery_type, config, numbers)
        
        # 找出每個號碼對應的生肖
        number_zodiacs = []
        for n in numbers:
            for zodiac, nums in zodiac_numbers.items():
                if n in nums:
                    number_zodiacs.append(f"{n}({zodiac})")
                    break
        
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': '生肖號碼組合推薦',
            'details': {
                '選用生肖': '、'.join(selected_zodiacs),
                '號碼生肖': ', '.join(number_zodiacs)
            }
        })
    
    return results

def get_common_combinations(lottery_type, count=5):
    """生成常見組合推薦"""
    config = get_lottery_config(lottery_type)
    results = []
    
    # 定義常見的組合模式
    patterns = {
        'big-lotto': [
            {
                'name': '生日組合',
                'ranges': [(1, 31)],  # 日期範圍
                'count': 4  # 從日期範圍選4個
            },
            {
                'name': '對角線組合',
                'ranges': [(1, 10), (11, 20), (31, 40), (41, 49)],
                'count': [2, 1, 2, 1]  # 從每個範圍分別選擇的數量
            },
            {
                'name': '區塊組合',
                'ranges': [(1, 25), (26, 49)],  # 前半區和後半區
                'count': [3, 3]  # 每區選3個
            }
        ],
        'super-lotto': [
            {
                'name': '生日組合',
                'ranges': [(1, 31)],
                'count': 4
            },
            {
                'name': '區塊組合',
                'ranges': [(1, 19), (20, 38)],
                'count': [3, 3]
            }
        ],
        'daily-cash': [
            {
                'name': '生日組合',
                'ranges': [(1, 31)],
                'count': 3
            },
            {
                'name': '區塊組合',
                'ranges': [(1, 20), (21, 39)],
                'count': [3, 2]
            }
        ]
    }
    
    for _ in range(count):
        # 隨機選擇一種組合模式
        pattern = random.choice(patterns[lottery_type])
        numbers = set()
        
        if isinstance(pattern['count'], list):
            # 多區間選號
            for (start, end), cnt in zip(pattern['ranges'], pattern['count']):
                available = [n for n in range(start, end + 1) 
                           if n <= config['max_number'] and n not in numbers]
                if available and cnt <= len(available):
                    numbers.update(random.sample(available, cnt))
        else:
            # 單區間選號
            start, end = pattern['ranges'][0]
            available = [n for n in range(start, end + 1) 
                        if n <= config['max_number']]
            if available and pattern['count'] <= len(available):
                numbers.update(random.sample(available, pattern['count']))
        
        # 補充其他號碼
        remaining = config['numbers'] - len(numbers)
        if remaining > 0:
            available = [n for n in range(1, config['max_number'] + 1) 
                        if n not in numbers]
            numbers.update(random.sample(available, remaining))
        
        numbers = sorted(list(numbers))
        
        # 生成特別號
        special = get_special_number(lottery_type, config, numbers)
        
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': f'常見組合推薦({pattern["name"]})',
            'details': {
                '組合類型': pattern['name'],
                '說明': '根據玩家常用的選號模式生成'
            }
        })
    
    return results

def get_festival_combinations(lottery_type, count=5):
    """節日主題號碼組合推薦"""
    config = get_lottery_config(lottery_type)
    results = []
    
    # 定義節日相關的號碼組合
    festivals = {
        'chinese_new_year': {
            'name': '農曆新年',
            'numbers': [1, 2, 3, 8, 9],  # 代表大吉大利
            'ranges': [(1, 9)],  # 偏好個位數
            'weight': 3  # 權重
        },
        'dragon_boat': {
            'name': '端午節',
            'numbers': [5, 15, 25, 35, 45],  # 5代表五月初五
            'ranges': [(1, 31)],
            'weight': 2
        },
        'mid_autumn': {
            'name': '中秋節',
            'numbers': [8, 15, 18, 28, 38],  # 8代表八月十五
            'ranges': [(1, 38)],
            'weight': 2
        },
        'christmas': {
            'name': '聖誕節',
            'numbers': [12, 24, 25],  # 12月24/25日
            'ranges': [(1, 25)],
            'weight': 1
        }
    }
    
    for _ in range(count):
        numbers = set()
        # 隨機選擇一個節日主題
        festival = random.choices(
            list(festivals.values()), 
            weights=[f['weight'] for f in festivals.values()]
        )[0]
        
        # 從節日特定號碼中選擇
        available_numbers = [n for n in festival['numbers'] 
                           if n <= config['max_number']]
        if available_numbers:
            numbers.update(
                random.sample(
                    available_numbers,
                    min(3, len(available_numbers))  # 最多選3個特定號碼
                )
            )
        
        # 從節日相關範圍中補充號碼
        remaining = config['numbers'] - len(numbers)
        if remaining > 0:
            for start, end in festival['ranges']:
                available = [n for n in range(start, min(end + 1, config['max_number'] + 1))
                           if n not in numbers]
                if available and remaining > 0:
                    sample_size = min(remaining, len(available))
                    numbers.update(random.sample(available, sample_size))
                    remaining -= sample_size
        
        # 如果還需要補充號碼
        if len(numbers) < config['numbers']:
            available = [n for n in range(1, config['max_number'] + 1)
                        if n not in numbers]
            numbers.update(random.sample(available, config['numbers'] - len(numbers)))
        
        numbers = sorted(list(numbers))
        
        # 生成特別號
        special = get_special_number(lottery_type, config, numbers)
        
        results.append({
            'numbers': numbers,
            'special_number': special,
            'reason': f'節日主題推薦({festival["name"]})',
            'details': {
                '節日': festival['name'],
                '說明': '根據傳統節日的吉利數字和特殊日期推薦'
            }
        })
    
    return results

# ... 其他推薦函數的實現 ... 