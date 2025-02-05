import sqlite3

def analyze_lottery(lottery_type, periods):
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    
    # 根據彩券類型設定參數
    if lottery_type == 'big-lotto':
        table = 'big_lotto'
        max_number = 49
        num_columns = 6
    elif lottery_type == 'super-lotto':
        table = 'super_lotto'
        max_number = 38
        num_columns = 6
    else:  # daily-cash
        table = 'daily_cash'
        max_number = 39
        num_columns = 5
    
    # 定義要查詢的欄位
    columns = [f'num{i}' for i in range(1, num_columns + 1)]
    column_str = ', '.join(columns)
    
    # 獲取分析範圍內的所有期數，按照時間順序排列
    cursor.execute(f'''
        SELECT draw_term, {column_str}
        FROM {table} 
        ORDER BY draw_term DESC 
        LIMIT {periods}
    ''')
    draws = cursor.fetchall()
    
    # 準備分析結果
    results = {}
    for num in range(1, max_number + 1):
        # 計算出現次數
        placeholders = ' OR '.join([f'{col}=?' for col in columns])
        cursor.execute(f'''
            SELECT COUNT(*) 
            FROM (
                SELECT * FROM {table} 
                ORDER BY draw_term DESC 
                LIMIT ?
            )
            WHERE {placeholders}
        ''', [periods] + [num] * num_columns)
        frequency = cursor.fetchone()[0]
        
        # 計算最近開出期數和遺漏期數
        missing_periods = 0
        last_drawn_term = '未開出'
        
        # 遍歷最近N期的開獎號碼
        for i, draw in enumerate(draws):
            draw_numbers = draw[1:]  # 第一個元素是期數
            if num in draw_numbers:
                missing_periods = i
                last_drawn_term = draw[0]
                break
        else:
            missing_periods = periods
        
        results[num] = {
            'frequency': frequency,
            'frequency_rate': round(frequency / periods * 100, 2),
            'missing_periods': missing_periods,
            'last_drawn': str(last_drawn_term)
        }
    
    conn.close()
    return results 

def analyze_repeat_numbers(lottery_type, periods=50):
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    
    # 根據彩券類型設定參數
    if lottery_type == 'big-lotto':
        table = 'big_lotto'
        num_columns = 6
    elif lottery_type == 'super-lotto':
        table = 'super_lotto'
        num_columns = 6
    else:  # daily-cash
        table = 'daily_cash'
        num_columns = 5
        
    # 獲取最近N期的開獎號碼
    columns = [f'num{i}' for i in range(1, num_columns + 1)]
    column_str = ', '.join(columns)
    
    cursor.execute(f'''
        SELECT draw_term, {column_str}
        FROM {table}
        ORDER BY draw_term DESC
        LIMIT {periods}
    ''')
    draws = cursor.fetchall()
    
    # 轉換每期號碼為集合，方便比較
    draw_sets = [set(draw[1:]) for draw in draws]
    
    # 分析每個號碼的重複情況
    results = {}
    
    # 遍歷每一期
    for i in range(len(draws)):
        current_draw = set(draws[i][1:])
        
        # 與上一期比較
        last_draw_repeat = set()
        if i < len(draws) - 1:
            last_draw_repeat = current_draw & draw_sets[i + 1]
        
        # 與前3期比較
        recent_3_draws = set()
        if i < len(draws) - 3:
            for j in range(i + 1, min(i + 4, len(draws))):
                recent_3_draws |= current_draw & draw_sets[j]
        
        # 與前5期比較
        recent_5_draws = set()
        if i < len(draws) - 5:
            for j in range(i + 1, min(i + 6, len(draws))):
                recent_5_draws |= current_draw & draw_sets[j]
        
        # 統計每個號碼的重複情況
        for num in current_draw:
            if num not in results:
                results[num] = {
                    'last_draw_repeat': 0,
                    'recent_3_draws': 0,
                    'recent_5_draws': 0,
                    'most_repeated': 0
                }
            
            if num in last_draw_repeat:
                results[num]['last_draw_repeat'] += 1
            if num in recent_3_draws:
                results[num]['recent_3_draws'] += 1
            if num in recent_5_draws:
                results[num]['recent_5_draws'] += 1
    
    # 修改計算期數內最常重複的次數的邏輯
    for num in results:
        cursor.execute(f'''
            WITH recent_draws AS (
                SELECT * FROM {table}
                ORDER BY draw_term DESC
                LIMIT ?
            )
            SELECT COUNT(*) 
            FROM recent_draws
            WHERE {' OR '.join([f'{col}=?' for col in columns])}
        ''', [periods] + [num] * num_columns)
        total_appearances = cursor.fetchone()[0]
        
        # 計算實際重複次數（出現次數減1就是重複次數）
        results[num]['most_repeated'] = max(0, total_appearances - 1)
    
    conn.close()
    return results 

def get_zodiac_year():
    # 定義生肖號碼對應表（以大樂透為例）
    zodiac_numbers = {
        '鼠': [4, 16, 28, 40],
        '牛': [3, 15, 27, 39],
        '虎': [2, 14, 26, 38],
        '兔': [1, 13, 25, 37, 49],
        '龍': [12, 24, 36, 48],
        '蛇': [11, 23, 35, 47],
        '馬': [10, 22, 34, 46],
        '羊': [9, 21, 33, 45],
        '猴': [8, 20, 32, 44],
        '雞': [7, 19, 31, 43],
        '狗': [6, 18, 30, 42],
        '豬': [5, 17, 29, 41]
    }
    return zodiac_numbers

def analyze_special_numbers(lottery_type, periods=50):
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    
    # 根據彩券類型設定參數
    if lottery_type == 'big-lotto':  # 大樂透 1-49
        table = 'big_lotto'
        num_columns = 6
        max_number = 49
        zodiac_numbers = get_zodiac_year()  # 原有的生肖對應表
        elements_numbers = {
            '金': [1,2,8,9,15,16,22,23,29,30,36,37,43,44],
            '木': [3,4,10,11,17,18,24,25,31,32,38,39,45,46],
            '水': [5,6,12,13,19,20,26,27,33,34,40,41,47,48],
            '火': [7,14,21,28,35,42,49],
            '土': [19,24,31,32,39,40,45,46]
        }
        
    elif lottery_type == 'super-lotto':  # 威力彩第一區 1-38
        table = 'super_lotto'
        num_columns = 6
        max_number = 38
        # 修改生肖對應表只包含1-38
        zodiac_numbers = {k: [n for n in v if n <= 38] for k, v in get_zodiac_year().items()}
        # 修改五行對應表只包含1-38
        elements_numbers = {
            '金': [1,2,8,9,15,16,22,23,29,30,36,37],
            '木': [3,4,10,11,17,18,24,25,31,32,38],
            '水': [5,6,12,13,19,20,26,27,33,34],
            '火': [7,14,21,28,35],
            '土': [19,24,31,32]
        }
        
    else:  # 今彩539 1-39
        table = 'daily_cash'
        num_columns = 5
        max_number = 39
        # 修改生肖對應表只包含1-39
        zodiac_numbers = {k: [n for n in v if n <= 39] for k, v in get_zodiac_year().items()}
        # 修改五行對應表只包含1-39
        elements_numbers = {
            '金': [1,2,8,9,15,16,22,23,29,30,36,37],
            '木': [3,4,10,11,17,18,24,25,31,32,38,39],
            '水': [5,6,12,13,19,20,26,27,33,34],
            '火': [7,14,21,28,35],
            '土': [19,24,31,32,39]
        }
    
    # 獲取最近N期的開獎號碼
    columns = [f'num{i}' for i in range(1, num_columns + 1)]
    column_str = ', '.join(columns)
    
    cursor.execute(f'''
        SELECT {column_str}
        FROM {table}
        ORDER BY draw_term DESC
        LIMIT {periods}
    ''')
    draws = cursor.fetchall()
    
    # 先計算數字特性的範圍
    prime_numbers = [n for n in range(2, max_number + 1) if is_prime(n)]
    composite_numbers = [n for n in range(1, max_number + 1) if n not in prime_numbers]
    odd_numbers = [n for n in range(1, max_number + 1) if n % 2 == 1]
    even_numbers = [n for n in range(1, max_number + 1) if n % 2 == 0]
    
    # 初始化結果
    results = {
        'zodiac': {
            zodiac: {
                'count': 0,
                'rate': 0,
                'numbers': numbers
            }
            for zodiac, numbers in zodiac_numbers.items()
        },
        'elements': {
            element: {
                'count': 0,
                'rate': 0,
                'numbers': numbers
            }
            for element, numbers in elements_numbers.items()
        },
        'numbers': {
            '質數': {'count': 0, 'rate': 0, 'numbers': prime_numbers},
            '合數': {'count': 0, 'rate': 0, 'numbers': composite_numbers},
            '奇數': {'count': 0, 'rate': 0, 'numbers': odd_numbers},
            '偶數': {'count': 0, 'rate': 0, 'numbers': even_numbers}
        }
    }
    
    # 分析每一期的號碼
    total_numbers = len(draws) * num_columns
    for draw in draws:
        for num in draw:
            # 生肖分析
            for zodiac, numbers in zodiac_numbers.items():
                if num in numbers:
                    results['zodiac'][zodiac]['count'] += 1
            
            # 五行分析
            if num in elements_numbers['金']:
                results['elements']['金']['count'] += 1
            elif num in elements_numbers['木']:
                results['elements']['木']['count'] += 1
            elif num in elements_numbers['水']:
                results['elements']['水']['count'] += 1
            elif num in elements_numbers['火']:
                results['elements']['火']['count'] += 1
            else:
                results['elements']['土']['count'] += 1
            
            # 數字特性分析
            if num in prime_numbers:
                results['numbers']['質數']['count'] += 1
            else:
                results['numbers']['合數']['count'] += 1
            
            if num % 2 == 1:
                results['numbers']['奇數']['count'] += 1
            else:
                results['numbers']['偶數']['count'] += 1
    
    # 計算百分比
    for zodiac in results['zodiac']:
        results['zodiac'][zodiac]['rate'] = round(results['zodiac'][zodiac]['count'] / total_numbers * 100, 2)
    
    for element in results['elements']:
        results['elements'][element]['rate'] = round(results['elements'][element]['count'] / total_numbers * 100, 2)
    
    for number_type in results['numbers']:
        results['numbers'][number_type]['rate'] = round(results['numbers'][number_type]['count'] / total_numbers * 100, 2)
    
    conn.close()
    return results

def is_prime(n):
    if n < 2:
        return False
    for i in range(2, int(n ** 0.5) + 1):
        if n % i == 0:
            return False
    return True 

def analyze_combination_numbers(lottery_type, periods=50):
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    
    # 根據彩券類型設定參數
    if lottery_type == 'big-lotto':
        table = 'big_lotto'
        num_columns = 6
        max_number = 49
    elif lottery_type == 'super-lotto':
        table = 'super_lotto'
        num_columns = 6
        max_number = 38
    else:  # daily-cash
        table = 'daily_cash'
        num_columns = 5
        max_number = 39
    
    # 獲取最近N期的開獎號碼
    columns = [f'num{i}' for i in range(1, num_columns + 1)]
    column_str = ', '.join(columns)
    
    cursor.execute(f'''
        SELECT {column_str}
        FROM {table}
        ORDER BY draw_term DESC
        LIMIT {periods}
    ''')
    draws = cursor.fetchall()
    
    # 初始化結果
    results = {
        'size_ratio': {'big': 0, 'small': 0, 'most_common_ratio': ''},
        'odd_even_ratio': {'odd': 0, 'even': 0, 'most_common_ratio': ''},
        'range_distribution': {
            '1-9': {'count': 0, 'rate': 0},
            '10-19': {'count': 0, 'rate': 0},
            '20-29': {'count': 0, 'rate': 0},
            '30-39': {'count': 0, 'rate': 0}
        },
        'popular_combinations': []
    }
    
    if max_number > 39:
        results['range_distribution']['40-49'] = {'count': 0, 'rate': 0}
    
    # 分析大小比例和單雙比例
    ratio_counts = {'size': {}, 'odd_even': {}}
    total_numbers = 0
    big_count = 0
    odd_count = 0
    
    for draw in draws:
        # 計算當期大小比和單雙比
        big = sum(1 for num in draw if num >= 25)
        small = num_columns - big
        odd = sum(1 for num in draw if num % 2 == 1)
        even = num_columns - odd
        
        # 記錄比例出現次數
        size_ratio = f"{big}:{small}"
        odd_even_ratio = f"{odd}:{even}"
        ratio_counts['size'][size_ratio] = ratio_counts['size'].get(size_ratio, 0) + 1
        ratio_counts['odd_even'][odd_even_ratio] = ratio_counts['odd_even'].get(odd_even_ratio, 0) + 1
        
        # 統計大號和單號總數
        big_count += big
        odd_count += odd
        
        # 區間分布分析
        for num in draw:
            total_numbers += 1
            if 1 <= num <= 9:
                results['range_distribution']['1-9']['count'] += 1
            elif 10 <= num <= 19:
                results['range_distribution']['10-19']['count'] += 1
            elif 20 <= num <= 29:
                results['range_distribution']['20-29']['count'] += 1
            elif 30 <= num <= 39:
                results['range_distribution']['30-39']['count'] += 1
            elif num >= 40:
                results['range_distribution']['40-49']['count'] += 1
    
    # 計算比例
    results['size_ratio']['big'] = round(big_count / total_numbers * 100, 2)
    results['size_ratio']['small'] = round(100 - results['size_ratio']['big'], 2)
    results['odd_even_ratio']['odd'] = round(odd_count / total_numbers * 100, 2)
    results['odd_even_ratio']['even'] = round(100 - results['odd_even_ratio']['odd'], 2)
    
    # 找出最常見的比例
    results['size_ratio']['most_common_ratio'] = max(ratio_counts['size'].items(), key=lambda x: x[1])[0]
    results['odd_even_ratio']['most_common_ratio'] = max(ratio_counts['odd_even'].items(), key=lambda x: x[1])[0]
    
    # 計算區間分布比例
    for range_key in results['range_distribution']:
        results['range_distribution'][range_key]['rate'] = round(
            results['range_distribution'][range_key]['count'] / total_numbers * 100, 2
        )
    
    # 分析熱門組合
    combinations = {}
    for draw in draws:
        draw_set = tuple(sorted(draw))
        combinations[draw_set] = combinations.get(draw_set, 0) + 1
    
    # 取出前5個最常出現的組合
    popular_combinations = sorted(combinations.items(), key=lambda x: x[1], reverse=True)[:5]
    results['popular_combinations'] = [
        {'numbers': list(combo), 'count': count}
        for combo, count in popular_combinations
    ]
    
    conn.close()
    return results 

def analyze_prediction_numbers(lottery_type, periods=50):
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    
    # 根據彩券類型設定參數
    if lottery_type == 'big-lotto':
        table = 'big_lotto'
        num_columns = 6
        max_number = 49
    elif lottery_type == 'super-lotto':
        table = 'super_lotto'
        num_columns = 6
        max_number = 38
    else:  # daily-cash
        table = 'daily_cash'
        num_columns = 5
        max_number = 39
    
    # 獲取最近N期的開獎號碼
    columns = [f'num{i}' for i in range(1, num_columns + 1)]
    column_str = ', '.join(columns)
    
    cursor.execute(f'''
        SELECT {column_str}
        FROM {table}
        ORDER BY draw_term DESC
        LIMIT {periods}
    ''')
    draws = cursor.fetchall()
    
    # 統計每個號碼的出現次數和遺漏期數
    number_stats = {}
    for i in range(1, max_number + 1):
        number_stats[i] = {
            'frequency': 0,
            'missing_periods': 0
        }
    
    # 計算頻率和遺漏期數
    for draw in draws:
        for num in draw:
            number_stats[num]['frequency'] += 1
    
    # 計算遺漏期數
    for i in range(1, max_number + 1):
        for j, draw in enumerate(draws):
            if i in draw:
                number_stats[i]['missing_periods'] = j
                break
            if j == len(draws) - 1:
                number_stats[i]['missing_periods'] = periods
    
    # 選出遺漏值最高的前6個號碼
    missing_numbers = sorted(
        [(num, stats['missing_periods']) for num, stats in number_stats.items()],
        key=lambda x: x[1],
        reverse=True
    )[:num_columns]
    
    # 選出出現頻率最高的前6個號碼
    hot_numbers = sorted(
        [(num, stats['frequency']) for num, stats in number_stats.items()],
        key=lambda x: x[1],
        reverse=True
    )[:num_columns]
    
    # 選出出現頻率最低的前6個號碼
    cold_numbers = sorted(
        [(num, stats['frequency']) for num, stats in number_stats.items()],
        key=lambda x: x[1]
    )[:num_columns]
    
    # 生成號碼組合建議
    suggested_combinations = [
        {
            'numbers': [num for num, _ in missing_numbers],
            'reason': '長期未開出的號碼組合'
        },
        {
            'numbers': [num for num, _ in hot_numbers],
            'reason': '近期高頻出現的號碼組合'
        },
        {
            'numbers': [num for num, _ in cold_numbers],
            'reason': '近期低頻出現的號碼組合'
        },
        {
            'numbers': [num for num, _ in missing_numbers[:3] + hot_numbers[:3]],
            'reason': '遺漏值高和高頻號碼的混合組合'
        },
        {
            'numbers': [num for num, _ in missing_numbers[:3] + cold_numbers[:3]],
            'reason': '遺漏值高和低頻號碼的混合組合'
        }
    ]
    
    results = {
        'missing_numbers': [num for num, _ in missing_numbers],
        'missing_periods': [periods for _, periods in missing_numbers],
        'hot_numbers': [num for num, _ in hot_numbers],
        'hot_frequencies': [freq for _, freq in hot_numbers],
        'cold_numbers': [num for num, _ in cold_numbers],
        'cold_frequencies': [freq for _, freq in cold_numbers],
        'suggested_combinations': suggested_combinations
    }
    
    conn.close()
    return results 

def analyze_route_numbers(lottery_type, periods=50):
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    
    # 根據彩券類型設定參數
    if lottery_type == 'big-lotto':
        table = 'big_lotto'
        num_columns = 6
        max_number = 49
    elif lottery_type == 'super-lotto':
        table = 'super_lotto'
        num_columns = 6
        max_number = 38
    else:  # daily-cash
        table = 'daily_cash'
        num_columns = 5
        max_number = 39
    
    # 獲取最近N期的開獎號碼
    columns = [f'num{i}' for i in range(1, num_columns + 1)]
    column_str = ', '.join(columns)
    
    cursor.execute(f'''
        SELECT {column_str}
        FROM {table}
        ORDER BY draw_term DESC
        LIMIT {periods}
    ''')
    draws = cursor.fetchall()
    
    # 初始化尾數分布統計
    digit_distribution = {str(i): {'count': 0, 'rate': 0, 'numbers': []} for i in range(10)}
    
    # 統計尾數分布
    total_numbers = 0
    for draw in draws:
        for num in draw:
            digit = num % 10
            digit_distribution[str(digit)]['count'] += 1
            if num not in digit_distribution[str(digit)]['numbers']:
                digit_distribution[str(digit)]['numbers'].append(num)
            total_numbers += 1
    
    # 計算尾數出現率
    for digit in digit_distribution:
        digit_distribution[digit]['rate'] = round(digit_distribution[digit]['count'] / total_numbers * 100, 2)
        digit_distribution[digit]['numbers'].sort()
    
    # 分析尾數組合
    digit_combinations = {}
    for draw in draws:
        digits = tuple(sorted(num % 10 for num in draw))
        digit_combinations[digits] = digit_combinations.get(digits, 0) + 1
    
    # 找出最常出現的尾數組合
    popular_digit_combinations = sorted(
        [{'digits': list(combo), 'count': count} 
         for combo, count in digit_combinations.items()],
        key=lambda x: x['count'],
        reverse=True
    )[:5]
    
    # 分析尾數連號
    consecutive_count = 0
    consecutive_digits = []
    for draw in draws:
        digits = sorted(num % 10 for num in draw)
        for i in range(len(digits) - 1):
            if digits[i] + 1 == digits[i + 1]:
                consecutive_count += 1
                consecutive_digits.append([digits[i], digits[i + 1]])
    
    # 計算尾數連號出現率
    consecutive_digits_rate = round(consecutive_count / (len(draws) * (num_columns - 1)) * 100, 2)
    
    # 找出最常見的連號組合
    most_common_consecutive = []
    if consecutive_digits:
        most_common_consecutive = max(consecutive_digits, key=consecutive_digits.count)
    
    # 分析尾數重複
    repeat_count = 0
    repeated_digits = []
    for draw in draws:
        digits = [num % 10 for num in draw]
        for digit in set(digits):
            if digits.count(digit) > 1:
                repeat_count += 1
                repeated_digits.append(digit)
    
    # 計算尾數重複出現率
    repeat_digits_rate = round(repeat_count / len(draws) * 100, 2)
    
    # 找出最常重複的尾數
    most_repeated_digits = []
    if repeated_digits:
        digit_counts = {}
        for digit in repeated_digits:
            digit_counts[digit] = digit_counts.get(digit, 0) + 1
        most_repeated_digits = sorted(digit_counts.items(), key=lambda x: x[1], reverse=True)[:3]
        most_repeated_digits = [digit for digit, _ in most_repeated_digits]
    
    results = {
        'digit_distribution': digit_distribution,
        'popular_digit_combinations': popular_digit_combinations,
        'consecutive_digits_rate': consecutive_digits_rate,
        'most_common_consecutive': most_common_consecutive,
        'repeat_digits_rate': repeat_digits_rate,
        'most_repeated_digits': most_repeated_digits
    }
    
    conn.close()
    return results 

def analyze_repetition_numbers(lottery_type, periods=50):
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    
    # 根據彩券類型設定參數
    if lottery_type == 'big-lotto':
        table = 'big_lotto'
        num_columns = 6
        max_number = 49
    elif lottery_type == 'super-lotto':
        table = 'super_lotto'
        num_columns = 6
        max_number = 38
    else:  # daily-cash
        table = 'daily_cash'
        num_columns = 5
        max_number = 39
    
    # 獲取最近N期的開獎號碼
    columns = [f'num{i}' for i in range(1, num_columns + 1)]
    column_str = ', '.join(columns)
    
    cursor.execute(f'''
        SELECT {column_str}
        FROM {table}
        ORDER BY draw_term DESC
        LIMIT {periods}
    ''')
    draws = cursor.fetchall()
    
    # 分析相鄰期重複
    adjacent_repeat_count = 0
    adjacent_repeated_numbers = []
    total_adjacent_repeats = 0
    
    for i in range(len(draws) - 1):
        current_draw = set(draws[i])
        next_draw = set(draws[i + 1])
        repeats = current_draw & next_draw
        if repeats:
            adjacent_repeat_count += 1
            adjacent_repeated_numbers.extend(list(repeats))
            total_adjacent_repeats += len(repeats)
    
    # 計算相鄰期重複率和平均重複個數
    adjacent_repeat_rate = round(adjacent_repeat_count / (len(draws) - 1) * 100, 2)
    avg_adjacent_repeat_count = round(total_adjacent_repeats / (len(draws) - 1), 2)
    
    # 找出最常重複的號碼
    most_adjacent_repeated = []
    if adjacent_repeated_numbers:
        from collections import Counter
        number_counts = Counter(adjacent_repeated_numbers)
        most_common = number_counts.most_common(3)
        most_adjacent_repeated = [num for num, _ in most_common]
    
    # 分析間隔期重複
    interval_stats = {}
    for interval in [2, 3, 5, 10]:
        repeat_count = 0
        repeated_numbers = set()
        
        for i in range(len(draws) - interval):
            current_draw = set(draws[i])
            interval_draw = set(draws[i + interval])
            repeats = current_draw & interval_draw
            if repeats:
                repeat_count += 1
                repeated_numbers.update(repeats)
        
        repeat_rate = round(repeat_count / (len(draws) - interval) * 100, 2)
        interval_stats[f'{interval}'] = {
            'repeat_rate': repeat_rate,
            'numbers': sorted(list(repeated_numbers))
        }
    
    # 分析週期性重複
    periodic_patterns = []
    for period in range(2, min(11, len(draws) // 2)):
        pattern_counts = {}
        
        for i in range(len(draws) - period):
            current_draw = set(draws[i])
            period_draw = set(draws[i + period])
            repeats = current_draw & period_draw
            
            if repeats:
                repeats_tuple = tuple(sorted(repeats))
                if repeats_tuple not in pattern_counts:
                    pattern_counts[repeats_tuple] = {'count': 0, 'period': period}
                pattern_counts[repeats_tuple]['count'] += 1
        
        # 找出該週期最顯著的模式
        if pattern_counts:
            best_pattern = max(pattern_counts.items(), key=lambda x: x[1]['count'])
            periodic_patterns.append({
                'period': period,
                'numbers': list(best_pattern[0]),
                'count': best_pattern[1]['count']
            })
    
    # 按出現次數排序週期性模式
    periodic_patterns.sort(key=lambda x: x['count'], reverse=True)
    periodic_patterns = periodic_patterns[:5]  # 只保留前5個最顯著的模式
    
    # 分析重複組合
    combination_repeats = {}
    for i in range(len(draws) - 1):
        current_draw = set(draws[i])
        
        # 向後查找重複組合
        for j in range(i + 1, len(draws)):
            next_draw = set(draws[j])
            common_numbers = current_draw & next_draw
            
            # 如果有4個或以上號碼相同
            if len(common_numbers) >= 4:
                common_numbers = tuple(sorted(common_numbers))
                if common_numbers not in combination_repeats:
                    combination_repeats[common_numbers] = {
                        'count': 0,
                        'intervals': [],
                        'match_count': len(common_numbers)
                    }
                combination_repeats[common_numbers]['count'] += 1
                combination_repeats[common_numbers]['intervals'].append(j - i)
    
    # 轉換重複組合為列表格式
    repeated_combinations = [
        {
            'numbers': list(combo),
            'repeat_count': stats['count'],
            'intervals': stats['intervals'],
            'reason': f'有{stats["match_count"]}個號碼相同'
        }
        for combo, stats in sorted(
            combination_repeats.items(),
            key=lambda x: (x[1]['match_count'], x[1]['count']),
            reverse=True
        )[:5]  # 只保留前5個最顯著的組合
    ]
    
    results = {
        'adjacent_repeat_rate': adjacent_repeat_rate,
        'most_adjacent_repeated': most_adjacent_repeated,
        'avg_adjacent_repeat_count': avg_adjacent_repeat_count,
        'interval_stats': interval_stats,
        'periodic_patterns': periodic_patterns,
        'repeated_combinations': repeated_combinations
    }
    
    conn.close()
    return results 

def analyze_consecutive_numbers(lottery_type, periods=50):
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    
    # 根據彩券類型設定參數
    if lottery_type == 'big-lotto':
        table = 'big_lotto'
        num_columns = 6
        max_number = 49
    elif lottery_type == 'super-lotto':
        table = 'super_lotto'
        num_columns = 6
        max_number = 38
    else:  # daily-cash
        table = 'daily_cash'
        num_columns = 5
        max_number = 39
    
    # 獲取最近N期的開獎號碼
    columns = [f'num{i}' for i in range(1, num_columns + 1)]
    column_str = ', '.join(columns)
    
    cursor.execute(f'''
        SELECT {column_str}
        FROM {table}
        ORDER BY draw_term DESC
        LIMIT {periods}
    ''')
    draws = cursor.fetchall()
    
    # 分析連號出現頻率
    consecutive_count = 0
    total_consecutive_count = 0
    max_consecutive_count = 0
    
    # 分析連號組合
    consecutive_combinations = {}
    
    for draw in draws:
        numbers = sorted(draw)
        current_consecutive = 1
        current_combination = []
        
        # 檢查每組號碼中的連號
        for i in range(len(numbers) - 1):
            if numbers[i] + 1 == numbers[i + 1]:
                current_consecutive += 1
                if len(current_combination) == 0:
                    current_combination.append(numbers[i])
                current_combination.append(numbers[i + 1])
            else:
                if current_consecutive > 1:
                    combo_key = tuple(current_combination)
                    consecutive_combinations[combo_key] = consecutive_combinations.get(combo_key, 0) + 1
                    consecutive_count += 1
                    total_consecutive_count += current_consecutive
                    max_consecutive_count = max(max_consecutive_count, current_consecutive)
                current_consecutive = 1
                current_combination = []
        
        # 處理最後一組連號
        if current_consecutive > 1:
            combo_key = tuple(current_combination)
            consecutive_combinations[combo_key] = consecutive_combinations.get(combo_key, 0) + 1
            consecutive_count += 1
            total_consecutive_count += current_consecutive
            max_consecutive_count = max(max_consecutive_count, current_consecutive)
    
    # 計算連號出現率和平均連號個數
    consecutive_rate = round(consecutive_count / len(draws) * 100, 2)
    avg_consecutive_count = round(total_consecutive_count / len(draws), 2) if consecutive_count > 0 else 0
    
    # 整理連號組合分析結果
    consecutive_combinations = [
        {
            'numbers': list(combo),
            'count': count,
            'rate': round(count / len(draws) * 100, 2)
        }
        for combo, count in sorted(
            consecutive_combinations.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
    ]
    
    # 分析連號間隔
    interval_stats = []
    last_consecutive_draw = None
    intervals = []
    
    for i, draw in enumerate(draws):
        numbers = sorted(draw)
        has_consecutive = False
        for j in range(len(numbers) - 1):
            if numbers[j] + 1 == numbers[j + 1]:
                has_consecutive = True
                break
        
        if has_consecutive:
            if last_consecutive_draw is not None:
                intervals.append(i - last_consecutive_draw)
            last_consecutive_draw = i
    
    if intervals:
        from collections import Counter
        interval_counts = Counter(intervals)
        total_intervals = len(intervals)
        interval_stats = [
            {
                'count': count,
                'rate': round(count / total_intervals * 100, 2),
                'common_numbers': [str(interval)]
            }
            for interval, count in sorted(
                interval_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )[:5]
        ]
    
    # 分析熱門連號模式
    popular_patterns = []
    pattern_last_seen = {}
    
    for i, draw in enumerate(draws):
        numbers = sorted(draw)
        for j in range(len(numbers) - 1):
            if numbers[j] + 1 == numbers[j + 1]:
                pattern = (numbers[j], numbers[j + 1])
                if pattern not in pattern_last_seen:
                    pattern_last_seen[pattern] = {
                        'numbers': list(pattern),
                        'count': 1,
                        'last_seen': i
                    }
                else:
                    pattern_last_seen[pattern]['count'] += 1
                    pattern_last_seen[pattern]['last_seen'] = i
    
    popular_patterns = sorted(
        [
            {
                'numbers': pattern['numbers'],
                'count': pattern['count'],
                'last_seen': pattern['last_seen']
            }
            for pattern in pattern_last_seen.values()
        ],
        key=lambda x: x['count'],
        reverse=True
    )[:5]
    
    results = {
        'consecutive_rate': consecutive_rate,
        'avg_consecutive_count': avg_consecutive_count,
        'max_consecutive_count': max_consecutive_count,
        'consecutive_combinations': consecutive_combinations,
        'interval_stats': interval_stats,
        'popular_patterns': popular_patterns
    }
    
    conn.close()
    return results 

def analyze_numeric_numbers(lottery_type, periods=50):
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    
    # 根據彩券類型設定參數
    if lottery_type == 'big-lotto':
        table = 'big_lotto'
        num_columns = 6
        max_number = 49
    elif lottery_type == 'super-lotto':
        table = 'super_lotto'
        num_columns = 6
        max_number = 38
    else:  # daily-cash
        table = 'daily_cash'
        num_columns = 5
        max_number = 39
    
    # 獲取最近N期的開獎號碼
    columns = [f'num{i}' for i in range(1, num_columns + 1)]
    column_str = ', '.join(columns)
    
    cursor.execute(f'''
        SELECT {column_str}
        FROM {table}
        ORDER BY draw_term DESC
        LIMIT {periods}
    ''')
    draws = cursor.fetchall()
    
    # 定義質數、平方數和斐波那契數列
    def is_prime(n):
        if n < 2:
            return False
        for i in range(2, int(n ** 0.5) + 1):
            if n % i == 0:
                return False
        return True
    
    primes = [n for n in range(1, max_number + 1) if is_prime(n)]
    squares = [n * n for n in range(1, int(max_number ** 0.5) + 1)]
    
    # 生成斐波那契數列
    fibonacci = [1, 1]
    while fibonacci[-1] < max_number:
        fibonacci.append(fibonacci[-1] + fibonacci[-2])
    fibonacci = [n for n in fibonacci if n <= max_number]
    
    # 統計各類數字的出現次數
    prime_count = 0
    square_count = 0
    fibonacci_count = 0
    number_counts = {}
    sum_values = []
    
    for draw in draws:
        draw_sum = sum(draw)
        sum_values.append(draw_sum)
        
        for num in draw:
            number_counts[num] = number_counts.get(num, 0) + 1
            if num in primes:
                prime_count += 1
            if num in squares:
                square_count += 1
            if num in fibonacci:
                fibonacci_count += 1
    
    total_numbers = len(draws) * num_columns
    
    # 找出熱門和冷門的特殊數字
    def get_popular_numbers(numbers):
        counts = [(n, number_counts.get(n, 0)) for n in numbers]
        sorted_counts = sorted(counts, key=lambda x: x[1], reverse=True)
        return [n for n, _ in sorted_counts[:3]], [n for n, _ in sorted_counts[-3:]]
    
    popular_primes, cold_primes = get_popular_numbers(primes)
    popular_squares, _ = get_popular_numbers(squares)
    popular_fibonacci, _ = get_popular_numbers(fibonacci)
    
    # 計算數字和的統計
    average_sum = round(sum(sum_values) / len(sum_values), 2)
    sum_counts = {}
    for s in sum_values:
        sum_counts[s] = sum_counts.get(s, 0) + 1
    most_common_sum = max(sum_counts.items(), key=lambda x: x[1])[0]
    
    results = {
        'prime_rate': round(prime_count / total_numbers * 100, 2),
        'popular_primes': popular_primes,
        'cold_primes': cold_primes,
        'square_rate': round(square_count / total_numbers * 100, 2),
        'popular_squares': popular_squares,
        'all_squares': squares,
        'fibonacci_rate': round(fibonacci_count / total_numbers * 100, 2),
        'popular_fibonacci': popular_fibonacci,
        'all_fibonacci': fibonacci,
        'average_sum': average_sum,
        'most_common_sum': most_common_sum,
        'sum_range': {
            'min': min(sum_values),
            'max': max(sum_values)
        }
    }
    
    conn.close()
    return results 

def analyze_distribution_numbers(lottery_type, periods=50):
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    
    # 根據彩券類型設定參數
    if lottery_type == 'big-lotto':
        table = 'big_lotto'
        num_columns = 6
        max_number = 49
    elif lottery_type == 'super-lotto':
        table = 'super_lotto'
        num_columns = 6
        max_number = 38
    else:  # daily-cash
        table = 'daily_cash'
        num_columns = 5
        max_number = 39
    
    # 獲取最近N期的開獎號碼
    columns = [f'num{i}' for i in range(1, num_columns + 1)]
    column_str = ', '.join(columns)
    
    cursor.execute(f'''
        SELECT {column_str}
        FROM {table}
        ORDER BY draw_term DESC
        LIMIT {periods}
    ''')
    draws = cursor.fetchall()
    
    # 區間分布分析
    range_size = 10
    range_distribution = {}
    number_counts = {}
    
    # 初始化區間
    for i in range(0, max_number, range_size):
        range_start = i + 1
        range_end = min(i + range_size, max_number)
        range_key = f'{range_start}-{range_end}'
        range_distribution[range_key] = {'count': 0, 'numbers': set()}
    
    # 統計每個號碼出現次數
    for draw in draws:
        for num in draw:
            number_counts[num] = number_counts.get(num, 0) + 1
            range_start = ((num - 1) // range_size) * range_size + 1
            range_end = min(range_start + range_size - 1, max_number)
            range_key = f'{range_start}-{range_end}'
            range_distribution[range_key]['count'] += 1
            range_distribution[range_key]['numbers'].add(num)
    
    # 計算每個區間的統計資料
    total_numbers = len(draws) * num_columns
    for range_key, stats in range_distribution.items():
        numbers_in_range = list(stats['numbers'])
        hot_numbers = sorted(numbers_in_range, 
                           key=lambda x: number_counts.get(x, 0), 
                           reverse=True)[:3]
        stats['rate'] = round(stats['count'] / total_numbers * 100, 2)
        stats['hot_numbers'] = hot_numbers
        stats['numbers'] = list(stats['numbers'])
    
    # 位置分布分析
    position_distribution = []
    for pos in range(num_columns):
        position_numbers = [draw[pos] for draw in draws]
        position_stats = {
            'average': round(sum(position_numbers) / len(position_numbers), 2),
            'range': {'min': min(position_numbers), 'max': max(position_numbers)},
            'most_common': sorted(
                [(n, position_numbers.count(n)) for n in set(position_numbers)],
                key=lambda x: x[1],
                reverse=True
            )[:3]
        }
        position_stats['most_common'] = [n for n, _ in position_stats['most_common']]
        position_distribution.append(position_stats)
    
    # 熱區分析
    hot_threshold = total_numbers / (max_number / 3)  # 將號碼範圍分成三等分
    hot_zones = []
    for range_key, stats in range_distribution.items():
        if stats['count'] >= hot_threshold:
            hot_zones.append({
                'range': range_key,
                'rate': stats['rate'],
                'numbers': sorted(stats['hot_numbers'])
            })
    hot_zones.sort(key=lambda x: x['rate'], reverse=True)
    
    # 修改冷區分析部分
    cold_zones = []
    
    # 計算每個區間的平均出現次數
    total_appearances = sum(stats['count'] for stats in range_distribution.values())
    average_appearances = total_appearances / len(range_distribution)
    
    # 將每個區間按出現率排序，選擇出現率最低的區間
    sorted_ranges = sorted(
        range_distribution.items(),
        key=lambda x: x[1]['count']
    )
    
    # 選擇前3個最冷門的區間
    for range_key, stats in sorted_ranges[:3]:
        # 找出該區間內出現次數最少的號碼
        numbers_in_range = list(stats['numbers'])
        cold_numbers = sorted(
            numbers_in_range,
            key=lambda x: number_counts.get(x, 0)
        )[:3]
        
        cold_zones.append({
            'range': range_key,
            'rate': stats['rate'],
            'numbers': cold_numbers
        })
    
    results = {
        'range_distribution': range_distribution,
        'position_distribution': position_distribution,
        'hot_zones': hot_zones[:3],
        'cold_zones': cold_zones  # 修改後的冷區分析結果
    }
    
    conn.close()
    return results 