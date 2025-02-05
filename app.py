from flask import Flask, render_template, request, jsonify
import sqlite3
from datetime import datetime
from lottery_analysis import analyze_lottery, analyze_repeat_numbers, analyze_special_numbers, analyze_combination_numbers, analyze_prediction_numbers, analyze_route_numbers, analyze_repetition_numbers, analyze_consecutive_numbers, analyze_numeric_numbers, analyze_distribution_numbers

app = Flask(__name__)

def get_data_range():
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    
    ranges = {}
    
    # 獲取大樂透資料範圍
    cursor.execute('''
        SELECT MIN(draw_date), MAX(draw_date), 
               MIN(draw_term), MAX(draw_term),
               COUNT(draw_term)
        FROM big_lotto
    ''')
    min_date, max_date, min_term, max_term, total_count = cursor.fetchone()
    ranges['big_lotto'] = {
        'start': min_date, 
        'end': max_date,
        'start_term': min_term,
        'end_term': max_term,
        'total': total_count
    }
    
    # 獲取威力彩資料範圍
    cursor.execute('''
        SELECT MIN(draw_date), MAX(draw_date),
               MIN(draw_term), MAX(draw_term),
               COUNT(draw_term)
        FROM super_lotto
    ''')
    min_date, max_date, min_term, max_term, total_count = cursor.fetchone()
    ranges['super_lotto'] = {
        'start': min_date, 
        'end': max_date,
        'start_term': min_term,
        'end_term': max_term,
        'total': total_count
    }
    
    # 獲取今彩539資料範圍
    cursor.execute('''
        SELECT MIN(draw_date), MAX(draw_date),
               MIN(draw_term), MAX(draw_term),
               COUNT(draw_term)
        FROM daily_cash
    ''')
    min_date, max_date, min_term, max_term, total_count = cursor.fetchone()
    ranges['daily_cash'] = {
        'start': min_date, 
        'end': max_date,
        'start_term': min_term,
        'end_term': max_term,
        'total': total_count
    }
    
    conn.close()
    return ranges

def get_latest_draws():
    conn = sqlite3.connect('lottery.db')
    cursor = conn.cursor()
    
    # 獲取大樂透最新三期
    cursor.execute('''
        SELECT draw_term, draw_date, num1, num2, num3, num4, num5, num6, special_num 
        FROM big_lotto 
        ORDER BY draw_term DESC 
        LIMIT 3
    ''')
    big_lotto = cursor.fetchall()
    
    # 獲取威力彩最新三期
    cursor.execute('''
        SELECT draw_term, draw_date, num1, num2, num3, num4, num5, num6, special_num 
        FROM super_lotto 
        ORDER BY draw_term DESC 
        LIMIT 3
    ''')
    super_lotto = cursor.fetchall()
    
    # 獲取今彩539最新三期
    cursor.execute('''
        SELECT draw_term, draw_date, num1, num2, num3, num4, num5 
        FROM daily_cash 
        ORDER BY draw_term DESC 
        LIMIT 3
    ''')
    daily_cash = cursor.fetchall()
    
    conn.close()

    # 對每一期的號碼進行排序
    sorted_draws = {
        'big_lotto': [],
        'super_lotto': [],
        'daily_cash': []
    }

    # 處理大樂透資料
    for draw in big_lotto:
        draw_term, draw_date = draw[0], draw[1]
        numbers = sorted(draw[2:8])  # 排序前6個號碼
        special_num = draw[8]        # 特別號不參與排序
        sorted_draws['big_lotto'].append((draw_term, draw_date) + tuple(numbers) + (special_num,))

    # 處理威力彩資料
    for draw in super_lotto:
        draw_term, draw_date = draw[0], draw[1]
        numbers = sorted(draw[2:8])  # 排序前6個號碼
        special_num = draw[8]        # 特別號不參與排序
        sorted_draws['super_lotto'].append((draw_term, draw_date) + tuple(numbers) + (special_num,))

    # 處理今彩539資料
    for draw in daily_cash:
        draw_term, draw_date = draw[0], draw[1]
        numbers = sorted(draw[2:7])  # 排序5個號碼
        sorted_draws['daily_cash'].append((draw_term, draw_date) + tuple(numbers))

    return sorted_draws

@app.route('/')
def index():
    latest_draws = get_latest_draws()
    data_ranges = get_data_range()
    return render_template('index.html', draws=latest_draws, ranges=data_ranges)

@app.route('/api/analyze/<lottery_type>')
def analyze(lottery_type):
    try:
        periods = request.args.get('periods', default=50, type=int)
        
        # 檢查資料庫中實際的期數
        conn = sqlite3.connect('lottery.db')
        cursor = conn.cursor()
        
        table_map = {
            'big-lotto': 'big_lotto',
            'super-lotto': 'super_lotto',
            'daily-cash': 'daily_cash'
        }
        
        cursor.execute(f'SELECT COUNT(*) FROM {table_map[lottery_type]}')
        max_periods = cursor.fetchone()[0]
        conn.close()
        
        # 如果請求的期數超過實際期數，則使用實際最大期數
        periods = min(periods, max_periods)
        
        results = analyze_lottery(lottery_type, periods)
        return jsonify(results)
    except Exception as e:
        print(f"Error in analyze: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analyze/repeat/<lottery_type>')
def analyze_repeat(lottery_type):
    try:
        periods = request.args.get('periods', default=50, type=int)
        
        # 檢查資料庫中實際的期數
        conn = sqlite3.connect('lottery.db')
        cursor = conn.cursor()
        table_map = {
            'big-lotto': 'big_lotto',
            'super-lotto': 'super_lotto',
            'daily-cash': 'daily_cash'
        }
        cursor.execute(f'SELECT COUNT(*) FROM {table_map[lottery_type]}')
        max_periods = cursor.fetchone()[0]
        conn.close()
        
        # 如果請求的期數超過實際期數，則使用實際最大期數
        periods = min(periods, max_periods)
        
        results = analyze_repeat_numbers(lottery_type, periods)
        return jsonify(results)
    except Exception as e:
        print(f"Error in analyze_repeat: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analyze/special/<lottery_type>')
def analyze_special(lottery_type):
    try:
        periods = request.args.get('periods', default=50, type=int)
        
        # 檢查資料庫中實際的期數
        conn = sqlite3.connect('lottery.db')
        cursor = conn.cursor()
        table_map = {
            'big-lotto': 'big_lotto',
            'super-lotto': 'super_lotto',
            'daily-cash': 'daily_cash'
        }
        cursor.execute(f'SELECT COUNT(*) FROM {table_map[lottery_type]}')
        max_periods = cursor.fetchone()[0]
        conn.close()
        
        # 如果請求的期數超過實際期數，則使用實際最大期數
        periods = min(periods, max_periods)
        
        results = analyze_special_numbers(lottery_type, periods)
        return jsonify(results)
    except Exception as e:
        print(f"Error in analyze_special: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analyze/combination/<lottery_type>')
def analyze_combination(lottery_type):
    try:
        periods = request.args.get('periods', default=50, type=int)
        
        # 檢查資料庫中實際的期數
        conn = sqlite3.connect('lottery.db')
        cursor = conn.cursor()
        table_map = {
            'big-lotto': 'big_lotto',
            'super-lotto': 'super_lotto',
            'daily-cash': 'daily_cash'
        }
        cursor.execute(f'SELECT COUNT(*) FROM {table_map[lottery_type]}')
        max_periods = cursor.fetchone()[0]
        conn.close()
        
        # 如果請求的期數超過實際期數，則使用實際最大期數
        periods = min(periods, max_periods)
        
        results = analyze_combination_numbers(lottery_type, periods)
        return jsonify(results)
    except Exception as e:
        print(f"Error in analyze_combination: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analyze/prediction/<lottery_type>')
def analyze_prediction(lottery_type):
    try:
        periods = request.args.get('periods', default=50, type=int)
        
        # 檢查資料庫中實際的期數
        conn = sqlite3.connect('lottery.db')
        cursor = conn.cursor()
        table_map = {
            'big-lotto': 'big_lotto',
            'super-lotto': 'super_lotto',
            'daily-cash': 'daily_cash'
        }
        cursor.execute(f'SELECT COUNT(*) FROM {table_map[lottery_type]}')
        max_periods = cursor.fetchone()[0]
        conn.close()
        
        # 如果請求的期數超過實際期數，則使用實際最大期數
        periods = min(periods, max_periods)
        
        results = analyze_prediction_numbers(lottery_type, periods)
        return jsonify(results)
    except Exception as e:
        print(f"Error in analyze_prediction: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analyze/route/<lottery_type>')
def analyze_route(lottery_type):
    try:
        periods = request.args.get('periods', default=50, type=int)
        
        # 檢查資料庫中實際的期數
        conn = sqlite3.connect('lottery.db')
        cursor = conn.cursor()
        table_map = {
            'big-lotto': 'big_lotto',
            'super-lotto': 'super_lotto',
            'daily-cash': 'daily_cash'
        }
        cursor.execute(f'SELECT COUNT(*) FROM {table_map[lottery_type]}')
        max_periods = cursor.fetchone()[0]
        conn.close()
        
        # 如果請求的期數超過實際期數，則使用實際最大期數
        periods = min(periods, max_periods)
        
        results = analyze_route_numbers(lottery_type, periods)
        return jsonify(results)
    except Exception as e:
        print(f"Error in analyze_route: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analyze/repetition/<lottery_type>')
def analyze_repetition(lottery_type):
    try:
        periods = request.args.get('periods', default=50, type=int)
        
        # 檢查資料庫中實際的期數
        conn = sqlite3.connect('lottery.db')
        cursor = conn.cursor()
        table_map = {
            'big-lotto': 'big_lotto',
            'super-lotto': 'super_lotto',
            'daily-cash': 'daily_cash'
        }
        cursor.execute(f'SELECT COUNT(*) FROM {table_map[lottery_type]}')
        max_periods = cursor.fetchone()[0]
        conn.close()
        
        # 如果請求的期數超過實際期數，則使用實際最大期數
        periods = min(periods, max_periods)
        
        results = analyze_repetition_numbers(lottery_type, periods)
        return jsonify(results)
    except Exception as e:
        print(f"Error in analyze_repetition: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analyze/consecutive/<lottery_type>')
def analyze_consecutive(lottery_type):
    try:
        periods = request.args.get('periods', default=50, type=int)
        
        # 檢查資料庫中實際的期數
        conn = sqlite3.connect('lottery.db')
        cursor = conn.cursor()
        table_map = {
            'big-lotto': 'big_lotto',
            'super-lotto': 'super_lotto',
            'daily-cash': 'daily_cash'
        }
        cursor.execute(f'SELECT COUNT(*) FROM {table_map[lottery_type]}')
        max_periods = cursor.fetchone()[0]
        conn.close()
        
        # 如果請求的期數超過實際期數，則使用實際最大期數
        periods = min(periods, max_periods)
        
        results = analyze_consecutive_numbers(lottery_type, periods)
        return jsonify(results)
    except Exception as e:
        print(f"Error in analyze_consecutive: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analyze/numeric/<lottery_type>')
def analyze_numeric(lottery_type):
    try:
        periods = request.args.get('periods', default=50, type=int)
        
        # 檢查資料庫中實際的期數
        conn = sqlite3.connect('lottery.db')
        cursor = conn.cursor()
        table_map = {
            'big-lotto': 'big_lotto',
            'super-lotto': 'super_lotto',
            'daily-cash': 'daily_cash'
        }
        cursor.execute(f'SELECT COUNT(*) FROM {table_map[lottery_type]}')
        max_periods = cursor.fetchone()[0]
        conn.close()
        
        # 如果請求的期數超過實際期數，則使用實際最大期數
        periods = min(periods, max_periods)
        
        results = analyze_numeric_numbers(lottery_type, periods)
        return jsonify(results)
    except Exception as e:
        print(f"Error in analyze_numeric: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analyze/distribution/<lottery_type>')
def analyze_distribution(lottery_type):
    try:
        periods = request.args.get('periods', default=50, type=int)
        
        # 檢查資料庫中實際的期數
        conn = sqlite3.connect('lottery.db')
        cursor = conn.cursor()
        table_map = {
            'big-lotto': 'big_lotto',
            'super-lotto': 'super_lotto',
            'daily-cash': 'daily_cash'
        }
        cursor.execute(f'SELECT COUNT(*) FROM {table_map[lottery_type]}')
        max_periods = cursor.fetchone()[0]
        conn.close()
        
        # 如果請求的期數超過實際期數，則使用實際最大期數
        periods = min(periods, max_periods)
        
        results = analyze_distribution_numbers(lottery_type, periods)
        return jsonify(results)
    except Exception as e:
        print(f"Error in analyze_distribution: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True) 