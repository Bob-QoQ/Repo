from flask import Flask, render_template, request, jsonify
from lottery_analysis import LotteryAnalysis
import sqlite3

app = Flask(__name__)
analysis = LotteryAnalysis()

def get_db_connection():
    """獲取資料庫連接"""
    return sqlite3.connect('lottery.db')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/analyze', methods=['POST'])
def analyze():
    data = request.get_json()
    lottery_type = data.get('lottery_type', 'big_lotto')
    analysis_type = data.get('analysis_type', 'basic')
    periods = data.get('periods')
    
    if periods:
        try:
            periods = int(periods)
            if periods <= 0:
                return jsonify({'error': '期數必須大於 0'})
        except ValueError:
            return jsonify({'error': '請輸入有效的期數'})
    
    try:
        if analysis_type == 'basic':
            result = analysis.basic_statistics(lottery_type, periods)
        elif analysis_type == 'interval':
            result = analysis.interval_analysis(lottery_type, periods)
        elif analysis_type == 'continuity':
            result = analysis.continuity_analysis(lottery_type, periods)
        elif analysis_type == 'time_series':
            result = analysis.time_series_analysis(lottery_type, periods)
        elif analysis_type == 'prediction':
            result = analysis.prediction_analysis(lottery_type, periods)
        elif analysis_type == 'recommend':
            result = analysis.recommend_numbers(lottery_type, periods)
        elif analysis_type == 'combination':
            result = analysis.combination_pattern_analysis(lottery_type, periods)
        elif analysis_type == 'combination_prediction':
            result = analysis.combination_prediction(lottery_type, periods)
        elif analysis_type == 'advanced_statistics':
            result = analysis.advanced_statistics(lottery_type, periods)
        elif analysis_type == 'missing_value':
            result = analysis.missing_value_analysis(lottery_type, periods)
        elif analysis_type == 'network':
            result = analysis.network_analysis(lottery_type, periods)
        elif analysis_type == 'probability':
            result = analysis.probability_distribution_analysis(lottery_type, periods)
        else:
            return jsonify({'error': '無效的分析類型'})
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/data_range')
def get_data_range():
    """獲取所有樂透類型的資料範圍"""
    lottery_types = ['big_lotto', 'super_lotto', 'daily_cash']
    lottery_names = {
        'big_lotto': '大樂透',
        'super_lotto': '威力彩',
        'daily_cash': '今彩539'
    }
    
    ranges = {}
    for lottery_type in lottery_types:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 獲取每種樂透的資料範圍
        cursor.execute(f"""
            SELECT 
                MIN(draw_date) as start_date,
                MAX(draw_date) as end_date,
                MAX(draw_term) as latest_term,
                COUNT(DISTINCT draw_term) as total_draws
            FROM {lottery_type}
        """)
        result = cursor.fetchone()
        
        ranges[lottery_type] = {
            'name': lottery_names[lottery_type],
            'start_date': result[0],
            'end_date': result[1],
            'latest_term': result[2],
            'total_draws': result[3]
        }
        
        conn.close()
    
    return jsonify(ranges)

@app.route('/recommend', methods=['POST'])
def recommend():
    try:
        data = request.get_json()
        lottery_type = data.get('lottery_type', 'big_lotto')
        periods = data.get('periods')
        method = data.get('method', 'hot_cold')
        
        analyzer = LotteryAnalysis()
        result = analyzer.recommend_by_method(lottery_type, method, periods)
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/recommend_from_hot_cold', methods=['POST'])
def recommend_from_hot_cold():
    data = request.get_json()
    lottery_type = data.get('lottery_type', 'big_lotto')
    periods = data.get('periods')
    
    try:
        result = analysis.recommend_from_hot_cold(lottery_type, periods)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True) 