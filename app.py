from flask import Flask, render_template, request, jsonify
from lottery_analysis import LotteryAnalysis

app = Flask(__name__)
analysis = LotteryAnalysis()

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
        else:
            return jsonify({'error': '無效的分析類型'})
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/data_range')
def get_data_range():
    lottery_type = request.args.get('lottery_type', 'big_lotto')
    result = analysis.get_data_range(lottery_type)
    return jsonify(result)

@app.route('/recommend', methods=['POST'])
def recommend():
    data = request.get_json()
    lottery_type = data.get('lottery_type', 'big_lotto')
    periods = data.get('periods')
    
    if periods:
        try:
            periods = int(periods)
            if periods <= 0:
                return jsonify({'error': '期數必須大於 0'})
        except ValueError:
            return jsonify({'error': '請輸入有效的期數'})
    
    try:
        result = analysis.recommend_numbers(lottery_type, periods)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True) 