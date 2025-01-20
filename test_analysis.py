from lottery_analysis import LotteryAnalysis
import sys

def show_menu():
    print("\n=== 樂透統計分析系統 ===")
    print("1. 大樂透")
    print("2. 威力彩")
    print("3. 今彩539")
    print("0. 離開")
    return input("請選擇要分析的樂透類型 (0-3): ")

def show_analysis_menu():
    print("\n=== 分析類型 ===")
    print("1. 基本統計")
    print("2. 區間分析")
    print("3. 連續性分析")
    print("4. 時間序列分析")
    print("5. 預測分析")
    print("0. 返回")
    return input("請選擇分析類型 (0-5): ")

def get_period_input():
    while True:
        try:
            periods = input("\n請輸入要分析的期數 (直接按 Enter 分析所有資料): ")
            if not periods:
                return None
            periods = int(periods)
            if periods <= 0:
                print("期數必須大於 0")
                continue
            return periods
        except ValueError:
            print("請輸入有效的數字")

def show_statistics(lottery_type, periods=None):
    analysis = LotteryAnalysis()
    
    # 取得資料範圍
    date_range = analysis.get_data_range(lottery_type)
    print(f"\n資料範圍：")
    print(f"從 {date_range['start_date']} 到 {date_range['end_date']}")
    print(f"共 {date_range['total_draws']} 期")
    
    if periods:
        print(f"\n分析最近 {periods} 期的資料")
    else:
        print("\n分析所有歷史資料")
    
    # 取得統計資料
    stats = analysis.basic_statistics(lottery_type, periods)
    
    # 1. 印出前10個最常出現的號碼
    print("\n最常出現的號碼：")
    for num in stats['number_frequency'][:10]:
        print(f"號碼 {num['number']}: 出現 {num['frequency']} 次 ({num['percentage']}%)")
    
    # 2. 印出特別號統計（如果有的話）
    if stats['special_number_frequency']:
        print("\n最常出現的特別號：")
        for num in stats['special_number_frequency'][:10]:
            print(f"號碼 {num['number']}: 出現 {num['frequency']} 次 ({num['percentage']}%)")
    
    # 3. 印出最常出現的組合
    print("\n最常出現的組合：")
    for combo in stats['common_combinations'][:3]:
        num_count = 5 if lottery_type == 'daily_cash' else 6
        numbers = []
        for i in range(1, num_count + 1):
            numbers.append(str(combo[f'num{i}']))
        print(f"組合 {', '.join(numbers)}: 出現 {combo['frequency']} 次")
    
    # 4. 印出最久沒出現的號碼
    print("\n最久沒出現的號碼：")
    for num in stats['cold_numbers'][:5]:
        if num['last_date'] == '從未出現':
            print(f"號碼 {num['number']}: 從未出現")
        else:
            print(f"號碼 {num['number']}: 最後出現於 {num['last_date']} ({num['days_since_last']} 天前)")

def show_interval_analysis(lottery_type, periods=None):
    analysis = LotteryAnalysis()
    
    print(f"\n開始進行區間分析...")
    result = analysis.interval_analysis(lottery_type, periods)
    
    # 1. 顯示分區統計
    print("\n=== 號碼分區統計 ===")
    for section in result['section_stats']:
        print(f"區間 {section['section_start']}-{section['section_end']}: "
              f"出現 {section['frequency']} 次 ({section['percentage']}%)")
    
    # 2. 顯示大小號碼比例
    print("\n=== 大小號碼比例 ===")
    for ratio in result['size_ratio']:
        big_small = ratio['ratio'].split(':')
        print(f"大號{big_small[0]}個:小號{big_small[1]}個 "
              f"出現 {ratio['frequency']} 次 ({ratio['percentage']}%)")
    
    # 3. 顯示奇偶數比例
    print("\n=== 奇偶數比例 ===")
    for ratio in result['odd_even_ratio']:
        odd_even = ratio['ratio'].split(':')
        print(f"奇數{odd_even[0]}個:偶數{odd_even[1]}個 "
              f"出現 {ratio['frequency']} 次 ({ratio['percentage']}%)")

def show_continuity_analysis(lottery_type, periods=None):
    analysis = LotteryAnalysis()
    
    print(f"\n開始進行連續性分析...")
    result = analysis.continuity_analysis(lottery_type, periods)
    
    # 1. 顯示連續號碼統計
    print("\n=== 連續號碼統計 ===")
    for stat in result['consecutive_stats']:
        print(f"{stat['consecutive_count']} 個連續號碼: "
              f"出現 {stat['frequency']} 次 ({stat['percentage']}%)")
    
    # 2. 顯示號碼間隔統計
    print("\n=== 號碼間隔統計 (最常見的間隔) ===")
    for stat in result['gap_stats']:
        print(f"間隔 {stat['gap']}: "
              f"出現 {stat['frequency']} 次 ({stat['percentage']}%)")
    
    # 3. 顯示號碼間隔分析
    print("\n=== 個別號碼間隔分析 (間隔天數最長的號碼) ===")
    for stat in result['number_gaps']:
        print(f"號碼 {stat['number']}: "
              f"平均間隔 {stat['avg_gap']} 天, "
              f"最短 {stat['min_gap']} 天, "
              f"最長 {stat['max_gap']} 天")

def show_time_series_analysis(lottery_type, periods=None):
    analysis = LotteryAnalysis()
    
    print(f"\n開始進行時間序列分析...")
    result = analysis.time_series_analysis(lottery_type, periods)
    
    # 1. 顯示年度分析
    print("\n=== 年度分析（最近10年） ===")
    for stat in result['yearly_stats']:
        print(f"{stat['year']}年:")
        print(f"  期數: {stat['draw_count']} 期")
        print(f"  總開出號碼數: {stat['total_numbers']}")
        print(f"  熱門號碼: {stat['hot_numbers']}")
    
    # 2. 顯示月份分析
    print("\n=== 月份分析 ===")
    for stat in result['monthly_stats']:
        print(f"{stat['month']}月:")
        print(f"  期數: {stat['draw_count']} 期")
        print(f"  總開出號碼數: {stat['total_numbers']}")
        print(f"  常見號碼: {stat['common_numbers']}")
    
    # 3. 顯示星期分析
    print("\n=== 星期分析 ===")
    weekdays = ['日', '一', '二', '三', '四', '五', '六']
    for stat in result['weekday_stats']:
        print(f"星期{weekdays[stat['weekday']]}:")
        print(f"  期數: {stat['draw_count']} 期")
        print(f"  總開出號碼數: {stat['total_numbers']}")
        print(f"  常見號碼: {stat['common_numbers']}")
    
    # 4. 顯示季節分析
    print("\n=== 季節分析 ===")
    for stat in result['seasonal_stats']:
        print(f"{stat['season']}:")
        print(f"  期數: {stat['draw_count']} 期")
        print(f"  總開出號碼數: {stat['total_numbers']}")
        print(f"  常見號碼: {stat['common_numbers']}")

def show_prediction_analysis(lottery_type, periods=None):
    analysis = LotteryAnalysis()
    
    print(f"\n開始進行預測分析...")
    result = analysis.prediction_analysis(lottery_type, periods)
    
    # 1. 顯示熱門號碼
    print("\n=== 近期熱門號碼 ===")
    for num in result['hot_numbers']:
        print(f"號碼 {num['number']}: "
              f"出現 {num['frequency']} 次 ({num['percentage']}%)")
    
    # 2. 顯示冷門號碼
    print("\n=== 近期冷門號碼 ===")
    for num in result['cold_numbers']:
        print(f"號碼 {num['number']}: "
              f"最後出現於 {num['last_date']} ({num['days_since_last']} 天前), "
              f"總出現 {num['total_appearances']} 次")
    
    # 3. 顯示號碼趨勢
    print("\n=== 號碼趨勢分析 ===")
    for trend in result['number_trends']:
        print(f"號碼 {trend['number']}: "
              f"趨勢 {trend['trend']}, "
              f"平均出現 {trend['avg_frequency']:.1f} 次, "
              f"範圍 {trend['min_frequency']}-{trend['max_frequency']} 次")
    
    # 4. 顯示號碼模式
    print("\n=== 常見號碼模式 ===")
    for pattern in result['pattern_numbers']:
        print(f"模式: {pattern['size_pattern']}, {pattern['odd_even_pattern']}")
        print(f"區間分布: {pattern['section_pattern']}")
        print(f"出現 {pattern['frequency']} 次 ({pattern['percentage']}%)")

def main():
    while True:
        choice = show_menu()
        
        if choice == '0':
            print("\n謝謝使用，再見！")
            sys.exit(0)
        
        lottery_types = {
            '1': 'big_lotto',
            '2': 'super_lotto',
            '3': 'daily_cash'
        }
        
        if choice not in lottery_types:
            print("\n請輸入有效的選項！")
            continue
        
        while True:
            analysis_choice = show_analysis_menu()
            
            if analysis_choice == '0':
                break
            
            periods = get_period_input()
            
            if analysis_choice == '1':
                show_statistics(lottery_types[choice], periods)
            elif analysis_choice == '2':
                show_interval_analysis(lottery_types[choice], periods)
            elif analysis_choice == '3':
                show_continuity_analysis(lottery_types[choice], periods)
            elif analysis_choice == '4':
                show_time_series_analysis(lottery_types[choice], periods)
            elif analysis_choice == '5':
                show_prediction_analysis(lottery_types[choice], periods)
            else:
                print("\n請輸入有效的選項！")
                continue
            
            input("\n按 Enter 繼續...")

if __name__ == '__main__':
    main() 