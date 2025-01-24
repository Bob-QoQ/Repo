import sqlite3
from datetime import datetime
import pandas as pd
import numpy as np
import random

class LotteryAnalysis:
    def __init__(self, db_path='lottery.db'):
        self.db_path = db_path
        
    def _get_connection(self):
        """建立資料庫連線"""
        return sqlite3.connect(self.db_path)
    
    def basic_statistics(self, lottery_type='big_lotto', periods=None):
        """基本統計分析
        
        Args:
            lottery_type (str): 樂透類型 ('big_lotto', 'super_lotto', 'daily_cash')
            periods (int, optional): 要分析的期數，None 表示分析所有資料
            
        Returns:
            dict: 包含以下分析結果：
            - summary_stats: 基本統計摘要
            - hot_numbers: 熱門號碼分析
            - cold_numbers: 冷門號碼分析
            - number_trends: 號碼趨勢分析
        """
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 1. 基本統計摘要
        query_summary = f"""
        WITH NumberStats AS (
            SELECT 
                COUNT(DISTINCT draw_term) as total_draws,
                COUNT(DISTINCT value) as unique_numbers,
                MIN(value) as min_number,
                MAX(value) as max_number,
                (
                    SELECT GROUP_CONCAT(value)
                    FROM (
                        SELECT value, COUNT(*) as freq
                        FROM {lottery_type},
                        json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
                        WHERE 1=1 {period_filter}
                        GROUP BY value
                        ORDER BY freq DESC
                        LIMIT 3
                    )
                ) as most_common_numbers
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
        )
        SELECT 
            total_draws,
            unique_numbers,
            min_number,
            max_number,
            most_common_numbers
        FROM NumberStats
        """
        
        # 2. 熱門號碼分析（最近常開出的號碼）
        query_hot = f"""
        WITH RecentDraws AS (
            SELECT 
                value as number,
                COUNT(*) as frequency,
                MAX(draw_term) as last_appearance,
                COUNT(*) * 100.0 / (
                    SELECT COUNT(DISTINCT draw_term) 
                    FROM {lottery_type}
                    WHERE 1=1 {period_filter}
                ) as percentage,
                GROUP_CONCAT(draw_term) as recent_draws
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY value
            HAVING frequency >= 3
        )
        SELECT 
            number,
            frequency,
            ROUND(percentage, 2) as percentage,
            last_appearance,
            recent_draws
        FROM RecentDraws
        ORDER BY frequency DESC, last_appearance DESC
        LIMIT 20
        """
        
        # 3. 冷門號碼分析（最近很少開出的號碼）
        query_cold = f"""
        WITH NumberStats AS (
            SELECT 
                value as number,
                COUNT(*) as total_appearances,                    -- 總出現次數
                MAX(draw_term) as last_appearance,               -- 最後出現期數
                (
                    SELECT COUNT(DISTINCT draw_term)
                    FROM {lottery_type}
                    WHERE draw_term > MAX(t1.draw_term)
                    {period_filter}
                ) as draws_since_last,                          -- 最近遺漏期數
                COUNT(*) * 100.0 / (
                    SELECT COUNT(DISTINCT draw_term) 
                    FROM {lottery_type}
                    WHERE 1=1 {period_filter}
                ) as historical_percentage,                      -- 歷史出現率
                (
                    SELECT AVG(cnt) * 0.8                       -- 計算低頻門檻
                    FROM (
                        SELECT COUNT(*) as cnt
                        FROM {lottery_type},
                        json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
                        WHERE 1=1 {period_filter}
                        GROUP BY value
                    )
                ) as low_freq_threshold
            FROM {lottery_type} t1,
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY value
        )
        SELECT 
            number,
            total_appearances,
            last_appearance,
            draws_since_last,
            ROUND(historical_percentage, 2) as historical_percentage,
            CASE 
                WHEN total_appearances < low_freq_threshold THEN '低頻'
                WHEN draws_since_last > 10 THEN '遺漏'
                ELSE '正常'
            END as cold_type,
            low_freq_threshold as threshold
        FROM NumberStats
        WHERE total_appearances < low_freq_threshold OR draws_since_last > 10
        ORDER BY 
            cold_type,
            CASE cold_type
                WHEN '低頻' THEN total_appearances
                WHEN '遺漏' THEN draws_since_last
                ELSE 0
            END DESC
        """
        
        # 4. 號碼趨勢分析
        query_trends = f"""
        WITH PeriodStats AS (
            SELECT 
                value as number,
                COUNT(*) as recent_frequency,
                LAG(COUNT(*)) OVER (PARTITION BY value ORDER BY value) as previous_frequency
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY value
        )
        SELECT 
            number,
            recent_frequency,
            COALESCE(previous_frequency, 0) as previous_frequency,
            CASE 
                WHEN previous_frequency > 0 
                THEN ROUND((recent_frequency - previous_frequency) * 100.0 / previous_frequency, 2)
                ELSE 0 
            END as trend_percentage,
            CASE 
                WHEN recent_frequency > COALESCE(previous_frequency, 0) THEN '上升'
                WHEN recent_frequency < COALESCE(previous_frequency, 0) THEN '下降'
                ELSE '持平'
            END as trend_direction
        FROM PeriodStats
        ORDER BY ABS(trend_percentage) DESC
        """
        
        # 執行查詢
        summary_stats = pd.read_sql_query(query_summary, conn).to_dict('records')[0]
        hot_numbers = pd.read_sql_query(query_hot, conn).to_dict('records')
        cold_numbers = pd.read_sql_query(query_cold, conn).to_dict('records')
        number_trends = pd.read_sql_query(query_trends, conn).to_dict('records')
        
        # 分析說明
        analysis_description = {
            '熱門號碼分析': '''分析近期頻繁出現的號碼。
            包含：
            - 出現頻率
            - 出現率
            - 最近開出期數
            - 開出記錄
            這些號碼可能有較高的開出機率。''',
            
            '低頻號碼分析': '''統計歷史上出現次數較少的號碼。
            判定標準：
            - 出現次數低於平均值的80%
            
            特點：
            - 雖然出現頻率低
            - 但根據機率均衡原理
            - 可能即將開出''',
            
            '遺漏號碼分析': '''找出已經超過10期未開出的號碼。
            
            分析重點：
            - 最後開出期數
            - 遺漏期數統計
            - 歷史出現率
            
            理論基礎：
            根據機率理論，長期未開出的號碼
            可能會有回補的趨勢。''',
            
            '趨勢分析': '''比較號碼在不同時期的出現頻率變化。
            
            分析內容：
            - 近期開出頻率
            - 前期開出頻率
            - 變化幅度計算
            - 趨勢方向判定
            
            用途：
            幫助預測號碼未來可能的
            開出機率變化趨勢。'''
        }
        
        conn.close()
        
        return {
            'summary_stats': summary_stats,
            'hot_numbers': hot_numbers,
            'cold_numbers': cold_numbers,
            'number_trends': number_trends,
            'analysis_description': analysis_description
        }
    
    def _get_number_frequency(self, conn, lottery_type, period_filter=""):
        """計算每個號碼的出現頻率"""
        # 根據樂透類型決定是否需要包含 num6
        num6_query = f"""
            UNION ALL
            SELECT num6 FROM {lottery_type}
            WHERE 1=1 {period_filter}
        """ if lottery_type != 'daily_cash' else ""
        
        query = f"""
        SELECT 
            number,
            COUNT(*) as frequency,
            ROUND(COUNT(*) * 100.0 / (
                SELECT COUNT(*) * {6 if lottery_type != 'daily_cash' else 5} 
                FROM {lottery_type}
                WHERE 1=1 {period_filter}
            ), 2) as percentage
        FROM (
            SELECT num1 as number FROM {lottery_type} WHERE 1=1 {period_filter}
            UNION ALL
            SELECT num2 FROM {lottery_type} WHERE 1=1 {period_filter}
            UNION ALL
            SELECT num3 FROM {lottery_type} WHERE 1=1 {period_filter}
            UNION ALL
            SELECT num4 FROM {lottery_type} WHERE 1=1 {period_filter}
            UNION ALL
            SELECT num5 FROM {lottery_type} WHERE 1=1 {period_filter}
            {num6_query}
        )
        GROUP BY number
        ORDER BY frequency DESC
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def _get_special_number_frequency(self, conn, lottery_type, period_filter=""):
        """計算特別號的出現頻率"""
        if lottery_type == 'daily_cash':
            return None  # 今彩539沒有特別號
        
        query = f"""
        SELECT 
            special_num as number,
            COUNT(*) as frequency,
            ROUND(COUNT(*) * 100.0 / (
                SELECT COUNT(*) FROM {lottery_type}
                WHERE 1=1 {period_filter}
            ), 2) as percentage
        FROM {lottery_type}
        WHERE 1=1 {period_filter}
        GROUP BY special_num
        ORDER BY frequency DESC
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def _get_common_combinations(self, conn, lottery_type, period_filter=""):
        """找出最常出現的號碼組合"""
        if lottery_type == 'daily_cash':
            columns = 'num1, num2, num3, num4, num5'
            num_count = 5
        else:
            columns = 'num1, num2, num3, num4, num5, num6'
            num_count = 6
        
        query = f"""
        WITH SortedNumbers AS (
            SELECT 
                draw_term,
                {columns},
                (
                    SELECT GROUP_CONCAT(num, ',')
                    FROM (
                        SELECT CAST(value as INTEGER) as num
                        FROM json_each(json_array({columns}))
                        ORDER BY CAST(value as INTEGER)
                    )
                ) as sorted_numbers
            FROM {lottery_type}
            WHERE 1=1 {period_filter}
        )
        SELECT 
            {columns},
            COUNT(*) as frequency,
            sorted_numbers
        FROM SortedNumbers
        GROUP BY sorted_numbers
        HAVING frequency > 1
        ORDER BY frequency DESC
        LIMIT 10
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def _get_cold_numbers(self, conn, lottery_type, period_filter=""):
        """找出最久沒出現的號碼"""
        max_number = 49 if lottery_type != 'daily_cash' else 39
        
        # 根據樂透類型決定是否需要包含 num6
        num6_query = f"""
            UNION ALL 
            SELECT num6, draw_date FROM {lottery_type}
            WHERE 1=1 {period_filter}
        """ if lottery_type != 'daily_cash' else ""
        
        query = f"""
        WITH AllNumbers AS (
            SELECT number 
            FROM (
                SELECT ROW_NUMBER() OVER (ORDER BY NULL) as number
                FROM (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
                      SELECT 1)
            ) nums 
            WHERE number <= {max_number}
        ),
        LastAppearance AS (
            SELECT 
                number,
                MAX(draw_date) as last_date,
                MAX(
                    strftime('%Y-%m-%d',
                        (CAST(substr(draw_date, 1, 3) AS INTEGER) + 1911) || 
                        '-' || 
                        substr(draw_date, 5, 2) || 
                        '-' || 
                        substr(draw_date, 8, 2)
                    )
                ) as gregorian_date
            FROM (
                SELECT num1 as number, draw_date FROM {lottery_type} WHERE 1=1 {period_filter}
                UNION ALL
                SELECT num2, draw_date FROM {lottery_type} WHERE 1=1 {period_filter}
                UNION ALL
                SELECT num3, draw_date FROM {lottery_type} WHERE 1=1 {period_filter}
                UNION ALL
                SELECT num4, draw_date FROM {lottery_type} WHERE 1=1 {period_filter}
                UNION ALL
                SELECT num5, draw_date FROM {lottery_type} WHERE 1=1 {period_filter}
                {num6_query}
            )
            GROUP BY number
        )
        SELECT 
            a.number,
            COALESCE(l.last_date, '從未出現') as last_date,
            COALESCE(
                CAST(julianday('now') - julianday(l.gregorian_date) as INTEGER),
                99999
            ) as days_since_last
        FROM AllNumbers a
        LEFT JOIN LastAppearance l ON a.number = l.number
        ORDER BY days_since_last DESC, a.number ASC
        LIMIT 10
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def interval_analysis(self, lottery_type='big_lotto', periods=None):
        """區間分析
        
        Args:
            lottery_type (str): 樂透類型 ('big_lotto', 'super_lotto', 'daily_cash')
            periods (int, optional): 要分析的期數，None 表示分析所有資料
            
        Returns:
            dict: 包含以下分析結果：
            - section_stats: 號碼分區統計
            - size_ratio: 大小號碼比例
            - odd_even_ratio: 奇偶數比例
        """
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 根據樂透類型設定參數
        if lottery_type == 'daily_cash':
            max_number = 39
            sections = [(1,8), (9,16), (17,24), (25,32), (33,39)]
            mid_point = 20
            num_columns = 'num1, num2, num3, num4, num5'
        else:
            max_number = 49
            sections = [(1,10), (11,20), (21,30), (31,40), (41,49)]
            mid_point = 25
            num_columns = 'num1, num2, num3, num4, num5, num6'
        
        # 1. 分區統計
        section_stats = self._get_section_statistics(conn, lottery_type, sections, num_columns, period_filter)
        
        # 2. 大小號碼比例
        size_ratio = self._get_size_ratio(conn, lottery_type, mid_point, num_columns, period_filter)
        
        # 3. 奇偶數比例
        odd_even_ratio = self._get_odd_even_ratio(conn, lottery_type, num_columns, period_filter)
        
        conn.close()
        
        return {
            'section_stats': section_stats,
            'size_ratio': size_ratio,
            'odd_even_ratio': odd_even_ratio
        }
    
    def _get_section_statistics(self, conn, lottery_type, sections, num_columns, period_filter):
        """計算各區間的號碼分布統計"""
        numbers_query = f"""
        SELECT value as number
        FROM {lottery_type}, json_each(json_array({num_columns}))
        WHERE 1=1 {period_filter}
        """
        
        section_stats = []
        for start, end in sections:
            query = f"""
            SELECT 
                {start} as section_start,
                {end} as section_end,
                COUNT(*) as frequency,
                ROUND(COUNT(*) * 100.0 / (
                    SELECT COUNT(*) FROM ({numbers_query})
                ), 2) as percentage
            FROM ({numbers_query})
            WHERE number BETWEEN {start} AND {end}
            """
            df = pd.read_sql_query(query, conn)
            section_stats.extend(df.to_dict('records'))
        
        return section_stats
    
    def _get_size_ratio(self, conn, lottery_type, mid_point, num_columns, period_filter):
        """計算大小號碼比例"""
        query = f"""
        WITH DrawNumbers AS (
            SELECT 
                draw_term,
                SUM(CASE WHEN CAST(value as INTEGER) > {mid_point} THEN 1 ELSE 0 END) as big_count,
                SUM(CASE WHEN CAST(value as INTEGER) <= {mid_point} THEN 1 ELSE 0 END) as small_count
            FROM {lottery_type}, json_each(json_array({num_columns}))
            WHERE 1=1 {period_filter}
            GROUP BY draw_term
        )
        SELECT 
            big_count || ':' || small_count as ratio,
            COUNT(*) as frequency,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM DrawNumbers), 2) as percentage
        FROM DrawNumbers
        GROUP BY ratio
        ORDER BY frequency DESC
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def _get_odd_even_ratio(self, conn, lottery_type, num_columns, period_filter):
        """計算奇偶數比例"""
        query = f"""
        WITH DrawNumbers AS (
            SELECT 
                draw_term,
                SUM(CASE WHEN CAST(value as INTEGER) % 2 = 1 THEN 1 ELSE 0 END) as odd_count,
                SUM(CASE WHEN CAST(value as INTEGER) % 2 = 0 THEN 1 ELSE 0 END) as even_count
            FROM {lottery_type}, json_each(json_array({num_columns}))
            WHERE 1=1 {period_filter}
            GROUP BY draw_term
        )
        SELECT 
            odd_count || ':' || even_count as ratio,
            COUNT(*) as frequency,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM DrawNumbers), 2) as percentage
        FROM DrawNumbers
        GROUP BY ratio
        ORDER BY frequency DESC
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def continuity_analysis(self, lottery_type='big_lotto', periods=None):
        """連續性分析
        
        Args:
            lottery_type (str): 樂透類型 ('big_lotto', 'super_lotto', 'daily_cash')
            periods (int, optional): 要分析的期數，None 表示分析所有資料
            
        Returns:
            dict: 包含以下分析結果：
            - consecutive_stats: 連續號碼統計
            - gap_stats: 號碼間隔統計
            - number_gaps: 每個號碼的間隔分析
        """
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 1. 連續號碼分析
        consecutive_stats = self._get_consecutive_statistics(conn, lottery_type, period_filter)
        
        # 2. 號碼間隔分析
        gap_stats = self._get_gap_statistics(conn, lottery_type, period_filter)
        
        # 3. 每個號碼的間隔分析
        number_gaps = self._get_number_gap_analysis(conn, lottery_type, period_filter)
        
        conn.close()
        
        return {
            'consecutive_stats': consecutive_stats,
            'gap_stats': gap_stats,
            'number_gaps': number_gaps
        }
    
    def _get_consecutive_statistics(self, conn, lottery_type, period_filter):
        """分析連續號碼出現的情況"""
        query = f"""
        WITH RECURSIVE 
        Numbers AS (
            SELECT 
                draw_term,
                value as number
            FROM {lottery_type}, 
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
        ),
        ConsecutiveCounts AS (
            SELECT 
                draw_term,
                COUNT(*) as consecutive_count
            FROM (
                SELECT 
                    draw_term,
                    number,
                    number - ROW_NUMBER() OVER (PARTITION BY draw_term ORDER BY number) as grp
                FROM Numbers
            )
            GROUP BY draw_term, grp
            HAVING consecutive_count > 1
        )
        SELECT 
            consecutive_count,
            COUNT(*) as frequency,
            ROUND(COUNT(*) * 100.0 / (
                SELECT COUNT(DISTINCT draw_term) FROM Numbers
            ), 2) as percentage
        FROM ConsecutiveCounts
        GROUP BY consecutive_count
        ORDER BY consecutive_count
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def _get_gap_statistics(self, conn, lottery_type, period_filter):
        """分析號碼間的間隔統計"""
        query = f"""
        WITH Numbers AS (
            SELECT 
                draw_term,
                value as number
            FROM {lottery_type}, 
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            ORDER BY draw_term, number
        ),
        Gaps AS (
            SELECT 
                draw_term,
                number - LAG(number) OVER (PARTITION BY draw_term ORDER BY number) as gap
            FROM Numbers
        )
        SELECT 
            gap,
            COUNT(*) as frequency,
            ROUND(COUNT(*) * 100.0 / (
                SELECT COUNT(*) FROM Gaps WHERE gap IS NOT NULL
            ), 2) as percentage
        FROM Gaps
        WHERE gap IS NOT NULL
        GROUP BY gap
        ORDER BY frequency DESC
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def _get_number_gap_analysis(self, conn, lottery_type, period_filter):
        """分析每個號碼的出現間隔"""
        max_number = 49 if lottery_type != 'daily_cash' else 39
        
        query = f"""
        WITH RECURSIVE 
        AllNumbers AS (
            SELECT value as number
            FROM json_each(json_array({','.join(str(i) for i in range(1, max_number + 1))}))
        ),
        NumberGaps AS (
            SELECT 
                number,
                AVG(gap) as avg_gap,
                MIN(gap) as min_gap,
                MAX(gap) as max_gap
            FROM (
                SELECT 
                    number,
                    julianday(
                        (CAST(substr(LEAD(draw_date) OVER (PARTITION BY number ORDER BY draw_date), 1, 3) AS INTEGER) + 1911) || 
                        '-' || 
                        substr(LEAD(draw_date) OVER (PARTITION BY number ORDER BY draw_date), 5, 2) || 
                        '-' || 
                        substr(LEAD(draw_date) OVER (PARTITION BY number ORDER BY draw_date), 8, 2)
                    ) - 
                    julianday(
                        (CAST(substr(draw_date, 1, 3) AS INTEGER) + 1911) || 
                        '-' || 
                        substr(draw_date, 5, 2) || 
                        '-' || 
                        substr(draw_date, 8, 2)
                    ) as gap
                FROM (
                    SELECT DISTINCT
                        value as number,
                        draw_date
                    FROM {lottery_type}, 
                    json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
                    WHERE 1=1 {period_filter}
                )
            )
            WHERE gap IS NOT NULL
            GROUP BY number
        )
        SELECT 
            a.number,
            COALESCE(ROUND(n.avg_gap), 0) as avg_gap,
            COALESCE(n.min_gap, 0) as min_gap,
            COALESCE(n.max_gap, 0) as max_gap
        FROM AllNumbers a
        LEFT JOIN NumberGaps n ON a.number = n.number
        ORDER BY avg_gap DESC
        LIMIT 10
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def time_series_analysis(self, lottery_type='big_lotto', periods=None):
        """時間序列分析
        
        Args:
            lottery_type (str): 樂透類型 ('big_lotto', 'super_lotto', 'daily_cash')
            periods (int, optional): 要分析的期數，None 表示分析所有資料
            
        Returns:
            dict: 包含以下分析結果：
            - yearly_stats: 年度分析
            - monthly_stats: 月份分析
            - weekday_stats: 星期分析
            - seasonal_stats: 季節分析
        """
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 1. 年度分析
        yearly_stats = self._get_yearly_statistics(conn, lottery_type, period_filter)
        
        # 2. 月份分析
        monthly_stats = self._get_monthly_statistics(conn, lottery_type, period_filter)
        
        # 3. 星期分析
        weekday_stats = self._get_weekday_statistics(conn, lottery_type, period_filter)
        
        # 4. 季節分析
        seasonal_stats = self._get_seasonal_statistics(conn, lottery_type, period_filter)
        
        conn.close()
        
        return {
            'yearly_stats': yearly_stats,
            'monthly_stats': monthly_stats,
            'weekday_stats': weekday_stats,
            'seasonal_stats': seasonal_stats
        }
    
    def _get_yearly_statistics(self, conn, lottery_type, period_filter):
        """分析每年的號碼分布趨勢"""
        query = f"""
        WITH YearlyNumbers AS (
            SELECT 
                CAST(substr(draw_date, 1, 3) AS INTEGER) + 1911 as year,
                value as number,
                draw_term
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
        ),
        YearlyDraws AS (
            SELECT 
                year,
                COUNT(DISTINCT draw_term) as draw_count,
                COUNT(*) as total_numbers
            FROM YearlyNumbers
            GROUP BY year
        ),
        NumberFrequency AS (
            SELECT 
                year,
                number,
                COUNT(*) as frequency,
                ROW_NUMBER() OVER (PARTITION BY year ORDER BY COUNT(*) DESC) as rank
            FROM YearlyNumbers
            GROUP BY year, number
        )
        SELECT 
            n.year,
            d.total_numbers,
            d.draw_count,
            GROUP_CONCAT(
                n.number || '(' || n.frequency || '次)'
            ) as hot_numbers
        FROM NumberFrequency n
        JOIN YearlyDraws d ON n.year = d.year
        WHERE rank <= 20
        GROUP BY n.year
        ORDER BY n.year DESC
        LIMIT 10
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def _get_monthly_statistics(self, conn, lottery_type, period_filter):
        """分析每月的號碼分布趨勢"""
        query = f"""
        WITH MonthlyNumbers AS (
            SELECT 
                CAST(substr(draw_date, 5, 2) AS INTEGER) as month,
                value as number,
                draw_term
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
        ),
        MonthlyDraws AS (
            SELECT 
                month,
                COUNT(DISTINCT draw_term) as draw_count,
                COUNT(*) as total_numbers
            FROM MonthlyNumbers
            GROUP BY month
        ),
        NumberFrequency AS (
            SELECT 
                month,
                number,
                COUNT(*) as frequency,
                ROW_NUMBER() OVER (PARTITION BY month ORDER BY COUNT(*) DESC) as rank
            FROM MonthlyNumbers
            GROUP BY month, number
        )
        SELECT 
            n.month,
            d.draw_count,
            d.total_numbers,
            GROUP_CONCAT(
                n.number || '(' || n.frequency || '次)'
            ) as common_numbers
        FROM NumberFrequency n
        JOIN MonthlyDraws d ON n.month = d.month
        WHERE rank <= 20
        GROUP BY n.month
        ORDER BY n.month
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def _get_weekday_statistics(self, conn, lottery_type, period_filter):
        """分析每個星期的號碼分布趨勢"""
        query = f"""
        WITH WeekdayNumbers AS (
            SELECT 
                CAST(strftime('%w', 
                    (CAST(substr(draw_date, 1, 3) AS INTEGER) + 1911) || 
                    '-' || 
                    substr(draw_date, 5, 2) || 
                    '-' || 
                    substr(draw_date, 8, 2)
                ) AS INTEGER) as weekday,
                value as number,
                draw_term
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
        ),
        WeekdayDraws AS (
            SELECT 
                weekday,
                COUNT(DISTINCT draw_term) as draw_count,
                COUNT(*) as total_numbers
            FROM WeekdayNumbers
            GROUP BY weekday
        ),
        NumberFrequency AS (
            SELECT 
                weekday,
                number,
                COUNT(*) as frequency,
                ROW_NUMBER() OVER (PARTITION BY weekday ORDER BY COUNT(*) DESC) as rank
            FROM WeekdayNumbers
            GROUP BY weekday, number
        )
        SELECT 
            n.weekday,
            d.draw_count,
            d.total_numbers,
            GROUP_CONCAT(
                n.number || '(' || n.frequency || '次)'
            ) as common_numbers
        FROM NumberFrequency n
        JOIN WeekdayDraws d ON n.weekday = d.weekday
        WHERE rank <= 20
        GROUP BY n.weekday
        ORDER BY n.weekday
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def _get_seasonal_statistics(self, conn, lottery_type, period_filter):
        """分析季節性趨勢"""
        query = f"""
        WITH SeasonalNumbers AS (
            SELECT 
                CASE 
                    WHEN CAST(substr(draw_date, 5, 2) AS INTEGER) IN (3,4,5) THEN '春季'
                    WHEN CAST(substr(draw_date, 5, 2) AS INTEGER) IN (6,7,8) THEN '夏季'
                    WHEN CAST(substr(draw_date, 5, 2) AS INTEGER) IN (9,10,11) THEN '秋季'
                    ELSE '冬季'
                END as season,
                value as number,
                draw_term
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
        ),
        SeasonalDraws AS (
            SELECT 
                season,
                COUNT(DISTINCT draw_term) as draw_count,
                COUNT(*) as total_numbers
            FROM SeasonalNumbers
            GROUP BY season
        ),
        NumberFrequency AS (
            SELECT 
                season,
                number,
                COUNT(*) as frequency,
                ROW_NUMBER() OVER (PARTITION BY season ORDER BY COUNT(*) DESC) as rank
            FROM SeasonalNumbers
            GROUP BY season, number
        )
        SELECT 
            n.season,
            d.draw_count,
            d.total_numbers,
            GROUP_CONCAT(
                n.number || '(' || n.frequency || '次)'
            ) as common_numbers
        FROM NumberFrequency n
        JOIN SeasonalDraws d ON n.season = d.season
        WHERE rank <= 20
        GROUP BY n.season
        ORDER BY 
            CASE n.season 
                WHEN '春季' THEN 1 
                WHEN '夏季' THEN 2 
                WHEN '秋季' THEN 3 
                ELSE 4 
            END
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def prize_analysis(self, lottery_type='big_lotto'):
        """獎金分析
        - 銷售金額與開獎號碼的關係
        - 特定號碼組合出現時的平均銷售額
        """
        pass
    
    def prediction_analysis(self, lottery_type='big_lotto', periods=None):
        """預測分析
        
        Args:
            lottery_type (str): 樂透類型 ('big_lotto', 'super_lotto', 'daily_cash')
            periods (int, optional): 要分析的期數，None 表示分析所有資料
            
        Returns:
            dict: 包含以下分析結果：
            - hot_numbers: 近期熱門號碼分析
            - cold_numbers: 近期冷門號碼分析
            - number_trends: 號碼趨勢分析
            - pattern_numbers: 特定模式號碼
        """
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 1. 近期熱門號碼分析
        hot_numbers = self._get_hot_numbers(conn, lottery_type, period_filter)
        
        # 2. 近期冷門號碼分析
        cold_numbers = self._get_recent_cold_numbers(conn, lottery_type, period_filter)
        
        # 3. 號碼趨勢分析
        number_trends = self._get_number_trends(conn, lottery_type, period_filter)
        
        # 4. 特定模式號碼
        pattern_numbers = self._get_pattern_numbers(conn, lottery_type, period_filter)
        
        conn.close()
        
        return {
            'hot_numbers': hot_numbers,
            'cold_numbers': cold_numbers,
            'number_trends': number_trends,
            'pattern_numbers': pattern_numbers
        }
    
    def _get_hot_numbers(self, conn, lottery_type, period_filter):
        """分析近期熱門號碼"""
        query = f"""
        WITH NumberStats AS (
            SELECT 
                value as number,
                COUNT(*) as frequency,
                ROUND(COUNT(*) * 100.0 / (
                    SELECT COUNT(DISTINCT draw_term) 
                    FROM {lottery_type}
                    WHERE 1=1 {period_filter}
                ), 2) as percentage,
                ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rank
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY value
        )
        SELECT 
            number,
            frequency,
            percentage,
            rank
        FROM NumberStats
        WHERE rank <= 20
        ORDER BY frequency DESC
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def _get_recent_cold_numbers(self, conn, lottery_type, period_filter):
        """分析近期冷門號碼"""
        num6_query = f"""
            UNION ALL
            SELECT num6, draw_date FROM {lottery_type} WHERE 1=1 {period_filter}
        """ if lottery_type != 'daily_cash' else ""
        
        query = f"""
        WITH LastAppearance AS (
            SELECT 
                number,
                MAX(draw_date) as last_date,
                COUNT(*) as total_appearances,
                julianday(
                    (CAST(substr(MAX(draw_date), 1, 3) AS INTEGER) + 1911) || 
                    '-' || 
                    substr(MAX(draw_date), 5, 2) || 
                    '-' || 
                    substr(MAX(draw_date), 8, 2)
                ) as last_appearance_date
            FROM (
                SELECT num1 as number, draw_date FROM {lottery_type} WHERE 1=1 {period_filter}
                UNION ALL
                SELECT num2, draw_date FROM {lottery_type} WHERE 1=1 {period_filter}
                UNION ALL
                SELECT num3, draw_date FROM {lottery_type} WHERE 1=1 {period_filter}
                UNION ALL
                SELECT num4, draw_date FROM {lottery_type} WHERE 1=1 {period_filter}
                UNION ALL
                SELECT num5, draw_date FROM {lottery_type} WHERE 1=1 {period_filter}
                {num6_query}
            )
            GROUP BY number
        )
        SELECT 
            number,
            last_date,
            total_appearances,
            CAST(julianday('now') - last_appearance_date as INTEGER) as days_since_last
        FROM LastAppearance
        ORDER BY days_since_last DESC
        LIMIT 20
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def _get_number_trends(self, conn, lottery_type, period_filter):
        """分析號碼出現趨勢"""
        query = f"""
        WITH RECURSIVE 
        Periods AS (
            SELECT 
                draw_term,
                draw_date,
                ROW_NUMBER() OVER (ORDER BY draw_term DESC) as period_group
            FROM {lottery_type}
            WHERE 1=1 {period_filter}
        ),
        NumberCounts AS (
            SELECT 
                p.period_group,
                n.value as number,
                COUNT(*) as frequency
            FROM Periods p
            JOIN {lottery_type} t ON p.draw_term = t.draw_term
            JOIN json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''})) n
            GROUP BY p.period_group, n.value
        ),
        TrendAnalysis AS (
            SELECT 
                number,
                AVG(frequency) as avg_frequency,
                MIN(frequency) as min_frequency,
                MAX(frequency) as max_frequency,
                CASE 
                    WHEN COUNT(*) >= 3 AND 
                         MAX(CASE WHEN period_group <= 3 THEN frequency END) > 
                         AVG(CASE WHEN period_group > 3 THEN frequency END)
                    THEN '上升'
                    WHEN COUNT(*) >= 3 AND 
                         MAX(CASE WHEN period_group <= 3 THEN frequency END) < 
                         AVG(CASE WHEN period_group > 3 THEN frequency END)
                    THEN '下降'
                    ELSE '持平'
                END as trend
            FROM NumberCounts
            GROUP BY number
            HAVING COUNT(*) >= 3
        )
        SELECT *
        FROM TrendAnalysis
        ORDER BY 
            CASE trend
                WHEN '上升' THEN 1
                WHEN '下降' THEN 2
                ELSE 3
            END,
            avg_frequency DESC
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def _get_pattern_numbers(self, conn, lottery_type, period_filter):
        """分析特定模式的號碼"""
        query = f"""
        WITH DrawPatterns AS (
            SELECT 
                draw_term,
                -- 計算大小比例
                SUM(CASE WHEN value > {25 if lottery_type != 'daily_cash' else 20} THEN 1 ELSE 0 END) as big_count,
                -- 計算奇偶比例
                SUM(CASE WHEN value % 2 = 1 THEN 1 ELSE 0 END) as odd_count,
                -- 計算區間分布
                SUM(CASE WHEN value <= 10 THEN 1 ELSE 0 END) as section1_count,
                SUM(CASE WHEN value > 10 AND value <= 20 THEN 1 ELSE 0 END) as section2_count,
                SUM(CASE WHEN value > 20 AND value <= 30 THEN 1 ELSE 0 END) as section3_count,
                SUM(CASE WHEN value > 30 AND value <= 40 THEN 1 ELSE 0 END) as section4_count,
                SUM(CASE WHEN value > 40 THEN 1 ELSE 0 END) as section5_count
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY draw_term
        ),
        PatternStats AS (
            SELECT 
                big_count || '大' || ({6 if lottery_type != 'daily_cash' else 5} - big_count) || '小' as size_pattern,
                odd_count || '奇' || ({6 if lottery_type != 'daily_cash' else 5} - odd_count) || '偶' as odd_even_pattern,
                section1_count || '-' || section2_count || '-' || section3_count || '-' || 
                section4_count || '-' || section5_count as section_pattern,
                COUNT(*) as frequency,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM DrawPatterns), 2) as percentage
            FROM DrawPatterns
            GROUP BY size_pattern, odd_even_pattern, section_pattern
        )
        SELECT *
        FROM PatternStats
        ORDER BY frequency DESC
        LIMIT 10
        """
        
        df = pd.read_sql_query(query, conn)
        return df.to_dict('records')
    
    def get_data_range(self, lottery_type='big_lotto'):
        """取得資料的時間範圍
        
        Args:
            lottery_type (str): 樂透類型 ('big_lotto', 'super_lotto', 'daily_cash')
            
        Returns:
            dict: 包含以下資訊：
            - start_date: 最早的資料日期
            - end_date: 最新的資料日期
            - total_draws: 總開獎次數
        """
        conn = self._get_connection()
        
        query = f"""
        SELECT 
            MIN(draw_date) as start_date,
            MAX(draw_date) as end_date,
            COUNT(*) as total_draws
        FROM {lottery_type}
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df.to_dict('records')[0] 
    
    def recommend_numbers(self, lottery_type='big_lotto', periods=None):
        """推薦號碼
        
        根據綜合分析結果推薦熱門號碼和冷門號碼
        
        Args:
            lottery_type (str): 樂透類型 ('big_lotto', 'super_lotto', 'daily_cash')
            periods (int, optional): 要分析的期數，None 表示分析所有資料
            
        Returns:
            dict: 包含以下推薦結果：
            - hot_numbers: 推薦的熱門號碼（15個）
            - cold_numbers: 推薦的冷門號碼（15個）
            - analysis_summary: 分析依據摘要
        """
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 1. 綜合分析查詢
        query = f"""
        WITH 
        -- 近期熱門號碼分析
        HotNumbers AS (
            SELECT 
                value as number,
                COUNT(*) as frequency,
                ROUND(COUNT(*) * 100.0 / (
                    SELECT COUNT(DISTINCT draw_term) 
                    FROM {lottery_type}
                    WHERE 1=1 {period_filter}
                ), 2) as percentage,
                1 as score
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY value
        ),
        -- 趨勢分析
        TrendAnalysis AS (
            SELECT 
                number,
                CASE trend
                    WHEN '上升' THEN 2
                    WHEN '持平' THEN 1
                    ELSE 0
                END as trend_score
            FROM (
                WITH RECURSIVE 
                Periods AS (
                    SELECT 
                        draw_term,
                        draw_date,
                        ROW_NUMBER() OVER (ORDER BY draw_term DESC) as period_group
                    FROM {lottery_type}
                    WHERE 1=1 {period_filter}
                ),
                NumberCounts AS (
                    SELECT 
                        p.period_group,
                        n.value as number,
                        COUNT(*) as frequency
                    FROM Periods p
                    JOIN {lottery_type} t ON p.draw_term = t.draw_term
                    JOIN json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''})) n
                    GROUP BY p.period_group, n.value
                )
                SELECT 
                    number,
                    CASE 
                        WHEN COUNT(*) >= 3 AND 
                             MAX(CASE WHEN period_group <= 3 THEN frequency END) > 
                             AVG(CASE WHEN period_group > 3 THEN frequency END)
                        THEN '上升'
                        WHEN COUNT(*) >= 3 AND 
                             MAX(CASE WHEN period_group <= 3 THEN frequency END) < 
                             AVG(CASE WHEN period_group > 3 THEN frequency END)
                        THEN '下降'
                        ELSE '持平'
                    END as trend
                FROM NumberCounts
                GROUP BY number
                HAVING COUNT(*) >= 3
            )
        ),
        -- 綜合評分
        FinalScore AS (
            SELECT 
                h.number,
                h.frequency,
                h.percentage,
                COALESCE(t.trend_score, 1) as trend_score,
                h.frequency * COALESCE(t.trend_score, 1) as total_score
            FROM HotNumbers h
            LEFT JOIN TrendAnalysis t ON h.number = t.number
        )
        SELECT *
        FROM FinalScore
        ORDER BY total_score DESC
        """
        
        df = pd.read_sql_query(query, conn)
        
        # 2. 取得冷門號碼
        cold_query = self._get_recent_cold_numbers(conn, lottery_type, period_filter)
        
        # 3. 整理推薦結果
        hot_recommendations = df.head(15).to_dict('records')
        cold_recommendations = cold_query[:15]
        
        # 4. 產生分析摘要
        summary = {
            'hot_numbers_criteria': [
                '根據近期出現頻率',
                '考慮號碼趨勢（上升/持平/下降）',
                '綜合評分排序'
            ],
            'cold_numbers_criteria': [
                '根據最近出現日期',
                '考慮歷史出現次數',
                '間隔天數加權'
            ]
        }
        
        conn.close()
        
        return {
            'hot_numbers': hot_recommendations,
            'cold_numbers': cold_recommendations,
            'analysis_summary': summary
        } 
    
    def combination_pattern_analysis(self, lottery_type='big_lotto', periods=None):
        """分析中獎號碼的組合模式"""
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 1. 連續號碼組合分析
        query_consecutive = f"""
        WITH DrawNumbers AS (
            SELECT 
                draw_term,
                GROUP_CONCAT(value) OVER (PARTITION BY draw_term ORDER BY value) as numbers
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
        ),
        ConsecutivePatterns AS (
            SELECT DISTINCT
                draw_term,
                numbers,
                CASE 
                    WHEN numbers LIKE '%,_,_+1,_+2%' THEN '3連號'
                    WHEN numbers LIKE '%,_,_+1%' THEN '2連號'
                    ELSE '無連號'
                END as pattern_type
            FROM DrawNumbers
        )
        SELECT 
            pattern_type,
            COUNT(*) as frequency,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT draw_term) FROM ConsecutivePatterns), 2) as percentage
        FROM ConsecutivePatterns
        GROUP BY pattern_type
        ORDER BY frequency DESC
        """
        
        # 2. 同尾數組合分析
        query_same_tail = f"""
        WITH DrawNumbers AS (
            SELECT 
                draw_term,
                GROUP_CONCAT(value % 10) as tails
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY draw_term
        ),
        TailCounts AS (
            SELECT 
                draw_term,
                tails,
                (
                    LENGTH(tails) - 
                    LENGTH(REPLACE(tails, '0', '')) +
                    LENGTH(tails) - 
                    LENGTH(REPLACE(tails, '1', '')) +
                    LENGTH(tails) - 
                    LENGTH(REPLACE(tails, '2', '')) +
                    LENGTH(tails) - 
                    LENGTH(REPLACE(tails, '3', '')) +
                    LENGTH(tails) - 
                    LENGTH(REPLACE(tails, '4', '')) +
                    LENGTH(tails) - 
                    LENGTH(REPLACE(tails, '5', '')) +
                    LENGTH(tails) - 
                    LENGTH(REPLACE(tails, '6', '')) +
                    LENGTH(tails) - 
                    LENGTH(REPLACE(tails, '7', '')) +
                    LENGTH(tails) - 
                    LENGTH(REPLACE(tails, '8', '')) +
                    LENGTH(tails) - 
                    LENGTH(REPLACE(tails, '9', ''))
                ) / 2 as same_tail_count
            FROM DrawNumbers
        )
        SELECT 
            CAST(same_tail_count as INTEGER) || '個同尾' as pattern_type,
            COUNT(*) as frequency,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM DrawNumbers), 2) as percentage
        FROM TailCounts
        GROUP BY same_tail_count
        ORDER BY same_tail_count DESC
        """
        
        # 3. 等差數列組合分析
        query_arithmetic = f"""
        WITH RECURSIVE 
        Numbers AS (
            SELECT 
                draw_term,
                value as number,
                ROW_NUMBER() OVER (PARTITION BY draw_term ORDER BY value) as position
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
        ),
        ArithmeticSequences AS (
            SELECT 
                n1.draw_term,
                n1.number as num1,
                n2.number as num2,
                n3.number as num3,
                n2.number - n1.number as diff
            FROM Numbers n1
            JOIN Numbers n2 ON n1.draw_term = n2.draw_term AND n2.position = n1.position + 1
            JOIN Numbers n3 ON n2.draw_term = n3.draw_term AND n3.position = n2.position + 1
            WHERE n3.number - n2.number = n2.number - n1.number
        )
        SELECT 
            diff || '差距' as pattern_type,
            COUNT(*) as frequency,
            ROUND(COUNT(*) * 100.0 / (
                SELECT COUNT(DISTINCT draw_term) FROM {lottery_type} WHERE 1=1 {period_filter}
            ), 2) as percentage,
            GROUP_CONCAT(DISTINCT num1 || ',' || num2 || ',' || num3) as examples
        FROM ArithmeticSequences
        GROUP BY diff
        HAVING frequency >= 2
        ORDER BY frequency DESC
        """
        
        # 4. 鄰近號碼組合分析
        query_adjacent = f"""
        WITH DrawNumbers AS (
            SELECT 
                draw_term,
                value as number,
                LAG(value) OVER (PARTITION BY draw_term ORDER BY value) as prev_number
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
        ),
        AdjacentPairs AS (
            SELECT 
                draw_term,
                COUNT(*) as adjacent_pairs
            FROM DrawNumbers
            WHERE number - prev_number <= 3 
            AND number - prev_number > 1  -- 排除連續號碼
            GROUP BY draw_term
        )
        SELECT 
            adjacent_pairs || '組鄰近號碼' as pattern_type,
            COUNT(*) as frequency,
            ROUND(COUNT(*) * 100.0 / (
                SELECT COUNT(DISTINCT draw_term) FROM {lottery_type} WHERE 1=1 {period_filter}
            ), 2) as percentage
        FROM AdjacentPairs
        GROUP BY adjacent_pairs
        ORDER BY adjacent_pairs DESC
        """
        
        # 執行查詢
        consecutive_patterns = pd.read_sql_query(query_consecutive, conn)
        same_tail_patterns = pd.read_sql_query(query_same_tail, conn)
        arithmetic_patterns = pd.read_sql_query(query_arithmetic, conn)
        adjacent_patterns = pd.read_sql_query(query_adjacent, conn)
        
        conn.close()
        
        return {
            'consecutive_patterns': consecutive_patterns.to_dict('records'),
            'same_tail_patterns': same_tail_patterns.to_dict('records'),
            'arithmetic_patterns': arithmetic_patterns.to_dict('records'),
            'adjacent_patterns': adjacent_patterns.to_dict('records')
        } 
    
    def combination_prediction(self, lottery_type='big_lotto', periods=None):
        """號碼組合預測分析
        
        Args:
            lottery_type (str): 樂透類型 ('big_lotto', 'super_lotto', 'daily_cash')
            periods (int, optional): 要分析的期數，None 表示分析所有資料
            
        Returns:
            dict: 包含以下分析結果：
            - high_prob_combinations: 高機率組合
            - balanced_combinations: 平衡性組合
            - pattern_combinations: 模式型組合
            - analysis_criteria: 分析標準說明
        """
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 1. 高機率組合分析
        query_high_prob = f"""
        WITH NumberStats AS (
            SELECT 
                n1.value as num1,
                n2.value as num2,
                COUNT(*) as frequency,
                ROUND(COUNT(*) * 100.0 / (
                    SELECT COUNT(DISTINCT draw_term) 
                    FROM {lottery_type}
                    WHERE 1=1 {period_filter}
                ), 2) as percentage
            FROM {lottery_type} t1
            JOIN json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''})) n1
            JOIN json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''})) n2
            WHERE 1=1 {period_filter}
            AND n1.value < n2.value
            GROUP BY n1.value, n2.value
            HAVING frequency >= 3
            ORDER BY frequency DESC
            LIMIT 10
        )
        SELECT 
            num1 || ',' || num2 as number_pair,
            frequency,
            percentage
        FROM NumberStats
        """
        
        # 2. 平衡性組合分析
        query_balanced = f"""
        WITH DrawStats AS (
            SELECT 
                draw_term,
                COUNT(CASE WHEN value <= {25 if lottery_type != 'daily_cash' else 20} THEN 1 END) as small_count,
                COUNT(CASE WHEN value % 2 = 1 THEN 1 END) as odd_count,
                GROUP_CONCAT(value) as numbers
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY draw_term
            HAVING 
                small_count BETWEEN 2 AND 4
                AND odd_count BETWEEN 2 AND 4
        )
        SELECT 
            numbers as combination,
            COUNT(*) as frequency,
            ROUND(COUNT(*) * 100.0 / (
                SELECT COUNT(*) FROM {lottery_type} WHERE 1=1 {period_filter}
            ), 2) as percentage
        FROM DrawStats
        GROUP BY numbers
        ORDER BY frequency DESC
        LIMIT 10
        """
        
        # 3. 模式型組合分析
        query_pattern = f"""
        WITH RECURSIVE 
        DrawPatterns AS (
            SELECT 
                draw_term,
                GROUP_CONCAT(
                    CASE 
                        WHEN value <= {25 if lottery_type != 'daily_cash' else 20} THEN 'S'
                        ELSE 'L'
                    END || 
                    CASE 
                        WHEN value % 2 = 1 THEN 'O'
                        ELSE 'E'
                    END
                ) as pattern,
                GROUP_CONCAT(value) as numbers
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY draw_term
        )
        SELECT 
            pattern,
            COUNT(*) as frequency,
            ROUND(COUNT(*) * 100.0 / (
                SELECT COUNT(*) FROM DrawPatterns
            ), 2) as percentage,
            GROUP_CONCAT(numbers) as example_combinations
        FROM DrawPatterns
        GROUP BY pattern
        HAVING frequency >= 3
        ORDER BY frequency DESC
        LIMIT 10
        """
        
        # 執行查詢
        high_prob_combinations = pd.read_sql_query(query_high_prob, conn)
        balanced_combinations = pd.read_sql_query(query_balanced, conn)
        pattern_combinations = pd.read_sql_query(query_pattern, conn)
        
        # 分析標準說明
        analysis_criteria = {
            'high_prob': [
                '分析號碼對的共同出現頻率',
                '考慮號碼對的最近出現時間',
                '計算號碼對的穩定性'
            ],
            'balanced': [
                '大小號碼比例平衡',
                '奇偶數比例平衡',
                '區間分布均勻'
            ],
            'pattern': [
                '分析歷史開獎的號碼模式',
                '考慮模式的重複性',
                '評估模式的穩定性'
            ]
        }
        
        conn.close()
        
        return {
            'high_prob_combinations': high_prob_combinations.to_dict('records'),
            'balanced_combinations': balanced_combinations.to_dict('records'),
            'pattern_combinations': pattern_combinations.to_dict('records'),
            'analysis_criteria': analysis_criteria
        } 
    
    def advanced_statistics(self, lottery_type='big_lotto', periods=None):
        """進階統計分析
        
        Args:
            lottery_type (str): 樂透類型 ('big_lotto', 'super_lotto', 'daily_cash')
            periods (int, optional): 要分析的期數，None 表示分析所有資料
            
        Returns:
            dict: 包含以下分析結果：
            - autocorrelation: 號碼自相關性分析
            - cross_correlation: 號碼交叉關聯分析
            - markov_chain: 馬可夫鏈分析
            - bayes_probability: 貝氏機率分析
        """
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 1. 號碼自相關性分析
        query_autocorr = f"""
        WITH NumberSequence AS (
            SELECT 
                draw_term,
                value as number,
                LAG(draw_term, 1) OVER (PARTITION BY value ORDER BY draw_term) as prev_draw_term,
                ROW_NUMBER() OVER (PARTITION BY value ORDER BY draw_term) as appearance_count
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
        ),
        DateConversion AS (
            SELECT 
                number,
                CAST(
                    (
                        (CAST(substr(draw_term, 1, 3) AS INTEGER) * 365) + 
                        (CAST(substr(draw_term, 4, 2) AS INTEGER) * 30) + 
                        CAST(substr(draw_term, 6, 2) AS INTEGER)
                    ) - 
                    (
                        (CAST(substr(prev_draw_term, 1, 3) AS INTEGER) * 365) + 
                        (CAST(substr(prev_draw_term, 4, 2) AS INTEGER) * 30) + 
                        CAST(substr(prev_draw_term, 6, 2) AS INTEGER)
                    ) AS INTEGER
                ) as interval_days
            FROM NumberSequence
            WHERE prev_draw_term IS NOT NULL
        ),
        IntervalStats AS (
            SELECT 
                number,
                COUNT(*) as repeat_count,
                AVG(interval_days) as avg_interval,
                SQRT(
                    AVG(CAST(interval_days AS FLOAT) * CAST(interval_days AS FLOAT)) - 
                    POWER(AVG(CAST(interval_days AS FLOAT)), 2)
                ) as std_dev
            FROM DateConversion
            GROUP BY number
            HAVING COUNT(*) >= 3
        )
        SELECT 
            number,
            ROUND(avg_interval, 1) as avg_interval,
            repeat_count,
            ROUND(std_dev, 1) as std_dev
        FROM IntervalStats
        ORDER BY avg_interval ASC
        LIMIT 10
        """
        
        # 2. 號碼交叉關聯分析
        query_cross = f"""
        WITH PairCounts AS (
            SELECT 
                n1.value as num1,
                n2.value as num2,
                COUNT(*) as frequency,
                ROUND(COUNT(*) * 100.0 / (
                    SELECT COUNT(DISTINCT draw_term) 
                    FROM {lottery_type}
                    WHERE 1=1 {period_filter}
                ), 2) as percentage
            FROM {lottery_type} t1
            JOIN json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''})) n1
            JOIN json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''})) n2
            WHERE 1=1 {period_filter}
            AND n1.value < n2.value
            GROUP BY n1.value, n2.value
        )
        SELECT 
            num1 || '-' || num2 as number_pair,
            frequency,
            percentage,
            ROUND(frequency * percentage / 100.0, 2) as correlation_score
        FROM PairCounts
        WHERE frequency >= 3
        ORDER BY correlation_score DESC
        LIMIT 10
        """
        
        # 3. 馬可夫鏈分析
        query_markov = f"""
        WITH ConsecutiveDraws AS (
            SELECT 
                draw_term,
                GROUP_CONCAT(value) as current_numbers,
                LEAD(GROUP_CONCAT(value)) OVER (ORDER BY draw_term) as next_numbers
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY draw_term
        ),
        Transitions AS (
            SELECT 
                current_numbers,
                next_numbers,
                COUNT(*) as frequency
            FROM ConsecutiveDraws
            WHERE next_numbers IS NOT NULL
            GROUP BY current_numbers, next_numbers
        )
        SELECT 
            current_numbers,
            next_numbers,
            frequency,
            ROUND(frequency * 100.0 / (
                SELECT COUNT(*) FROM ConsecutiveDraws WHERE next_numbers IS NOT NULL
            ), 2) as transition_prob
        FROM Transitions
        ORDER BY frequency DESC
        LIMIT 10
        """
        
        # 4. 貝氏機率分析
        query_bayes = f"""
        WITH PriorProbs AS (
            SELECT 
                value as number,
                COUNT(*) as frequency,
                COUNT(*) * 1.0 / (
                    SELECT COUNT(*) 
                    FROM {lottery_type},
                    json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
                    WHERE 1=1 {period_filter}
                ) as prior_prob
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY value
        ),
        ConditionalProbs AS (
            SELECT 
                p1.number,
                p1.prior_prob,
                COUNT(DISTINCT CASE WHEN 
                    p1.number IN (
                        SELECT value 
                        FROM json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
                        WHERE value = p1.number
                    )
                    THEN t2.draw_term 
                END) * 1.0 / 
                COUNT(DISTINCT t2.draw_term) as conditional_prob
            FROM PriorProbs p1
            CROSS JOIN {lottery_type} t2
            WHERE t2.draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                WHERE 1=1 {period_filter}
                ORDER BY draw_term DESC 
                LIMIT 10
            )
            GROUP BY p1.number, p1.prior_prob
        )
        SELECT 
            number,
            ROUND(prior_prob * 100, 2) as prior_probability,
            ROUND(conditional_prob * 100, 2) as conditional_probability,
            ROUND(prior_prob * conditional_prob * 100, 2) as posterior_probability
        FROM ConditionalProbs
        ORDER BY posterior_probability DESC
        LIMIT 10
        """
        
        # 執行查詢
        autocorrelation = pd.read_sql_query(query_autocorr, conn)
        cross_correlation = pd.read_sql_query(query_cross, conn)
        markov_chain = pd.read_sql_query(query_markov, conn)
        bayes_probability = pd.read_sql_query(query_bayes, conn)
        
        # 分析說明
        analysis_description = {
            '號碼自相關性': '''分析每個號碼的出現間隔模式，包括：
            - 平均間隔：計算號碼兩次出現之間的平均天數
            - 重複次數：統計號碼在分析期間內出現的總次數
            - 標準差：衡量號碼出現間隔的穩定性，數值越小表示越規律
            這種分析可以幫助我們了解每個號碼的出現週期和規律性''',
            
            '號碼交叉關聯': '''分析不同號碼之間的關聯性，包括：
            - 共同出現頻率：計算兩個號碼一起出現的次數
            - 出現率：兩個號碼共同出現的機率
            - 相關分數：綜合考慮頻率和出現率的關聯強度指標
            這種分析可以找出經常一起出現的號碼組合''',
            
            '馬可夫鏈分析': '''馬可夫鏈是一種機率模型，用於分析事件的轉移規律，在樂透分析中：
            
            1. 基本概念：
               - 假設下一期號碼組合的出現，與當前期的號碼組合有關
               - 通過分析歷史數據，計算不同號碼組合之間的轉移機率
            
            2. 分析內容：
               - 當前組合：本期開出的號碼組合（例如：1,15,26,33,42,48）
               - 下期組合：下一期實際開出的號碼組合
               - 出現次數：這種轉移模式出現的次數
               - 轉移機率：特定轉移模式發生的機率
            
            3. 實際應用：
               - 找出高機率的號碼轉移模式
               - 預測下一期可能出現的號碼組合
               - 發現號碼組合的演變規律
            
            4. 使用建議：
               - 關注高轉移機率的組合
               - 結合其他分析方法使用
               - 注意樣本數量的影響
            
            這種分析方法特別適合尋找號碼組合的演變規律，但需要注意樂透開獎具有隨機性，
            預測僅供參考。''',
            
            '貝氏機率分析': '''使用貝氏定理進行機率分析，包括：
            - 先驗機率：根據歷史數據計算的基本出現機率
            - 條件機率：考慮最近開獎結果後的出現機率
            - 後驗機率：綜合歷史數據和近期表現的最終機率
            這種分析可以更準確地預測號碼出現的可能性'''
        }
        
        conn.close()
        
        return {
            'autocorrelation': autocorrelation.to_dict('records'),
            'cross_correlation': cross_correlation.to_dict('records'),
            'markov_chain': markov_chain.to_dict('records'),
            'bayes_probability': bayes_probability.to_dict('records'),
            'analysis_description': {
                'autocorrelation': '號碼自相關性',
                'cross_correlation': '號碼交叉關聯',
                'markov_chain': '馬可夫鏈分析',
                'bayes_probability': '貝氏機率分析',
                'descriptions': analysis_description
            }
        } 
    
    def missing_value_analysis(self, lottery_type='big_lotto', periods=None):
        """遺漏值分析
        
        Args:
            lottery_type (str): 樂透類型 ('big_lotto', 'super_lotto', 'daily_cash')
            periods (int, optional): 要分析的期數，None 表示分析所有資料
            
        Returns:
            dict: 包含以下分析結果：
            - current_missing: 當前遺漏值分析
            - historical_missing: 歷史遺漏值分析
            - missing_patterns: 遺漏值模式分析
            - missing_statistics: 遺漏值統計
        """
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 1. 當前遺漏值分析
        query_current = f"""
        WITH RECURSIVE 
        Numbers(num) AS (
            SELECT 1
            UNION ALL
            SELECT num + 1 
            FROM Numbers 
            WHERE num < {49 if lottery_type != 'daily_cash' else 39}
        ),
        LastAppearance AS (
            SELECT 
                value as num,
                MAX(draw_term) as last_appearance,
                COUNT(*) as total_appearances
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY value
        )
        SELECT 
            n.num as number,
            COALESCE(l.last_appearance, '從未出現') as last_appearance,
            COALESCE(l.total_appearances, 0) as total_appearances,
            (
                SELECT COUNT(DISTINCT draw_term)
                FROM {lottery_type}
                WHERE draw_term > COALESCE(l.last_appearance, '000000000')
                {period_filter}
            ) as missing_draws
        FROM Numbers n
        LEFT JOIN LastAppearance l ON n.num = l.num
        ORDER BY missing_draws DESC, n.num ASC
        """
        
        # 2. 歷史遺漏值分析
        query_historical = f"""
        WITH RECURSIVE 
        DrawSequence AS (
            SELECT 
                draw_term,
                value as number
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
        ),
        MissingPeriods AS (
            SELECT 
                number,
                draw_term,
                LAG(draw_term) OVER (PARTITION BY number ORDER BY draw_term) as prev_appearance,
                ROW_NUMBER() OVER (PARTITION BY number ORDER BY draw_term DESC) as rn
            FROM DrawSequence
        )
        SELECT 
            number,
            MAX(CAST(
                (
                    (CAST(substr(draw_term, 1, 3) AS INTEGER) * 365) + 
                    (CAST(substr(draw_term, 4, 2) AS INTEGER) * 30) + 
                    CAST(substr(draw_term, 6, 2) AS INTEGER)
                ) - 
                (
                    (CAST(substr(prev_appearance, 1, 3) AS INTEGER) * 365) + 
                    (CAST(substr(prev_appearance, 4, 2) AS INTEGER) * 30) + 
                    CAST(substr(prev_appearance, 6, 2) AS INTEGER)
                ) AS INTEGER
            )) as max_missing_periods,
            AVG(CAST(
                (
                    (CAST(substr(draw_term, 1, 3) AS INTEGER) * 365) + 
                    (CAST(substr(draw_term, 4, 2) AS INTEGER) * 30) + 
                    CAST(substr(draw_term, 6, 2) AS INTEGER)
                ) - 
                (
                    (CAST(substr(prev_appearance, 1, 3) AS INTEGER) * 365) + 
                    (CAST(substr(prev_appearance, 4, 2) AS INTEGER) * 30) + 
                    CAST(substr(prev_appearance, 6, 2) AS INTEGER)
                ) AS INTEGER
            )) as avg_missing_periods
        FROM MissingPeriods
        WHERE prev_appearance IS NOT NULL
        GROUP BY number
        ORDER BY max_missing_periods DESC
        LIMIT 10
        """
        
        # 3. 遺漏值模式分析
        query_patterns = f"""
        WITH RECURSIVE 
        NumberPatterns AS (
            SELECT 
                draw_term,
                GROUP_CONCAT(
                    CASE 
                        WHEN value IN (
                            SELECT number 
                            FROM (
                                SELECT value as number, MAX(draw_term) as last_draw
                                FROM {lottery_type},
                                json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
                                WHERE draw_term < t1.draw_term {period_filter}
                                GROUP BY value
                                HAVING julianday(t1.draw_term) - julianday(last_draw) >= 10
                            )
                        ) THEN 'L'
                        ELSE 'N'
                    END
                ) as pattern
            FROM {lottery_type} t1,
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY draw_term
        )
        SELECT 
            pattern,
            COUNT(*) as frequency,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM NumberPatterns), 2) as percentage
        FROM NumberPatterns
        GROUP BY pattern
        ORDER BY frequency DESC
        LIMIT 10
        """
        
        # 4. 遺漏值統計
        query_stats = f"""
        WITH NumberSequence AS (
            SELECT 
                draw_term,
                value as number,
                LAG(draw_term) OVER (PARTITION BY value ORDER BY draw_term) as prev_draw_term
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
        ),
        MissingStats AS (
            SELECT 
                number,
                COUNT(*) as appearance_count,
                MAX(missing_periods) as max_missing,
                AVG(missing_periods) as avg_missing,
                MIN(missing_periods) as min_missing
            FROM (
                SELECT 
                    number,
                    draw_term,
                    CAST(
                        (
                            (CAST(substr(draw_term, 1, 3) AS INTEGER) * 365) + 
                            (CAST(substr(draw_term, 4, 2) AS INTEGER) * 30) + 
                            CAST(substr(draw_term, 6, 2) AS INTEGER)
                        ) - 
                        (
                            (CAST(substr(prev_draw_term, 1, 3) AS INTEGER) * 365) + 
                            (CAST(substr(prev_draw_term, 4, 2) AS INTEGER) * 30) + 
                            CAST(substr(prev_draw_term, 6, 2) AS INTEGER)
                        ) AS INTEGER
                    ) as missing_periods
                FROM NumberSequence
                WHERE prev_draw_term IS NOT NULL
            )
            GROUP BY number
        )
        SELECT 
            number,
            appearance_count,
            ROUND(max_missing, 0) as max_missing_periods,
            ROUND(avg_missing, 1) as avg_missing_periods,
            min_missing as min_missing_periods
        FROM MissingStats
        ORDER BY avg_missing_periods DESC
        LIMIT 10
        """
        
        # 執行查詢
        current_missing = pd.read_sql_query(query_current, conn)
        historical_missing = pd.read_sql_query(query_historical, conn)
        missing_patterns = pd.read_sql_query(query_patterns, conn)
        missing_statistics = pd.read_sql_query(query_stats, conn)
        
        # 分析說明
        analysis_description = {
            '當前遺漏值': '''分析每個號碼目前的遺漏情況：
            - 最後出現期數：號碼最後一次開出的期數
            - 總出現次數：號碼在分析期間內出現的總次數
            - 遺漏期數：目前已經遺漏的期數
            這可以幫助我們找出長期未開出的號碼''',
            
            '歷史遺漏值': '''分析號碼的歷史遺漏規律：
            - 最大遺漏期數：號碼曾經最長連續未開出的期數
            - 平均遺漏期數：號碼平均每次遺漏的期數
            這可以幫助我們了解號碼的出現週期''',
            
            '遺漏值模式': '''分析遺漏值的組合模式：
            - L: 表示該號碼遺漏超過10期
            - N: 表示該號碼最近有開出
            這可以幫助我們發現遺漏值的組合規律''',
            
            '遺漏值統計': '''綜合統計號碼的遺漏特性：
            - 出現次數：號碼在分析期間內的出現總次數
            - 最大/平均/最小遺漏期數：完整的遺漏期數統計
            這可以幫助我們全面了解號碼的遺漏特性'''
        }
        
        conn.close()
        
        return {
            'current_missing': current_missing.to_dict('records'),
            'historical_missing': historical_missing.to_dict('records'),
            'missing_patterns': missing_patterns.to_dict('records'),
            'missing_statistics': missing_statistics.to_dict('records'),
            'analysis_description': analysis_description
        } 
    
    def network_analysis(self, lottery_type='big_lotto', periods=None):
        """號碼關聯網絡分析
        
        Args:
            lottery_type (str): 樂透類型 ('big_lotto', 'super_lotto', 'daily_cash')
            periods (int, optional): 要分析的期數，None 表示分析所有資料
            
        Returns:
            dict: 包含以下分析結果：
            - nodes: 節點資訊（號碼及其特性）
            - links: 連結資訊（號碼間的關聯）
            - centrality: 中心性分析
            - communities: 社群分析
        """
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 1. 節點分析（號碼出現頻率和特性）
        query_nodes = f"""
        WITH NumberStats AS (
            SELECT 
                value as number,
                COUNT(*) as frequency,
                COUNT(*) * 100.0 / (
                    SELECT COUNT(DISTINCT draw_term) 
                    FROM {lottery_type}
                    WHERE 1=1 {period_filter}
                ) as occurrence_rate,
                AVG(CASE WHEN value <= {25 if lottery_type != 'daily_cash' else 20} THEN 1 ELSE 0 END) as small_rate,
                AVG(CASE WHEN value % 2 = 1 THEN 1 ELSE 0 END) as odd_rate
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY value
        )
        SELECT 
            number,
            frequency,
            ROUND(occurrence_rate, 2) as occurrence_rate,
            ROUND(small_rate * 100, 2) as small_number_rate,
            ROUND(odd_rate * 100, 2) as odd_number_rate
        FROM NumberStats
        ORDER BY frequency DESC
        """
        
        # 2. 連結分析（號碼間的共同出現關係）
        query_links = f"""
        WITH PairCounts AS (
            SELECT 
                n1.value as source,
                n2.value as target,
                COUNT(*) as weight,
                COUNT(*) * 100.0 / (
                    SELECT COUNT(DISTINCT draw_term) 
                    FROM {lottery_type}
                    WHERE 1=1 {period_filter}
                ) as correlation_strength
            FROM {lottery_type} t1
            JOIN json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''})) n1
            JOIN json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''})) n2
            WHERE 1=1 {period_filter}
            AND n1.value < n2.value
            GROUP BY n1.value, n2.value
            HAVING weight >= 3
        )
        SELECT 
            source,
            target,
            weight,
            ROUND(correlation_strength, 2) as strength
        FROM PairCounts
        ORDER BY weight DESC
        """
        
        # 3. 中心性分析
        query_centrality = f"""
        WITH NodeConnections AS (
            SELECT 
                value as number,
                COUNT(DISTINCT draw_term) as degree,
                AVG(
                    CASE 
                        WHEN value IN (
                            SELECT value
                            FROM json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
                            WHERE draw_term = t1.draw_term
                        ) THEN 1 
                        ELSE 0 
                    END
                ) as betweenness
            FROM {lottery_type} t1,
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY value
        )
        SELECT 
            number,
            degree as degree_centrality,
            ROUND(betweenness * 100, 2) as betweenness_centrality,
            ROUND(degree * betweenness, 2) as eigenvector_centrality
        FROM NodeConnections
        ORDER BY eigenvector_centrality DESC
        LIMIT 10
        """
        
        # 4. 社群分析
        query_communities = f"""
        WITH NumberGroups AS (
            SELECT 
                draw_term,
                GROUP_CONCAT(value) as number_group
            FROM (
                SELECT 
                    draw_term,
                    value,
                    ROW_NUMBER() OVER (PARTITION BY draw_term ORDER BY value) as position
                FROM {lottery_type},
                json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
                WHERE 1=1 {period_filter}
            )
            GROUP BY draw_term
        ),
        GroupPatterns AS (
            SELECT 
                number_group,
                COUNT(*) as frequency,
                COUNT(*) * 100.0 / (
                    SELECT COUNT(*) FROM NumberGroups
                ) as occurrence_rate
            FROM NumberGroups
            GROUP BY number_group
            HAVING frequency >= 2
        )
        SELECT 
            number_group as community_numbers,
            frequency as community_size,
            ROUND(occurrence_rate, 2) as community_strength
        FROM GroupPatterns
        ORDER BY frequency DESC
        LIMIT 10
        """
        
        # 執行查詢
        nodes = pd.read_sql_query(query_nodes, conn)
        links = pd.read_sql_query(query_links, conn)
        centrality = pd.read_sql_query(query_centrality, conn)
        communities = pd.read_sql_query(query_communities, conn)
        
        # 分析說明
        analysis_description = {
            '節點分析': '''分析每個號碼的特性：
            - 出現頻率：號碼在分析期間內的出現次數
            - 出現率：號碼出現的機率
            - 大小號比例：號碼為小號的比例
            - 奇偶比例：號碼為奇數的比例
            這可以幫助我們了解每個號碼的基本特性''',
            
            '連結分析': '''分析號碼之間的關聯關係：
            - 連結權重：兩個號碼共同出現的次數
            - 關聯強度：兩個號碼的關聯程度
            這可以幫助我們找出經常一起出現的號碼組合''',
            
            '中心性分析': '''分析號碼在網絡中的重要性：
            - 度中心性：與其他號碼的直接連接數量
            - 中介中心性：號碼作為橋樑連接其他號碼的程度
            - 特徵向量中心性：綜合考慮號碼的整體重要性
            這可以幫助我們找出關鍵號碼''',
            
            '社群分析': '''分析號碼的群聚現象：
            - 社群號碼：經常一起出現的號碼組合
            - 社群大小：社群出現的次數
            - 社群強度：社群出現的機率
            這可以幫助我們發現穩定的號碼組合'''
        }
        
        conn.close()
        
        return {
            'nodes': nodes.to_dict('records'),
            'links': links.to_dict('records'),
            'centrality': centrality.to_dict('records'),
            'communities': communities.to_dict('records'),
            'analysis_description': analysis_description
        }
    
    def probability_distribution_analysis(self, lottery_type='big_lotto', periods=None):
        """機率分布分析
        
        Args:
            lottery_type (str): 樂透類型 ('big_lotto', 'super_lotto', 'daily_cash')
            periods (int, optional): 要分析的期數，None 表示分析所有資料
            
        Returns:
            dict: 包含以下分析結果：
            - empirical_distribution: 經驗分布分析
            - theoretical_comparison: 與理論分布比較
            - joint_distribution: 聯合機率分布
            - conditional_probability: 條件機率分析
        """
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 1. 經驗分布分析
        query_empirical = f"""
        WITH NumberCounts AS (
            SELECT 
                value as number,
                COUNT(*) as frequency,
                COUNT(*) * 100.0 / (
                    SELECT COUNT(DISTINCT draw_term) 
                    FROM {lottery_type}
                    WHERE 1=1 {period_filter}
                ) as empirical_prob
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY value
        )
        SELECT 
            number,
            frequency,
            ROUND(empirical_prob, 2) as empirical_probability,
            ROUND(
                ABS(empirical_prob - (100.0 / {49 if lottery_type != 'daily_cash' else 39})), 
                2
            ) as deviation_from_uniform
        FROM NumberCounts
        ORDER BY deviation_from_uniform DESC
        """
        
        # 2. 與理論分布比較
        query_theoretical = f"""
        WITH DrawStats AS (
            SELECT 
                draw_term,
                COUNT(*) as numbers_drawn,
                (
                    SELECT COUNT(DISTINCT value)
                    FROM {lottery_type},
                    json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
                    WHERE draw_term <= t1.draw_term {period_filter}
                ) as unique_numbers,
                MAX(value) - MIN(value) as range_size
            FROM {lottery_type} t1,
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY draw_term
        )
        SELECT 
            ROUND(AVG(numbers_drawn), 2) as avg_numbers_per_draw,
            ROUND(AVG(unique_numbers), 2) as avg_unique_numbers,
            ROUND(AVG(range_size), 2) as avg_range_size,
            ROUND(
                (
                    SELECT COUNT(*)
                    FROM DrawStats
                    WHERE numbers_drawn > (SELECT AVG(numbers_drawn) FROM DrawStats)
                ) * 100.0 / COUNT(*),
                2
            ) as above_mean_percentage
        FROM DrawStats
        """
        
        # 3. 聯合機率分布
        query_joint = f"""
        WITH PairProbs AS (
            SELECT 
                n1.value as num1,
                n2.value as num2,
                COUNT(*) as joint_frequency,
                COUNT(*) * 100.0 / (
                    SELECT COUNT(DISTINCT draw_term) 
                    FROM {lottery_type}
                    WHERE 1=1 {period_filter}
                ) as joint_prob
            FROM {lottery_type} t1
            JOIN json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''})) n1
            JOIN json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''})) n2
            WHERE 1=1 {period_filter}
            AND n1.value < n2.value
            GROUP BY n1.value, n2.value
            HAVING joint_frequency >= 3
        )
        SELECT 
            num1 || '-' || num2 as number_pair,
            joint_frequency,
            ROUND(joint_prob, 2) as joint_probability,
            ROUND(
                joint_prob / (
                    SELECT COUNT(*) * 100.0 / (
                        SELECT COUNT(DISTINCT draw_term) 
                        FROM {lottery_type}
                        WHERE 1=1 {period_filter}
                    )
                    FROM {lottery_type},
                    json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
                    WHERE value IN (num1, num2)
                    AND 1=1 {period_filter}
                ),
                2
            ) as dependency_ratio
        FROM PairProbs
        ORDER BY joint_probability DESC
        LIMIT 10
        """
        
        # 4. 條件機率分析
        query_conditional = f"""
        WITH NumberSequence AS (
            SELECT 
                draw_term,
                value as number,
                LAG(value) OVER (PARTITION BY value ORDER BY draw_term) as prev_number
            FROM {lottery_type},
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
        ),
        ConditionalProbs AS (
            SELECT 
                number,
                COUNT(*) as frequency,
                SUM(CASE WHEN prev_number IS NOT NULL THEN 1 ELSE 0 END) as conditional_count,
                COUNT(*) * 100.0 / (
                    SELECT COUNT(DISTINCT draw_term) 
                    FROM {lottery_type}
                    WHERE 1=1 {period_filter}
                ) as marginal_prob,
                SUM(CASE WHEN prev_number IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / 
                NULLIF(COUNT(prev_number), 0) as conditional_prob
            FROM NumberSequence
            GROUP BY number
        )
        SELECT 
            number,
            frequency,
            ROUND(marginal_prob, 2) as marginal_probability,
            ROUND(conditional_prob, 2) as conditional_probability,
            ROUND(
                CASE 
                    WHEN marginal_prob > 0 
                    THEN (conditional_prob - marginal_prob) / marginal_prob * 100 
                    ELSE 0 
                END,
                2
            ) as probability_lift
        FROM ConditionalProbs
        WHERE conditional_count >= 3
        ORDER BY probability_lift DESC
        LIMIT 10
        """
        
        # 執行查詢
        empirical = pd.read_sql_query(query_empirical, conn)
        theoretical = pd.read_sql_query(query_theoretical, conn)
        joint = pd.read_sql_query(query_joint, conn)
        conditional = pd.read_sql_query(query_conditional, conn)
        
        # 分析說明
        analysis_description = {
            '經驗分布分析': '''分析實際開出的號碼分布：
            - 出現頻率：號碼在分析期間內的出現次數
            - 經驗機率：實際觀察到的出現機率
            - 與均勻分布的偏差：實際機率與理論機率的差異
            這可以幫助我們了解號碼的實際分布情況''',
            
            '理論分布比較': '''將實際分布與理論分布進行比較：
            - 每期平均號碼數：實際開出的平均號碼數量
            - 平均不重複號碼數：不同號碼的平均數量
            - 平均範圍大小：號碼的平均分布範圍
            - 高於平均值比例：超過平均值的比例
            這可以幫助我們發現是否存在系統性偏差''',
            
            '聯合機率分布': '''分析號碼之間的聯合出現機率：
            - 號碼對：一起出現的兩個號碼
            - 聯合頻率：一起出現的次數
            - 聯合機率：一起出現的機率
            - 依賴比率：實際聯合機率與獨立情況下的理論機率之比
            這可以幫助我們了解號碼之間的相互關係''',
            
            '條件機率分析': '''分析號碼的條件機率：
            - 邊際機率：號碼單獨出現的機率
            - 條件機率：在特定條件下出現的機率
            - 機率提升度：條件機率相對於邊際機率的提升程度
            這可以幫助我們了解號碼出現的條件依賴關係'''
        }
        
        conn.close()
        
        return {
            'empirical_distribution': empirical.to_dict('records'),
            'theoretical_comparison': theoretical.to_dict('records')[0],  # 只有一行數據
            'joint_distribution': joint.to_dict('records'),
            'conditional_probability': conditional.to_dict('records'),
            'analysis_description': analysis_description
        }
    
    def recommend_from_hot_cold(self, lottery_type='big_lotto', periods=None):
        """基於冷熱門號碼分析的號碼推薦系統"""
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 根據彩券類型設置參數
        if lottery_type == 'daily_cash':
            numbers_per_draw = 5  # 今彩539只需要5個號碼
            max_hot_numbers = 3   # 熱門號碼數量
            max_cold_numbers = 2  # 冷門號碼數量
        else:
            numbers_per_draw = 6  # 大樂透和威力彩需要6個號碼
            max_hot_numbers = 4   # 熱門號碼數量
            max_cold_numbers = 2  # 冷門號碼數量
        
        # 1. 綜合分析查詢
        query = f"""
        WITH NumberAnalysis AS (
            SELECT 
                value as number,
                COUNT(*) as frequency,
                MAX(draw_term) as last_appearance,
                (
                    SELECT COUNT(DISTINCT draw_term)
                    FROM {lottery_type}
                    WHERE draw_term > MAX(t1.draw_term)
                    {period_filter}
                ) as missing_draws,
                COUNT(*) * 100.0 / (
                    SELECT COUNT(DISTINCT draw_term) 
                    FROM {lottery_type}
                    WHERE 1=1 {period_filter}
                ) as hit_rate,
                CASE 
                    WHEN COUNT(*) > (
                        SELECT AVG(cnt)
                        FROM (
                            SELECT COUNT(*) as cnt
                            FROM {lottery_type},
                            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
                            WHERE 1=1 {period_filter}
                            GROUP BY value
                        )
                    ) THEN 'H'  -- Hot
                    ELSE 'C'    -- Cold
                END as number_type
            FROM {lottery_type} t1,
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY value
        )
        SELECT 
            number,
            frequency,
            last_appearance,
            missing_draws,
            ROUND(hit_rate, 2) as hit_rate,
            number_type,
            ROUND(
                (hit_rate * 0.4) + 
                (CASE WHEN missing_draws > 0 
                    THEN (1.0 / missing_draws) * 100 
                    ELSE 100 
                END * 0.6),
                2
            ) as recommendation_score
        FROM NumberAnalysis
        ORDER BY recommendation_score DESC
        """
        
        # 執行查詢
        df = pd.read_sql_query(query, conn)
        
        # 2. 生成推薦組合
        hot_numbers = df[df['number_type'] == 'H']['number'].tolist()
        cold_numbers = df[df['number_type'] == 'C']['number'].tolist()
        
        # 修改推薦組合生成部分
        recommendations = {
            'balanced': [],    # 平衡型
            'aggressive': [],  # 進取型
            'conservative': [] # 保守型
        }
        
        # 根據彩券類型調整推薦組合
        if lottery_type == 'daily_cash':
            # 今彩539的推薦組合
            for _ in range(5):
                # 平衡型選號（3熱門 + 2冷門）
                balanced = (
                    random.sample(hot_numbers[:8], 3) + 
                    random.sample(cold_numbers[:8], 2)
                )
                balanced.sort()
                recommendations['balanced'].append(balanced)
                
                # 進取型選號（4熱門 + 1冷門）
                aggressive = (
                    random.sample(hot_numbers[:8], 4) + 
                    random.sample(cold_numbers[:8], 1)
                )
                aggressive.sort()
                recommendations['aggressive'].append(aggressive)
                
                # 保守型選號（2熱門 + 3冷門）
                conservative = (
                    random.sample(hot_numbers[:8], 2) + 
                    random.sample(cold_numbers[:8], 3)
                )
                conservative.sort()
                recommendations['conservative'].append(conservative)
        else:
            # 大樂透和威力彩的推薦組合
            for _ in range(5):
                # 平衡型選號（3熱門 + 3冷門）
                balanced = (
                    random.sample(hot_numbers[:10], 3) + 
                    random.sample(cold_numbers[:10], 3)
                )
                balanced.sort()
                recommendations['balanced'].append(balanced)
                
                # 進取型選號（4熱門 + 2冷門）
                aggressive = (
                    random.sample(hot_numbers[:8], 4) + 
                    random.sample(cold_numbers[:8], 2)
                )
                aggressive.sort()
                recommendations['aggressive'].append(aggressive)
                
                # 保守型選號（2熱門 + 4冷門）
                conservative = (
                    random.sample(hot_numbers[:8], 2) + 
                    random.sample(cold_numbers[:8], 4)
                )
                conservative.sort()
                recommendations['conservative'].append(conservative)
        
        # 3. 產生分析摘要
        analysis_description = {
            '選號策略': '''根據號碼的冷熱分析進行推薦。
            
            熱門號碼：近期開出頻率高的號碼
            冷門號碼：近期開出頻率低或遺漏期數多的號碼''',
            
            '推薦組合': f'''提供三種不同風格的選號組合：
            
            平衡型：{3 if lottery_type == "daily_cash" else 3}熱門 + {2 if lottery_type == "daily_cash" else 3}冷門，適合一般玩家
            進取型：{4 if lottery_type == "daily_cash" else 4}熱門 + {1 if lottery_type == "daily_cash" else 2}冷門，追求高命中率
            保守型：{2 if lottery_type == "daily_cash" else 2}熱門 + {3 if lottery_type == "daily_cash" else 4}冷門，期待遺漏值回補''',
            
            '使用建議': '''建議根據個人風格選擇合適的組合類型。
            
            參考依據：
            <strong style="color: #dc3545">【推薦分數】</strong>
            - 綜合考慮號碼的熱冷程度和遺漏值計算出的綜合評分
            - 分數越高表示越推薦選擇
            - 計算公式：命中率 × 40% + 遺漏值權重 × 60%
            
            <strong style="color: #0d6efd">【歷史表現】</strong>
            - 頻率：分析期間內出現次數
            - 命中率：出現次數占總期數的百分比
            - 最後開出：最近一次開出的期數
            
            <strong style="color: #198754">【遺漏情況】</strong>
            - 遺漏期數：該號碼已經多少期沒出現
            - 越久沒開出的號碼，遺漏期數越大
            - 可用來判斷號碼是否"該出現了"
            
            溫馨提醒：
            記得理性購買，適度娛樂。'''
        }
        
        conn.close()
        
        return {
            'balanced_numbers': recommendations['balanced'],
            'aggressive_numbers': recommendations['aggressive'],
            'conservative_numbers': recommendations['conservative'],
            'number_analysis': df.to_dict('records'),
            'analysis_description': analysis_description
        }
    
    def recommend_by_method(self, lottery_type='big_lotto', method='hot_cold', periods=None):
        """根據不同方法進行號碼推薦"""
        try:
            if method == 'hot_cold':
                return self.recommend_from_hot_cold(lottery_type, periods)
            elif method == 'interval':
                return self.recommend_from_interval(lottery_type, periods)
            elif method == 'odd_even':
                return self.recommend_from_odd_even(lottery_type, periods)
            elif method == 'sequence':
                return self.recommend_from_sequence(lottery_type, periods)
            elif method == 'cycle':
                return self.recommend_from_cycle(lottery_type, periods)
            else:
                raise ValueError(f"不支援的推薦方法: {method}")
        except Exception as e:
            print(f"推薦過程發生錯誤: {str(e)}")
            raise
    
    def recommend_from_interval(self, lottery_type='big_lotto', periods=None):
        """區間平衡法推薦"""
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 設定彩種參數
        if lottery_type == 'daily_cash':
            max_number = 39
            numbers_per_draw = 5
            interval_count = 5  # 分成5個區間
        else:
            max_number = 49
            numbers_per_draw = 6
            interval_count = 6  # 分成6個區間
        
        # 計算每個區間的範圍
        interval_size = max_number // interval_count
        intervals = [(i * interval_size + 1, (i + 1) * interval_size) for i in range(interval_count)]
        if max_number % interval_count != 0:
            intervals[-1] = (intervals[-1][0], max_number)
        
        # 分析每個區間的號碼分布
        query = f"""
        WITH NumberAnalysis AS (
            SELECT 
                value as number,
                COUNT(*) as frequency,
                MAX(draw_term) as last_appearance,
                (
                    SELECT COUNT(DISTINCT draw_term)
                    FROM {lottery_type}
                    WHERE draw_term > MAX(t1.draw_term)
                    {period_filter}
                ) as missing_draws,
                COUNT(*) * 100.0 / (
                    SELECT COUNT(DISTINCT draw_term) 
                    FROM {lottery_type}
                    WHERE 1=1 {period_filter}
                ) as hit_rate,
                CASE 
                    WHEN value <= {interval_size} THEN 'I1'
                    WHEN value <= {interval_size * 2} THEN 'I2'
                    WHEN value <= {interval_size * 3} THEN 'I3'
                    WHEN value <= {interval_size * 4} THEN 'I4'
                    WHEN value <= {interval_size * 5} THEN 'I5'
                    ELSE 'I6'
                END as number_type
            FROM {lottery_type} t1,
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY value
        )
        SELECT 
            number,
            frequency,
            last_appearance,
            missing_draws,
            ROUND(hit_rate, 2) as hit_rate,
            number_type,
            ROUND(
                (hit_rate * 0.4) + 
                (CASE WHEN missing_draws > 0 
                    THEN (1.0 / missing_draws) * 100 
                    ELSE 100 
                END * 0.6),
                2
            ) as recommendation_score
        FROM NumberAnalysis
        ORDER BY frequency DESC
        """
        
        df = pd.read_sql_query(query, conn)
        
        # 生成推薦組合
        recommendations = {
            'balanced': [],    # 平衡型：每個區間選1個
            'aggressive': [],  # 進取型：高頻區間多選
            'conservative': [] # 保守型：低頻區間多選
        }
        
        for _ in range(5):  # 生成5組推薦
            # 平衡型
            balanced = []
            for start, end in intervals:
                interval_numbers = df[
                    (df['number'] >= start) & 
                    (df['number'] <= end)
                ]['number'].tolist()
                if interval_numbers:
                    balanced.append(random.choice(interval_numbers))
            
            # 如果號碼不足，從其他區間補充
            while len(balanced) < numbers_per_draw:
                num = random.randint(1, max_number)
                if num not in balanced:
                    balanced.append(num)
            
            balanced.sort()
            recommendations['balanced'].append(balanced[:numbers_per_draw])
            
            # 進取型：偏好高頻率區間
            aggressive = []
            high_freq_intervals = sorted(intervals, 
                key=lambda x: df[
                    (df['number'] >= x[0]) & 
                    (df['number'] <= x[1])
                ]['frequency'].mean(), 
                reverse=True
            )
            
            for start, end in high_freq_intervals[:numbers_per_draw]:
                interval_numbers = df[
                    (df['number'] >= start) & 
                    (df['number'] <= end)
                ]['number'].tolist()
                if interval_numbers:
                    aggressive.append(random.choice(interval_numbers))
            
            # 補充不足的號碼
            while len(aggressive) < numbers_per_draw:
                num = random.randint(1, max_number)
                if num not in aggressive:
                    aggressive.append(num)
            
            aggressive.sort()
            recommendations['aggressive'].append(aggressive)
            
            # 保守型：偏好低頻率區間
            conservative = []
            low_freq_intervals = sorted(intervals, 
                key=lambda x: df[
                    (df['number'] >= x[0]) & 
                    (df['number'] <= x[1])
                ]['frequency'].mean()
            )
            
            for start, end in low_freq_intervals[:numbers_per_draw]:
                interval_numbers = df[
                    (df['number'] >= start) & 
                    (df['number'] <= end)
                ]['number'].tolist()
                if interval_numbers:
                    conservative.append(random.choice(interval_numbers))
            
            # 補充不足的號碼
            while len(conservative) < numbers_per_draw:
                num = random.randint(1, max_number)
                if num not in conservative:
                    conservative.append(num)
            
            conservative.sort()
            recommendations['conservative'].append(conservative)
        
        # 生成分析說明
        analysis_description = {
            '選號策略': '''根據號碼區間分布進行推薦。
            
            將號碼範圍分成幾個區間，分析每個區間的開出機率''',
            
            '推薦組合': f'''提供三種不同風格的選號組合：
            
            平衡型：每個區間平均選擇，追求均衡
            進取型：偏好高頻率區間，追求熱門
            保守型：偏好低頻率區間，期待遺漏回補''',
            
            '使用建議': '''建議根據個人風格選擇合適的組合類型。
            
            參考依據：
            <strong style="color: #dc3545">【區間分布】</strong>
            - 頻率分布：每個區間的號碼開出頻率
            - 區間比例：各區間號碼的占比情況
            - 區間範圍：號碼所在的區間位置
            
            <strong style="color: #0d6efd">【開出規律】</strong>
            - 熱門區間：開出頻率較高的號碼區間
            - 冷門區間：開出頻率較低的號碼區間
            - 區間組合：不同區間的搭配模式
            
            <strong style="color: #198754">【選號建議】</strong>
            - 平衡型：建議每個區間都選擇號碼
            - 進取型：可以偏重熱門區間的號碼
            - 保守型：可以關注冷門區間的號碼
            
            溫馨提醒：
            記得理性購買，適度娛樂。'''
        }
        
        conn.close()
        
        return {
            'balanced_numbers': recommendations['balanced'],
            'aggressive_numbers': recommendations['aggressive'],
            'conservative_numbers': recommendations['conservative'],
            'number_analysis': df.to_dict('records'),
            'analysis_description': analysis_description
        }
    
    def recommend_from_odd_even(self, lottery_type='big_lotto', periods=None):
        """奇偶均衡法推薦"""
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 設定彩種參數
        if lottery_type == 'daily_cash':
            max_number = 39
            numbers_per_draw = 5
        else:
            max_number = 49
            numbers_per_draw = 6
        
        # 分析號碼分布和奇偶比例
        query = f"""
        WITH NumberAnalysis AS (
            SELECT 
                value as number,
                COUNT(*) as frequency,
                MAX(draw_term) as last_appearance,
                (
                    SELECT COUNT(DISTINCT draw_term)
                    FROM {lottery_type}
                    WHERE draw_term > MAX(t1.draw_term)
                    {period_filter}
                ) as missing_draws,
                COUNT(*) * 100.0 / (
                    SELECT COUNT(DISTINCT draw_term) 
                    FROM {lottery_type}
                    WHERE 1=1 {period_filter}
                ) as hit_rate,
                CASE 
                    WHEN value % 2 = 0 THEN 'E'  -- Even (偶數)
                    ELSE 'O'                      -- Odd (奇數)
                END as number_type
            FROM {lottery_type} t1,
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
            GROUP BY value
        )
        SELECT 
            number,
            frequency,
            last_appearance,
            missing_draws,
            ROUND(hit_rate, 2) as hit_rate,
            number_type,
            ROUND(
                (hit_rate * 0.4) + 
                (CASE WHEN missing_draws > 0 
                    THEN (1.0 / missing_draws) * 100 
                    ELSE 100 
                END * 0.6),
                2
            ) as recommendation_score
        FROM NumberAnalysis
        ORDER BY recommendation_score DESC
        """
        
        df = pd.read_sql_query(query, conn)
        
        # 計算最佳奇偶比例
        odd_numbers = df[df['number_type'] == 'O']['number'].tolist()
        even_numbers = df[df['number_type'] == 'E']['number'].tolist()
        
        # 生成推薦組合
        recommendations = {
            'balanced': [],    # 平衡型：奇偶數量相近
            'aggressive': [],  # 進取型：偏好高頻率的奇偶組合
            'conservative': [] # 保守型：偏好低頻率的奇偶組合
        }
        
        for _ in range(5):  # 生成5組推薦
            # 平衡型
            odd_count = numbers_per_draw // 2
            even_count = numbers_per_draw - odd_count
            
            balanced = (
                random.sample(sorted(odd_numbers, key=lambda x: df[df['number'] == x]['recommendation_score'].iloc[0], reverse=True)[:10], odd_count) +
                random.sample(sorted(even_numbers, key=lambda x: df[df['number'] == x]['recommendation_score'].iloc[0], reverse=True)[:10], even_count)
            )
            balanced.sort()
            recommendations['balanced'].append(balanced)
            
            # 進取型：偏好高推薦分數的號碼
            aggressive_odd = sorted(odd_numbers, key=lambda x: df[df['number'] == x]['recommendation_score'].iloc[0], reverse=True)[:8]
            aggressive_even = sorted(even_numbers, key=lambda x: df[df['number'] == x]['recommendation_score'].iloc[0], reverse=True)[:8]
            
            aggressive = (
                random.sample(aggressive_odd, odd_count) +
                random.sample(aggressive_even, even_count)
            )
            aggressive.sort()
            recommendations['aggressive'].append(aggressive)
            
            # 保守型：偏好低頻率號碼
            conservative_odd = sorted(odd_numbers, key=lambda x: df[df['number'] == x]['frequency'].iloc[0])[:8]
            conservative_even = sorted(even_numbers, key=lambda x: df[df['number'] == x]['frequency'].iloc[0])[:8]
            
            conservative = (
                random.sample(conservative_odd, odd_count) +
                random.sample(conservative_even, even_count)
            )
            conservative.sort()
            recommendations['conservative'].append(conservative)
        
        # 生成分析說明
        analysis_description = {
            '選號策略': '''根據號碼的奇偶屬性進行分析和推薦。
            
            分析歷史開獎的奇偶比例，結合號碼的熱門程度''',
            
            '推薦組合': f'''提供三種不同風格的選號組合：
            
            平衡型：維持最佳奇偶比例，追求均衡
            進取型：選擇高推薦分數的奇偶組合
            保守型：選擇低頻率的奇偶組合''',
            
            '使用建議': '''建議根據個人風格選擇合適的組合類型。
            
            參考依據：
            <strong style="color: #dc3545">【奇偶比例】</strong>
            - 奇數比例：奇數號碼的開出比例
            - 偶數比例：偶數號碼的開出比例
            - 最佳配比：歷史數據中表現最好的奇偶比例
            
            <strong style="color: #0d6efd">【號碼表現】</strong>
            - 熱門號碼：開出頻率較高的奇/偶數
            - 冷門號碼：開出頻率較低的奇/偶數
            - 推薦分數：綜合評分較高的奇偶組合
            
            <strong style="color: #198754">【選號策略】</strong>
            - 平衡型：維持穩定的奇偶比例
            - 進取型：選擇高頻率的奇偶組合
            - 保守型：關注低頻率的奇偶組合
            
            溫馨提醒：
            記得理性購買，適度娛樂。'''
        }
        
        conn.close()
        
        return {
            'balanced_numbers': recommendations['balanced'],
            'aggressive_numbers': recommendations['aggressive'],
            'conservative_numbers': recommendations['conservative'],
            'number_analysis': df.to_dict('records'),
            'analysis_description': analysis_description
        }
    
    def recommend_from_sequence(self, lottery_type='big_lotto', periods=None):
        """連號分析法推薦"""
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 設定彩種參數
        if lottery_type == 'daily_cash':
            max_number = 39
            numbers_per_draw = 5
        else:
            max_number = 49
            numbers_per_draw = 6
        
        # 分析連號模式
        query = f"""
        WITH DrawSequences AS (
            SELECT 
                t1.draw_term,
                t1.draw_date,
                value as number,
                CASE 
                    WHEN value + 1 IN (
                        SELECT value 
                        FROM {lottery_type} t2,
                        json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
                        WHERE t2.draw_term = t1.draw_term
                    ) THEN 1
                    ELSE 0
                END as has_sequence,
                COUNT(*) OVER (PARTITION BY value) as frequency,
                MAX(t1.draw_term) OVER (PARTITION BY value) as last_appearance,
                COUNT(*) OVER (PARTITION BY value) * 100.0 / COUNT(*) OVER () as hit_rate
            FROM {lottery_type} t1,
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
        )
        SELECT 
            draw_term,
            draw_date,
            GROUP_CONCAT(number) as numbers,
            GROUP_CONCAT(has_sequence) as sequence_flags,
            SUM(has_sequence) as sequence_count,
            number,
            frequency,
            last_appearance,
            ROUND(hit_rate, 2) as hit_rate
        FROM DrawSequences
        GROUP BY draw_term, draw_date
        ORDER BY draw_term DESC
        """
        
        df = pd.read_sql_query(query, conn)
        
        # 計算每個號碼的連號出現次數
        number_stats = {}
        for _, row in df.iterrows():
            numbers = [int(n) for n in row['numbers'].split(',')]
            sequence_flags = [int(f) for f in row['sequence_flags'].split(',')]
            for num, flag in zip(numbers, sequence_flags):
                if num not in number_stats:
                    number_stats[num] = {'sequence_count': 0, 'total_count': 0}
                number_stats[num]['sequence_count'] += flag
                number_stats[num]['total_count'] += 1
        
        # 分類號碼
        sequence_numbers = [num for num, stats in number_stats.items() 
                           if stats['sequence_count'] / stats['total_count'] > 0.3]
        non_sequence_numbers = [num for num in range(1, max_number + 1) 
                              if num not in sequence_numbers]
        
        # 生成推薦組合
        recommendations = {
            'balanced': [],    # 平衡型：連號與非連號平衡
            'aggressive': [],  # 進取型：偏好連號
            'conservative': [] # 保守型：避免連號
        }
        
        for _ in range(5):
            # 平衡型：2個連號 + 其他非連號
            balanced = []
            if len(sequence_numbers) >= 2:
                balanced.extend(random.sample(sequence_numbers, 2))
            balanced.extend(random.sample(non_sequence_numbers, numbers_per_draw - len(balanced)))
            balanced.sort()
            recommendations['balanced'].append(balanced)
            
            # 進取型：偏好連號
            aggressive = []
            seq_count = min(4, len(sequence_numbers))
            if seq_count > 0:
                aggressive.extend(random.sample(sequence_numbers, seq_count))
            aggressive.extend(random.sample(non_sequence_numbers, numbers_per_draw - len(aggressive)))
            aggressive.sort()
            recommendations['aggressive'].append(aggressive)
            
            # 保守型：避免連號
            conservative = random.sample(non_sequence_numbers, numbers_per_draw)
            conservative.sort()
            recommendations['conservative'].append(conservative)
        
        # 生成分析說明
        analysis_description = {
            '選號策略': '''根據號碼的連號特性進行分析和推薦。
            
            分析歷史開獎的連號模式，評估連號出現的機率''',
            
            '推薦組合': f'''提供三種不同風格的選號組合：
            
            平衡型：適量連號，追求均衡
            進取型：偏好連號組合，追求熱門
            保守型：避免連號，期待分散''',
            
            '使用建議': '''建議根據個人風格選擇合適的組合類型。
            
            參考依據：
            <strong style="color: #dc3545">【連號特性】</strong>
            - 連號頻率：每期開出連號的機率
            - 連號數量：每期平均出現的連號組數
            - 連號位置：連號最常出現的位置
            
            <strong style="color: #0d6efd">【開出規律】</strong>
            - 連號組合：常見的連號組合模式
            - 連號間隔：連號之間的間隔規律
            - 連號趨勢：近期連號出現的趨勢
            
            <strong style="color: #198754">【選號建議】</strong>
            - 平衡型：適量選擇連號組合
            - 進取型：根據趨勢選擇連號
            - 保守型：避免過多連號
            
            溫馨提醒：
            記得理性購買，適度娛樂。'''
        }
        
        conn.close()
        
        # 準備分析結果
        draw_analysis = []
        for _, row in df.iterrows():
            draw_analysis.append({
                'draw_term': row['draw_term'],
                'draw_date': row['draw_date'],
                'numbers': row['numbers'],
                'sequence_count': row['sequence_count']
            })
        
        return {
            'balanced_numbers': recommendations['balanced'],
            'aggressive_numbers': recommendations['aggressive'],
            'conservative_numbers': recommendations['conservative'],
            'draw_analysis': draw_analysis,  # 改為回傳每期的分析結果
            'analysis_description': analysis_description
        }
    
    def recommend_from_cycle(self, lottery_type='big_lotto', periods=None):
        """週期回歸法推薦"""
        conn = self._get_connection()
        
        # 設定期數過濾條件
        if periods:
            period_filter = f"""
            AND draw_term IN (
                SELECT draw_term 
                FROM {lottery_type} 
                ORDER BY draw_term DESC 
                LIMIT {periods}
            )
            """
        else:
            period_filter = ""
        
        # 設定彩種參數
        if lottery_type == 'daily_cash':
            max_number = 39
            numbers_per_draw = 5
        else:
            max_number = 49
            numbers_per_draw = 6
        
        # 分析週期模式
        query = f"""
        WITH DrawHistory AS (
            SELECT 
                draw_term,
                draw_date,
                value as number
            FROM {lottery_type} t1,
            json_each(json_array(num1, num2, num3, num4, num5{', num6' if lottery_type != 'daily_cash' else ''}))
            WHERE 1=1 {period_filter}
        ),
        NumberStats AS (
            SELECT 
                number,
                COUNT(*) as frequency,
                MAX(draw_term) as last_appearance,
                -- 計算平均週期：總期數除以出現次數
                ROUND(CAST((
                    SELECT COUNT(DISTINCT draw_term) 
                    FROM {lottery_type}
                    WHERE 1=1 {period_filter}
                ) AS FLOAT) / COUNT(*), 1) as avg_cycle
            FROM DrawHistory
            GROUP BY number
        ),
        NumberCycles AS (
            SELECT 
                t1.number,
                MIN(t2.draw_term - t1.draw_term) as min_cycle,
                MAX(t2.draw_term - t1.draw_term) as max_cycle
            FROM DrawHistory t1
            JOIN DrawHistory t2 ON t1.number = t2.number AND t2.draw_term > t1.draw_term
            GROUP BY t1.number
        ),
        MissingDraws AS (
            SELECT 
                ns.*,
                (
                    SELECT COUNT(DISTINCT draw_term)
                    FROM {lottery_type}
                    WHERE draw_term > ns.last_appearance
                    {period_filter}
                ) as missing_draws
            FROM NumberStats ns
        )
        SELECT 
            md.number,
            md.avg_cycle,
            COALESCE(nc.min_cycle, 0) as min_cycle,
            COALESCE(nc.max_cycle, 0) as max_cycle,
            md.frequency,
            md.last_appearance,
            md.missing_draws,
            CASE 
                WHEN md.missing_draws >= ROUND(md.avg_cycle) THEN 'C1'  -- 已達週期
                WHEN md.missing_draws >= ROUND(md.avg_cycle * 0.7) THEN 'C2'  -- 接近週期
                ELSE 'C3'  -- 未達週期
            END as cycle_status
        FROM MissingDraws md
        LEFT JOIN NumberCycles nc ON md.number = nc.number
        ORDER BY 
            CASE cycle_status
                WHEN 'C1' THEN 1
                WHEN 'C2' THEN 2
                ELSE 3
            END,
            missing_draws DESC
        """
        
        df = pd.read_sql_query(query, conn)
        
        # 根據週期狀態分類號碼
        cycle_complete = df[df['cycle_status'] == 'C1']['number'].tolist()
        cycle_approaching = df[df['cycle_status'] == 'C2']['number'].tolist()
        cycle_ongoing = df[df['cycle_status'] == 'C3']['number'].tolist()
        
        # 生成推薦組合
        recommendations = {
            'balanced': [],    # 平衡型：混合不同週期狀態
            'aggressive': [],  # 進取型：偏好已達週期的號碼
            'conservative': [] # 保守型：偏好未達週期的號碼
        }
        
        for _ in range(5):  # 生成5組推薦
            # 平衡型：各週期狀態平均選擇
            balanced = []
            if cycle_complete:
                balanced.extend(random.sample(cycle_complete, min(2, len(cycle_complete))))
            if cycle_approaching:
                balanced.extend(random.sample(cycle_approaching, min(2, len(cycle_approaching))))
            remaining = numbers_per_draw - len(balanced)
            if remaining > 0:
                balanced.extend(random.sample(cycle_ongoing, remaining))
            balanced.sort()
            recommendations['balanced'].append(balanced)
            
            # 進取型：偏好已達週期和接近週期的號碼
            aggressive = []
            priority_numbers = cycle_complete + cycle_approaching
            if len(priority_numbers) >= numbers_per_draw:
                aggressive = random.sample(priority_numbers, numbers_per_draw)
            else:
                aggressive.extend(priority_numbers)
                aggressive.extend(random.sample(cycle_ongoing, numbers_per_draw - len(aggressive)))
            aggressive.sort()
            recommendations['aggressive'].append(aggressive)
            
            # 保守型：偏好未達週期的號碼
            conservative = []
            if len(cycle_ongoing) >= numbers_per_draw:
                conservative = random.sample(cycle_ongoing, numbers_per_draw)
            else:
                conservative.extend(random.sample(cycle_ongoing, len(cycle_ongoing)))
                conservative.extend(random.sample(cycle_approaching, numbers_per_draw - len(conservative)))
            conservative.sort()
            recommendations['conservative'].append(conservative)
        
        # 生成分析說明
        analysis_description = {
            '選號策略': '''根據號碼的週期性出現規律進行分析和推薦。
            
            分析每個號碼的平均出現週期，評估當前遺漏期數''',
            
            '推薦組合': f'''提供三種不同風格的選號組合：
            
            平衡型：混合不同週期狀態的號碼
            進取型：偏好已達週期的號碼
            保守型：偏好未達週期的號碼''',
            
            '使用建議': '''建議根據個人風格選擇合適的組合類型。
            
            參考依據：
            <strong style="color: #dc3545">【週期指標】</strong>
            - 平均週期：該號碼平均每隔幾期會出現一次
            - 最短週期：歷史上最快多少期就開出
            - 最長週期：歷史上最久多少期才開出
            
            <strong style="color: #0d6efd">【週期狀態】</strong>
            - 已達週期：遺漏期數已超過平均週期
            - 接近週期：遺漏期數達到平均週期的70%以上
            - 未達週期：遺漏期數低於平均週期的70%
            
            <strong style="color: #198754">【選號策略】</strong>
            - 平衡型：混合不同週期狀態的號碼
            - 進取型：優先選擇已達週期的號碼
            - 保守型：偏好未達週期的號碼
            
            溫馨提醒：
            記得理性購買，適度娛樂。'''
        }
        
        # 準備分析結果
        cycle_analysis = df.to_dict('records')
        
        return {
            'balanced_numbers': recommendations['balanced'],
            'aggressive_numbers': recommendations['aggressive'],
            'conservative_numbers': recommendations['conservative'],
            'cycle_analysis': cycle_analysis,
            'analysis_description': analysis_description
        }