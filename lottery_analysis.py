import sqlite3
from datetime import datetime
import pandas as pd
import numpy as np

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
        """
        conn = self._get_connection()
        
        # 如果指定了期數，需要先找出最近 N 期的期號
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
        
        # 修改所有查詢方法，加入期數過濾
        number_freq = self._get_number_frequency(conn, lottery_type, period_filter)
        special_freq = self._get_special_number_frequency(conn, lottery_type, period_filter)
        common_combs = self._get_common_combinations(conn, lottery_type, period_filter)
        cold_nums = self._get_cold_numbers(conn, lottery_type, period_filter)
        
        conn.close()
        
        return {
            'number_frequency': number_freq,
            'special_number_frequency': special_freq,
            'common_combinations': common_combs,
            'cold_numbers': cold_nums
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
        LIMIT 10
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