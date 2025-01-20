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
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL 
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