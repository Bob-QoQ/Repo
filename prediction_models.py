import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from datetime import datetime
import sqlite3
import logging

logger = logging.getLogger(__name__)

class LotteryPredictor:
    def __init__(self):
        self.rf_model = None
        self.scaler = StandardScaler()
        
    def prepare_data(self, lottery_type, periods=1000):
        """準備訓練資料"""
        try:
            conn = sqlite3.connect('lottery.db')
            table_name = {
                'big-lotto': 'big_lotto',
                'super-lotto': 'super_lotto',
                'daily-cash': 'daily_cash'
            }[lottery_type]
            
            # 修改 SQL 查詢以包含特別號
            query = f"""
                SELECT 
                    draw_term,
                    CASE 
                        WHEN draw_date LIKE '%/%/%' THEN 
                            CAST(substr(draw_date, 1, instr(draw_date, '/') - 1) AS INTEGER) + 1911 || 
                            '-' || 
                            substr(draw_date, instr(draw_date, '/') + 1, 2) || 
                            '-' || 
                            substr(draw_date, -2)
                        WHEN draw_date LIKE '%-%-%' THEN
                            CAST(substr(draw_date, 1, instr(draw_date, '-') - 1) AS INTEGER) + 1911 || 
                            '-' || 
                            substr(draw_date, instr(draw_date, '-') + 1, 2) || 
                            '-' || 
                            substr(draw_date, -2)
                        ELSE draw_date
                    END as draw_date,
                    num1, num2, num3, num4, num5
                    {', num6' if lottery_type != 'daily-cash' else ''}
                    {', special_num' if lottery_type == 'super-lotto' else ''}
                FROM {table_name}
                ORDER BY draw_term DESC
                LIMIT {periods}
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            # 確保日期格式正確
            df['draw_date'] = pd.to_datetime(df['draw_date'])
            
            # 特徵工程
            df['year'] = df['draw_date'].dt.year
            df['month'] = df['draw_date'].dt.month
            df['day'] = df['draw_date'].dt.day
            df['weekday'] = df['draw_date'].dt.weekday
            
            # 添加更多特徵
            df['year_mod'] = df['year'] % 10  # 年份的餘數
            df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)  # 月份的週期性
            df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
            df['day_sin'] = np.sin(2 * np.pi * df['day'] / 31)  # 日期的週期性
            df['day_cos'] = np.cos(2 * np.pi * df['day'] / 31)
            
            return df
            
        except Exception as e:
            logger.error(f'準備訓練資料時發生錯誤: {str(e)}', exc_info=True)
            raise Exception(f'準備訓練資料時發生錯誤: {str(e)}')
        
    def train_models(self, lottery_type, periods=1000):
        """訓練預測模型"""
        try:
            # 添加最小期數驗證
            if periods < 100:
                raise ValueError('訓練期數必須至少為100期才能獲得較好的預測效果')
            
            df = self.prepare_data(lottery_type, periods)
            
            # 檢查實際獲取的數據量
            if len(df) < 100:
                raise ValueError(f'可用數據量不足：僅有 {len(df)} 期，需要至少100期數據')
            
            # 準備特徵和標籤
            features = ['year_mod', 'month_sin', 'month_cos', 
                       'day_sin', 'day_cos', 'weekday']
            X = df[features].values
            
            # 根據彩種類型準備標籤
            if lottery_type == 'super-lotto':
                # 威力彩需要包含第二區特別號
                y = df[['num1', 'num2', 'num3', 'num4', 'num5', 'num6', 'special_num']].values
            elif lottery_type == 'big-lotto':
                y = df[['num1', 'num2', 'num3', 'num4', 'num5', 'num6']].values
            else:  # daily-cash
                y = df[['num1', 'num2', 'num3', 'num4', 'num5']].values
            
            # 資料分割
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
            
            # 特徵縮放
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # 訓練隨機森林模型
            self.rf_model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=42
            )
            self.rf_model.fit(X_train_scaled, y_train)
            
            score = self.rf_model.score(X_test_scaled, y_test)
            logger.info(f'模型訓練完成，準確度: {score}')
            
            return {
                'rf_score': score
            }
            
        except Exception as e:
            logger.error(f'訓練模型時發生錯誤: {str(e)}', exc_info=True)
            raise Exception(f'訓練模型時發生錯誤: {str(e)}')
    
    def predict_next_draw(self, lottery_type):
        """預測下一期號碼"""
        try:
            if self.rf_model is None:
                raise Exception('模型尚未訓練，請先訓練模型')
            
            today = datetime.now()
            
            # 準備特徵
            year_mod = today.year % 10
            month_sin = np.sin(2 * np.pi * today.month / 12)
            month_cos = np.cos(2 * np.pi * today.month / 12)
            day_sin = np.sin(2 * np.pi * today.day / 31)
            day_cos = np.cos(2 * np.pi * today.day / 31)
            weekday = today.weekday()
            
            features = np.array([[
                year_mod,
                month_sin,
                month_cos,
                day_sin,
                day_cos,
                weekday
            ]])
            
            # 特徵縮放
            features_scaled = self.scaler.transform(features)
            
            # 獲取預測結果
            predictions = self.rf_model.predict(features_scaled)
            
            # 根據彩種調整預測範圍
            if lottery_type == 'super-lotto':
                # 威力彩: 第一區 1-38，第二區 1-8
                numbers = []
                # 處理第一區 6 個號碼
                for pred in predictions[0][:6]:
                    num = max(1, min(38, int(round(pred))))
                    while num in numbers:  # 避免重複號碼
                        num = num + 1 if num < 38 else 1
                    numbers.append(num)
                # 生成第二區特別號 (1-8)
                special_num = max(1, min(8, int(round(predictions[0][6]))))
                numbers.append(special_num)
                
            elif lottery_type == 'big-lotto':
                # 大樂透: 1-49
                numbers = []
                for pred in predictions[0]:
                    num = max(1, min(49, int(round(pred))))
                    while num in numbers:
                        num = num + 1 if num < 49 else 1
                    numbers.append(num)
                
            else:  # daily-cash
                # 今彩539: 1-39
                numbers = []
                for pred in predictions[0][:5]:  # 只取前5個預測值
                    num = max(1, min(39, int(round(pred))))
                    while num in numbers:
                        num = num + 1 if num < 39 else 1
                    numbers.append(num)
            
            # 排序第一區號碼，特別號不參與排序
            if lottery_type == 'super-lotto':
                numbers = sorted(numbers[:6]) + [numbers[6]]
            else:
                numbers.sort()
            
            logger.info(f'預測完成，號碼: {numbers}')
            return numbers
            
        except Exception as e:
            logger.error(f'預測時發生錯誤: {str(e)}', exc_info=True)
            raise Exception(f'預測時發生錯誤: {str(e)}')

    def evaluate_prediction(self, lottery_type, actual_numbers):
        """評估預測準確度"""
        try:
            predicted = self.predict_next_draw(lottery_type)
            matches = len(set(predicted) & set(actual_numbers))
            accuracy = matches / len(actual_numbers)
            
            evaluation = {
                'predicted': predicted,
                'actual': actual_numbers,
                'matches': matches,
                'accuracy': accuracy
            }
            
            logger.info(f'預測評估結果: {evaluation}')
            return evaluation
            
        except Exception as e:
            logger.error(f'評估預測時發生錯誤: {str(e)}', exc_info=True)
            raise Exception(f'評估預測時發生錯誤: {str(e)}') 