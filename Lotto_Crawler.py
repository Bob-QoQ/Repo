import requests
import json
from datetime import datetime
import os

class LottoBase:
    def __init__(self, api='https://api.taiwanlottery.com/TLCAPIWeB/Lottery/Lotto649Result'):
        self.api = api
        self.default_dir = 'data'
        self.default_filename = 'Lotto.json'
        self.draws = {}

    def crawl(self, force_update=False):
        lastest_draw = self.getLastDraw()
        currentTime = datetime.now()

        print(f'最新一期: {lastest_draw["draw"] if lastest_draw else None}')

        if lastest_draw == None or force_update:
            ybegin, mbegin = (103, 1)
        else:
            ybegin = lastest_draw['year']
            mbegin = lastest_draw['month']
            
            current_month_data = self.crawlApi(ybegin, mbegin)
            if current_month_data:
                data = json.loads(current_month_data)
                content = data.get("content", {})
                latest_period = None
                
                if "lotto649Res" in content:
                    draws = content["lotto649Res"]
                elif "daily539Res" in content:
                    draws = content["daily539Res"]
                elif "superLotto638Res" in content:
                    draws = content["superLotto638Res"]
                
                if draws and len(draws) > 0:
                    latest_period = draws[0]["period"]
                    
                if latest_period and int(latest_period) <= int(lastest_draw['draw']):
                    print('資料已是最新，無需更新')
                    return self
                else:
                    print(f'發現新資料，最新期數: {latest_period}')

        yend, mend = (currentTime.year - 1911, currentTime.month)

        print(f'開始爬取資料: 從 {ybegin}/{mbegin} 到 {yend}/{mend}')

        for y in range(ybegin, yend + 1):
            for m in range(1, 13):
                if y == ybegin and m < mbegin:
                    continue
                elif y == yend and m > mend:
                    continue

                self.crawlMonth(y, m)
            
        return self

    def crawlYear(self, year):
        for m in range(1, 13):
            self.crawlMonth(year, m)
        return self

    def crawlMonth(self, year, month):
        data = self.crawlApi(year, month)
        if data:
            self.parse(data)
        return self

    def getAllDraws(self, r=False):
        return sorted(self.draws.items(), key=lambda x: x[1]['draw'], reverse=r)

    def getFirstDraw(self):
        sortedDraws = self.getAllDraws()
        return sortedDraws[0][1] if len(sortedDraws) > 0 else None
    
    def getLastDraw(self):
        sortedDraws = self.getAllDraws(r=True)
        return sortedDraws[0][1] if len(sortedDraws) > 0 else None

    def getDraw(self, id='103000001'):
        return self.draws[id]

    def load(self, filepath=''):
        if filepath == '':
            filename = f'{self.default_dir}/{self.default_filename}'

        print(f'載入資料從 {filename}')
        try:
            with open(filename, 'r', encoding='utf-8') as fp:
                self.draws = json.load(fp)
        except:
            print(f'開啟 {filename} 錯誤')

        return self
            
    def save(self, filepath=''):
        if filepath == '':
            filename = f'{self.default_dir}/{self.default_filename}'
            
        print(f'儲存資料至 {filename}')

        with open(filename, 'w', encoding='utf-8') as fp:
            fp.write(json.dumps(self.draws, indent=2, ensure_ascii=False, check_circular=False))

        return self

class BigLotto(LottoBase):
    def __init__(self):
        super().__init__('https://api.taiwanlottery.com/TLCAPIWeB/Lottery/Lotto649Result')
        self.default_filename = 'BigLotto.json'

    def crawlApi(self, year, month):
        print(f'爬取 {year}/{month} 的資料')

        if year < 103 or year > datetime.now().year - 1911 or month < 1 or month > 12:
            return None

        requestApi = f'{self.api}?period&month={year + 1911}-{"0" if month < 10 else ""}{month}&pageNum=1&pageSize=50'

        try:
            response = requests.get(requestApi)
            if response.status_code == 200:
                return response.content
            else:
                print(f'請求失敗: {response.status_code}')
                return None
        except Exception as e:
            print(f'爬取資料時發生錯誤: {str(e)}')
            return None

    def parse(self, jsonString):
        data = json.loads(jsonString)
        content = data["content"]
        draws = content["lotto649Res"]

        for draw in draws:
            drawID = draw["period"]
            date = datetime.strptime(draw["lotteryDate"], "%Y-%m-%dT%H:%M:%S")
            date = f"{date.year - 1911}/{'0' if date.month < 10 else ''}{date.month}/{'0' if date.day < 10 else ''}{date.day}"
            draw_numbers = draw["drawNumberAppear"][:6]
            size_numbers = draw["drawNumberSize"][:6]
            specialNum = draw["drawNumberAppear"][6]
            price = draw["totalAmount"]

            self.draws[str(drawID)] = {
                'draw': str(drawID),
                'date': date,
                'year': int(date.split('/')[0]),
                'month': int(date.split('/')[1]),
                'day': int(date.split('/')[2]),
                'price': price,
                'draw_order_nums': draw_numbers,
                'size_order_nums': size_numbers,
                'bonus_num': specialNum
            }

class SuperLotto(LottoBase):
    def __init__(self):
        super().__init__('https://api.taiwanlottery.com/TLCAPIWeB/Lottery/SuperLotto638Result')
        self.default_filename = 'SuperLotto.json'

    def crawlApi(self, year, month):
        print(f'爬取 {year}/{month} 的資料')

        if year < 103 or year > datetime.now().year - 1911 or month < 1 or month > 12:
            return None

        requestApi = f'{self.api}?period&month={year + 1911}-{"0" if month < 10 else ""}{month}&pageNum=1&pageSize=50'

        try:
            response = requests.get(requestApi)
            if response.status_code == 200:
                return response.content
            else:
                print(f'請求失敗: {response.status_code}')
                return None
        except Exception as e:
            print(f'爬取資料時發生錯誤: {str(e)}')
            return None

    def parse(self, jsonString):
        data = json.loads(jsonString)
        content = data["content"]
        draws = content["superLotto638Res"]

        for draw in draws:
            drawID = draw["period"]
            date = datetime.strptime(draw["lotteryDate"], "%Y-%m-%dT%H:%M:%S")
            date = f"{date.year - 1911}/{'0' if date.month < 10 else ''}{date.month}/{'0' if date.day < 10 else ''}{date.day}"
            draw_numbers = draw["drawNumberAppear"][:6]
            size_numbers = draw["drawNumberSize"][:6]
            specialNum = draw["drawNumberAppear"][6]
            price = draw["totalAmount"]

            self.draws[str(drawID)] = {
                'draw': str(drawID),
                'date': date,
                'year': int(date.split('/')[0]),
                'month': int(date.split('/')[1]),
                'day': int(date.split('/')[2]),
                'price': price,
                'draw_order_nums': draw_numbers,
                'size_order_nums': size_numbers,
                'bonus_num': specialNum
            }

class DailyCash(LottoBase):
    def __init__(self):
        super().__init__('https://api.taiwanlottery.com/TLCAPIWeB/Lottery/Daily539Result')
        self.default_filename = 'DailyCash.json'

    def crawlApi(self, year, month):
        print(f'爬取 {year}/{month} 的資料')

        if year < 103 or year > datetime.now().year - 1911 or month < 1 or month > 12:
            return None

        requestApi = f'{self.api}?period&month={year + 1911}-{"0" if month < 10 else ""}{month}&pageNum=1&pageSize=50'

        try:
            response = requests.get(requestApi)
            if response.status_code == 200:
                return response.content
            else:
                print(f'請求失敗: {response.status_code}')
                return None
        except Exception as e:
            print(f'爬取資料時發生錯誤: {str(e)}')
            return None

    def parse(self, jsonString):
        data = json.loads(jsonString)
        content = data["content"]
        draws = content["daily539Res"]

        for draw in draws:
            drawID = draw["period"]
            date = datetime.strptime(draw["lotteryDate"], "%Y-%m-%dT%H:%M:%S")
            date = f"{date.year - 1911}/{'0' if date.month < 10 else ''}{date.month}/{'0' if date.day < 10 else ''}{date.day}"
            draw_numbers = draw["drawNumberAppear"][:5]
            size_numbers = draw["drawNumberSize"][:5]

            self.draws[str(drawID)] = {
                'draw': str(drawID),
                'date': date,
                'year': int(date.split('/')[0]),
                'month': int(date.split('/')[1]),
                'day': int(date.split('/')[2]),
                'price': 8000000,
                'draw_order_nums': draw_numbers,
                'size_order_nums': size_numbers
            }

def update_all_lotto(force_update=False):
    # 確保data目錄存在
    os.makedirs('data', exist_ok=True)
    
    print('=== 開始更新大樂透資料 ===')
    bigLotto = BigLotto().load().crawl(force_update).save()
    print(f'大樂透最新一期: {bigLotto.getLastDraw()["draw"]}\n')
    
    print('=== 開始更新威力彩資料 ===')
    superLotto = SuperLotto().load().crawl(force_update).save()
    print(f'威力彩最新一期: {superLotto.getLastDraw()["draw"]}\n')
    
    print('=== 開始更新今彩539資料 ===')
    dailyCash = DailyCash().load().crawl(force_update).save()
    print(f'今彩539最新一期: {dailyCash.getLastDraw()["draw"]}\n')
    
    print('=== 所有資料更新完成 ===')

    # 更新數據庫
    print('=== 開始更新數據庫 ===')
    try:
        from create_db import import_data
        import_data()
        print('數據庫更新成功')
    except Exception as e:
        print(f'數據庫更新失敗: {str(e)}')

if __name__ == "__main__":
    # 正常更新（只更新新資料）
    update_all_lotto()
    
    # 如果需要強制更新所有資料，使用：
    # update_all_lotto(force_update=True) 