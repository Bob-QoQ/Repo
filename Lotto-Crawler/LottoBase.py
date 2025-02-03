from datetime import datetime
import json
# https://api.taiwanlottery.com/TLCAPIWeB/Lottery/Lotto649Result?period&month=2024-01&pageNum=1&pageSize=50
class Lotto():
    def __init__(self, api='https://api.taiwanlottery.com/TLCAPIWeB/Lottery/Lotto649Result'):
        self.api = api
        self.default_dir = 'data'
        self.default_filename = 'Lotto.json'
        self.draws = {}
        pass

    def crawl(self, force_update=False):
        lastest_draw = self.getLastDraw()
        currentTime = datetime.now()

        print(f'最新一期: {lastest_draw["draw"] if lastest_draw else None}')

        # 如果沒有資料或強制更新，則從103年1月開始爬取
        if lastest_draw == None or force_update:
            ybegin, mbegin = (103, 1)
        else:
            # 獲取最新一期的日期，從該日期所在月份開始爬取
            ybegin = lastest_draw['year']
            mbegin = lastest_draw['month']
            
            # 先爬取當前月份的資料來檢查是否有更新
            current_month_data = self.crawlApi(ybegin, mbegin)
            if current_month_data:
                data = json.loads(current_month_data)
                content = data.get("content", {})
                latest_period = None
                
                # 根據不同的彩票類型獲取最新期數
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
        self.parse(data)
        return self

    def crawlApi(self, year, month):
        raise('Need Implement')
    
    def parse(self, html_body):
        raise('Need Implement')

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

        print(f'Load data from {filename}')
        try:
            with open(filename, 'r', encoding='utf-8') as fp:
                self.draws = json.load(fp)
                fp.close()
        except:
            print(f'Open {filename} error')

        return self
            
    def save(self, filepath=''):
        if filepath == '':
            filename = f'{self.default_dir}/{self.default_filename}'
            
        print(f'Save data to {filename}')

        with open(filename, 'w', encoding='utf-8') as fp:
            fp.write(json.dumps(self.draws, indent=2, ensure_ascii=False, check_circular=False))
            fp.close()

        return self
