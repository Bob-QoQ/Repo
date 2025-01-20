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

        print(f'Lastest draw: {lastest_draw}')

        ybegin, mbegin = (103, 1) if lastest_draw == None or force_update else (lastest_draw['year'], lastest_draw['month'])
        yend, mend = (currentTime.year - 1911, currentTime.month)

        print(f'Start crawl from {ybegin}/{mbegin} to {yend}/{mend}')

        for y in range(ybegin, yend + 1):
            for m in range(1, 13):
                if y == ybegin and m < mbegin :
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
