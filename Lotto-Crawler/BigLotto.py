import requests
import json
from LottoBase import Lotto
from datetime import datetime

class BigLotto(Lotto):
    def __init__(self):
        super().__init__('https://api.taiwanlottery.com/TLCAPIWeB/Lottery/Lotto649Result')
        self.default_filename = 'BigLotto.json'

    def crawlApi(self, year, month):
        print(f'Crawl {year}/{month}')

        if year < 103 or year > datetime.now().year - 1911 or month < 1 or month > 12:
            return None

        requestApi = f'{self.api}?period&month={year + 1911}-{"0" if month < 10 else ""}{month}&pageNum=1&pageSize=50'

        # print('Request url: <' + requestApi + '>')

        return requests.get(requestApi).content

    def parse(self, jsonString):
        data = json.loads(jsonString)
        content = data["content"]
        draws = content["lotto649Res"]

        for draw in draws:
            drawID = draw["period"]
            date = datetime.strptime(draw["lotteryDate"], "%Y-%m-%dT%H:%M:%S") # e.g 2023-10-31T00:00:00
            date = f"{date.year - 1911}/{'0' if date.month < 10 else ''}{date.month}/{'0' if date.day < 10 else ''}{date.day}"
            draw_numbers = draw["drawNumberAppear"][:6]
            size_numbers = draw["drawNumberSize"][:6]
            specialNum = draw["drawNumberAppear"][6]
            price = draw["totalAmount"]

            # print(f'第 {drawID} 期, {date} 獎金 {price}， 號碼: {draw_numbers}，特別號: {specialNum}') 

            self.draws[str(drawID)] = {
                'draw'      : str(drawID),
                'date'      : date,
                'year'      : int(date.split('/')[0]),
                'month'     : int(date.split('/')[1]),
                'day'       : int(date.split('/')[2]),
                'price'     : price,
                'draw_order_nums'   : draw_numbers,
                'size_order_nums'   : size_numbers,
                'bonus_num'    :  specialNum
            }
            # print(self.draws[drawID])

if __name__ == "__main__":
    bigLotto = BigLotto()
    bigLotto.load()
    bigLotto.crawl()
    bigLotto.save()