import os
from BigLotto import BigLotto
from SuperLotto import SuperLotto

# 确保data目录存在
os.makedirs('data', exist_ok=True)

bigLotto = BigLotto().load().crawl().save()
print(f'第一期大樂透 {bigLotto.getFirstDraw()}')

superLotto = SuperLotto().load().crawl(force_update=True).save()
print(f'最新一期威力彩 {superLotto.getLastDraw()}')
