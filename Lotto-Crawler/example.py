from BigLotto import BigLotto
from SuperLotto import SuperLotto

bigLotto = BigLotto().load().crawl().save()
print(f'第一期大樂透 {bigLotto.getFirstDraw()}')

superLotto = SuperLotto().load().crawl(force_update=True).save()
print(f'最新一期威力彩 {superLotto.getLastDraw()}')
