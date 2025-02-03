import os
from BigLotto import BigLotto
from SuperLotto import SuperLotto
from DailyCash import DailyCash

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

if __name__ == "__main__":
    # 正常更新（只更新新資料）
    update_all_lotto()
    
    # 如果需要強制更新所有資料，使用：
    # update_all_lotto(force_update=True)
